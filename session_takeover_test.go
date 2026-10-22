package mqtt

import (
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

// stallHook holds the first call of the hook it watches until release is
// closed or hold has passed, and reports through entered that it is held.
// It widens, deterministically, the window a concurrent connection of the
// same client id races into: a broker restarted under thousands of devices
// sees exactly such pairs, a device's abandoned socket and its retry.
type stallHook struct {
	HookBase
	on      byte
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
	hold    time.Duration
}

func newStallHook(on byte) *stallHook {
	return &stallHook{on: on, entered: make(chan struct{}), release: make(chan struct{}), hold: 300 * time.Millisecond}
}

func (h *stallHook) ID() string           { return "stall" }
func (h *stallHook) Provides(b byte) bool { return b == h.on }

func (h *stallHook) stall() {
	if h.calls.Add(1) != 1 {
		return
	}
	close(h.entered)
	select {
	case <-h.release:
	case <-time.After(h.hold):
	}
}

func (h *stallHook) OnUnsubscribed(*Client, packets.Packet) { h.stall() }
func (h *stallHook) OnClientExpired(*Client)                { h.stall() }

// connectPacket is a CONNECT for client id zen.
func connectPacket(clean bool) packets.Packet {
	return packets.Packet{ProtocolVersion: 4, Connect: packets.ConnectParams{ClientIdentifier: "zen", Clean: clean}}
}

// sessionClient is a connection of client id zen, subscribed to a/b.
func sessionClient(s *Server, clean bool) *Client {
	cl, _, _ := newTestClient()
	cl.ID = "zen"
	cl.Properties.ProtocolVersion = 4
	cl.Properties.Clean = clean
	sub := packets.Subscription{Filter: "a/b"}
	cl.State.Subscriptions.Add(sub.Filter, sub)
	s.Topics.Subscribe(cl.ID, sub)
	return cl
}

// within runs f and reports whether it returned inside d. f keeps running
// when it did not.
func within(d time.Duration, f func()) (<-chan struct{}, bool) {
	done := make(chan struct{})
	go func() {
		f()
		close(done)
	}()
	select {
	case <-done:
		return done, true
	case <-time.After(d):
		return done, false
	}
}

// Two connections of one client id attaching at once: whichever attaches
// last holds the session and every other was taken over. Each used to read
// the same old session and register itself, so the first to register lost
// its entry to the second while neither took the other over: a live,
// subscribed connection that s.Clients no longer names, so nothing published
// to it was ever written to its socket.
func TestConcurrentAttachesLeaveOneHolder(t *testing.T) {
	s := newServer()
	hook := newStallHook(OnUnsubscribed)
	require.NoError(t, s.AddHook(hook, nil))
	s.Clients.Add(sessionClient(s, false))

	a, _, _ := newTestClient()
	a.ID = "zen"
	b, _, _ := newTestClient()
	b.ID = "zen"

	aDone, _ := within(0, func() { s.attachSession(connectPacket(true), a) })
	<-hook.entered
	bDone, _ := within(100*time.Millisecond, func() { s.attachSession(connectPacket(true), b) })
	close(hook.release)
	<-aDone
	<-bDone

	holder, ok := s.Clients.Get("zen")
	require.True(t, ok)
	require.False(t, holder.IsTakenOver(), "the session's holder was taken over")
	for _, cl := range []*Client{a, b} {
		if cl != holder {
			require.True(t, cl.IsTakenOver(), "a connection neither holds the session nor was taken over")
		}
	}
}

// A clean connection ending while its successor attaches leaves the
// successor's session alone. Its cleanup used to run on the client id, not on
// itself: it unsubscribed the id from every filter it had held and deleted
// whatever s.Clients held under the id, which by then was the successor.
func TestEndingConnectionKeepsItsSuccessorsSession(t *testing.T) {
	s := newServer()
	hook := newStallHook(OnUnsubscribed)
	require.NoError(t, s.AddHook(hook, nil))
	old := sessionClient(s, true)
	s.Clients.Add(old)

	oldDone, _ := within(0, func() { _ = s.endConnection(old, "tcp", errors.New("connection reset")) })
	<-hook.entered
	next, _, _ := newTestClient()
	next.ID = "zen"
	nextDone, _ := within(100*time.Millisecond, func() {
		s.attachSession(connectPacket(true), next)
		sub := packets.Subscription{Filter: "a/b"}
		next.State.Subscriptions.Add(sub.Filter, sub)
		s.Topics.Subscribe(next.ID, sub)
	})
	close(hook.release)
	<-oldDone
	<-nextDone

	holder, ok := s.Clients.Get("zen")
	require.True(t, ok, "the successor's session was deleted by its predecessor's end")
	require.Same(t, next, holder)
	require.Contains(t, s.Topics.Subscribers("a/b").Subscriptions, "zen", "the successor's subscription was removed by its predecessor's end")
}

// An expired session swept while a new connection resumes its id leaves the
// new connection's session alone. The sweep deleted the id it had listed, by
// then the new connection's.
func TestExpirySweepKeepsANewSession(t *testing.T) {
	s := newServer()
	s.Options.Capabilities.MaximumSessionExpiryInterval = 1
	hook := newStallHook(OnClientExpired)
	require.NoError(t, s.AddHook(hook, nil))
	stale := sessionClient(s, false)
	stale.Stop(errors.New("gone"))
	s.Clients.Add(stale)

	sweepDone, _ := within(0, func() { s.clearExpiredClients(time.Now().Unix() + 10) })
	<-hook.entered
	next, _, _ := newTestClient()
	next.ID = "zen"
	nextDone, _ := within(100*time.Millisecond, func() { s.attachSession(connectPacket(false), next) })
	close(hook.release)
	<-sweepDone
	<-nextDone

	holder, ok := s.Clients.Get("zen")
	require.True(t, ok, "the sweep deleted the session a new connection holds")
	require.Same(t, next, holder)
}

// DiscardClient ends the session of an offline client it names, and only
// that one. A connected client is refused: its socket would go on with no
// session. A taken-over client is refused: the id's new session stays. And a
// client stopped but not yet ended is fenced, so its end, running after a
// reconnect, leaves the new session's entry and subscription alone.
func TestDiscardClientEndsOnlyTheOfflineSessionItNames(t *testing.T) {
	s := newServer()
	live := sessionClient(s, false)
	s.Clients.Add(live)
	require.False(t, s.DiscardClient(live), "a connected client was discarded")
	_, ok := s.Clients.Get("zen")
	require.True(t, ok)

	taken, _, _ := newTestClient()
	taken.ID = "zen"
	s.attachSession(connectPacket(false), taken) // takes over live
	require.False(t, s.DiscardClient(live), "a taken-over client was discarded")
	holder, _ := s.Clients.Get("zen")
	require.Same(t, taken, holder)

	ending := taken
	ending.Properties.ProtocolVersion = 4
	ending.Properties.Clean = true
	ending.Stop(errors.New("connection reset")) // stopped, its end not yet run
	require.True(t, s.DiscardClient(ending))
	_, ok = s.Clients.Get("zen")
	require.False(t, ok, "the discarded session is still listed")
	require.NotContains(t, s.Topics.Subscribers("a/b").Subscriptions, "zen")

	next, _, _ := newTestClient()
	next.ID = "zen"
	s.attachSession(connectPacket(true), next)
	sub := packets.Subscription{Filter: "a/b"}
	next.State.Subscriptions.Add(sub.Filter, sub)
	s.Topics.Subscribe(next.ID, sub)
	_ = s.endConnection(ending, "tcp", errors.New("connection reset"))

	holder, ok = s.Clients.Get("zen")
	require.True(t, ok, "the discarded client's end removed the new session")
	require.Same(t, next, holder)
	require.Contains(t, s.Topics.Subscribers("a/b").Subscriptions, "zen", "the discarded client's end unsubscribed the new session")
}

// A delayed Will is sent only if the Will listed when the id's lock is held
// is still due. The sweep picks due Wills from a copy; meanwhile a connection
// may take the id, end, and leave a Will of its own still in its delay, which
// was published at once and cleared from that connection.
func TestDelayedWillSendRechecksTheListedWill(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	cl.ID = "zen"
	cl.Properties.Will = Will{Flag: 1, TopicName: "lwt", Payload: []byte("gone"), WillDelayInterval: 30}
	s.Clients.Add(cl)
	now := time.Now().Unix()
	s.loop.willDelayed.Add("zen", packets.Packet{TopicName: "lwt", Origin: "zen", Expiry: now + 30})

	s.sendDelayedWill("zen", now) // the sweep had seen an earlier, due Will
	_, listed := s.loop.willDelayed.Get("zen")
	require.True(t, listed, "a Will still in its delay was sent")
	require.Equal(t, uint32(1), cl.Properties.Will.Flag, "a Will still in its delay was cleared")

	s.sendDelayedWill("zen", now+31)
	_, listed = s.loop.willDelayed.Get("zen")
	require.False(t, listed, "a due Will was not sent")
}

// willCount counts the Wills sent.
type willCount struct {
	HookBase
	sent atomic.Int32
}

func (h *willCount) ID() string                         { return "will-count" }
func (h *willCount) Provides(b byte) bool               { return b == OnWillSent || b == OnConnectAuthenticate }
func (h *willCount) OnWillSent(*Client, packets.Packet) { h.sent.Add(1) }
func (h *willCount) OnConnectAuthenticate(*Client, packets.Packet) bool {
	return true
}

// A second CONNECT ends the connection, and its Will is published once, by
// the connection's end under the session lock, not ahead of it.
func TestSecondConnectSendsTheWillOnce(t *testing.T) {
	s := newServer()
	hook := new(willCount)
	require.NoError(t, s.AddHook(hook, nil))
	r, w := net.Pipe()
	o := make(chan error)
	go func() { o <- s.EstablishConnection("tcp", r) }()
	go func() { _, _ = io.Copy(io.Discard, w) }()
	_, err := w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectUserPassLWT).RawBytes)
	require.NoError(t, err)
	_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectUserPassLWT).RawBytes)

	require.ErrorIs(t, <-o, packets.ErrProtocolViolationSecondConnect)
	require.Equal(t, int32(1), hook.sent.Load())
	_ = w.Close()
}

// A connection taken over before its end ran never queues its delayed Will
// under the shared client id, where it fired while the successor was
// connected and cleared the successor's own Will. A successor that resumed
// the session cut the delay short, so the Will is not sent [MQTT-3.1.3-9];
// one that started clean ended the session, so the Will goes at once
// [MQTT-3.1.2-8].
func TestTakenOverConnectionQueuesNoDelayedWill(t *testing.T) {
	for _, tc := range []struct {
		name  string
		clean bool
		sent  int32
	}{
		{name: "successor resumes the session", clean: false, sent: 0},
		{name: "successor starts clean", clean: true, sent: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newServer()
			hook := new(willCount)
			require.NoError(t, s.AddHook(hook, nil))
			old, _, _ := newTestClient()
			old.ID = "zen"
			old.Properties.ProtocolVersion = 4 // no DISCONNECT is written to a 3.1.1 client the test never reads
			old.Properties.Will = Will{Flag: 1, TopicName: "lwt", Payload: []byte("old"), WillDelayInterval: 30}
			s.Clients.Add(old)

			next, _, _ := newTestClient()
			next.ID = "zen"
			next.Properties.Will = Will{Flag: 1, TopicName: "lwt", Payload: []byte("next"), WillDelayInterval: 30}
			s.attachSession(connectPacket(tc.clean), next)
			_ = s.endConnection(old, "tcp", errors.New("connection reset"))

			_, queued := s.loop.willDelayed.Get("zen")
			require.False(t, queued, "the taken-over connection queued its delayed Will")
			require.Equal(t, tc.sent, hook.sent.Load(), "Wills sent at the predecessor's end")
			s.sendDelayedLWT(time.Now().Unix() + 60)
			require.Equal(t, uint32(1), next.Properties.Will.Flag, "the successor's Will was cleared")
		})
	}
}
