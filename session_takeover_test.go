package mqtt

import (
	"errors"
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
