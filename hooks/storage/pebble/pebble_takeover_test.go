package pebble

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

// stallDisconnect holds the server's first OnDisconnect until release is
// closed or 300 ms passed. Added before the storage hook, it pauses an ending
// connection just ahead of the store's own OnDisconnect.
type stallDisconnect struct {
	mqtt.HookBase
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (h *stallDisconnect) ID() string           { return "stall-disconnect" }
func (h *stallDisconnect) Provides(b byte) bool { return b == mqtt.OnDisconnect }
func (h *stallDisconnect) OnDisconnect(*mqtt.Client, error, bool) {
	if h.calls.Add(1) != 1 {
		return
	}
	close(h.entered)
	select {
	case <-h.release:
	case <-time.After(300 * time.Millisecond):
	}
}

// disconnected closes done after the first OnDisconnect. Added after the
// storage hook, it tells the test the store has heard that end.
type disconnected struct {
	mqtt.HookBase
	once atomic.Bool
	done chan struct{}
}

func (h *disconnected) ID() string           { return "disconnected" }
func (h *disconnected) Provides(b byte) bool { return b == mqtt.OnDisconnect }
func (h *disconnected) OnDisconnect(*mqtt.Client, error, bool) {
	if h.once.CompareAndSwap(false, true) {
		close(h.done)
	}
}

// established closes done once a persistent session of client id zen was
// established. Added after the storage hook, it tells the test the store has
// written the successor's record: the CONNACK goes out before
// OnSessionEstablished, so the CONNACK alone does not.
type established struct {
	mqtt.HookBase
	once atomic.Bool
	done chan struct{}
}

func (h *established) ID() string           { return "established" }
func (h *established) Provides(b byte) bool { return b == mqtt.OnSessionEstablished }
func (h *established) OnSessionEstablished(cl *mqtt.Client, _ packets.Packet) {
	if !cl.Properties.Clean && h.once.CompareAndSwap(false, true) {
		close(h.done)
	}
}

// connect opens a connection to s, sends the CONNECT fixture and returns the
// client's end of the pipe and a channel that delivers once its CONNACK came.
func connect(t *testing.T, s *mqtt.Server, fixture byte) (net.Conn, <-chan struct{}) {
	t.Helper()
	server, client := net.Pipe()
	go func() { _ = s.EstablishConnection("tcp", server) }()
	_, err := client.Write(packets.TPacketData[packets.Connect].Get(fixture).RawBytes)
	require.NoError(t, err)
	acked := make(chan struct{})
	go func() {
		buf := make([]byte, 4)
		if _, err := io.ReadFull(client, buf); err == nil {
			close(acked)
		}
		_, _ = io.Copy(io.Discard, client)
	}()
	return client, acked
}

// A clean connection that loses its socket and is taken over before its
// OnDisconnect reached the store leaves its successor's record alone. The
// store's OnDisconnect ran outside the session lock and judged ownership by
// the stop cause alone, which a socket error had already set: it deleted the
// record the successor had just stored, so the surviving session could not
// be restored after a restart.
func TestTakeoverDuringDisconnectKeepsTheSuccessorsRecord(t *testing.T) {
	path := t.TempDir()
	s := mqtt.New(&mqtt.Options{})
	stall := &stallDisconnect{entered: make(chan struct{}), release: make(chan struct{})}
	require.NoError(t, s.AddHook(stall, nil))
	require.NoError(t, s.AddHook(new(auth.AllowHook), nil))
	store := new(Hook)
	require.NoError(t, s.AddHook(store, &Options{Path: path, Mode: Sync}))
	oldEnded := &disconnected{done: make(chan struct{})}
	require.NoError(t, s.AddHook(oldEnded, nil))
	nextEstablished := &established{done: make(chan struct{})}
	require.NoError(t, s.AddHook(nextEstablished, nil))
	require.NoError(t, s.Serve())

	old, oldAcked := connect(t, s, packets.TConnectClean)
	<-oldAcked
	_ = old.Close() // the socket is lost; its end stalls before the store hears it
	<-stall.entered

	next, nextAcked := connect(t, s, packets.TConnectMqtt311)
	select {
	case <-nextAcked:
	case <-time.After(100 * time.Millisecond):
	}
	close(stall.release)
	select {
	case <-nextAcked:
	case <-time.After(5 * time.Second):
		t.Fatal("the successor was never acknowledged")
	}
	<-oldEnded.done
	select {
	case <-nextEstablished.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the successor's session was never established")
	}
	holder, ok := s.Clients.Get("zen")
	require.True(t, ok && !holder.Closed(), "the successor does not hold the session")

	// Read while the successor is still connected: its own end would write
	// its record again and hide the loss.
	stored, err := store.StoredClients()
	_ = next.Close()
	defer func() { _ = s.Close() }()
	require.NoError(t, err)
	require.Len(t, stored, 1, "the successor's persistent session was not kept in the store")
	require.Equal(t, "zen", stored[0].ID)
	require.False(t, stored[0].Clean)
}

// stallWill holds the server's first OnWill until release is closed or 300 ms
// passed: an ending connection paused just as its Will goes out.
type stallWill struct {
	mqtt.HookBase
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (h *stallWill) ID() string           { return "stall-will" }
func (h *stallWill) Provides(b byte) bool { return b == mqtt.OnWill }
func (h *stallWill) OnWill(_ *mqtt.Client, will mqtt.Will) (mqtt.Will, error) {
	if h.calls.Add(1) == 1 {
		close(h.entered)
		select {
		case <-h.release:
		case <-time.After(300 * time.Millisecond):
		}
	}
	return will, nil
}

// A connection with a Will that loses its socket and is taken over while its
// Will goes out leaves the successor's record alone. The Will was sent before
// the session lock was taken, and OnWillSent rewrote the record from the
// ending client, so the successor's persistent session was stored as the
// predecessor's clean one.
func TestTakeoverDuringWillKeepsTheSuccessorsRecord(t *testing.T) {
	s := mqtt.New(&mqtt.Options{})
	stall := &stallWill{entered: make(chan struct{}), release: make(chan struct{})}
	require.NoError(t, s.AddHook(stall, nil))
	require.NoError(t, s.AddHook(new(auth.AllowHook), nil))
	store := new(Hook)
	require.NoError(t, s.AddHook(store, &Options{Path: t.TempDir(), Mode: Sync}))
	oldEnded := &disconnected{done: make(chan struct{})}
	require.NoError(t, s.AddHook(oldEnded, nil))
	nextEstablished := &established{done: make(chan struct{})}
	require.NoError(t, s.AddHook(nextEstablished, nil))
	require.NoError(t, s.Serve())
	defer func() { _ = s.Close() }()

	old, oldAcked := connect(t, s, packets.TConnectUserPassLWT)
	<-oldAcked
	_ = old.Close()
	<-stall.entered

	next, _ := connect(t, s, packets.TConnectMqtt311)
	defer func() { _ = next.Close() }()
	select {
	case <-nextEstablished.done:
	case <-time.After(100 * time.Millisecond):
	}
	close(stall.release)
	<-oldEnded.done
	select {
	case <-nextEstablished.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the successor's session was never established")
	}

	stored, err := store.StoredClients()
	require.NoError(t, err)
	require.Len(t, stored, 1, "the successor's persistent session was not kept in the store")
	require.False(t, stored[0].Clean, "the successor's record was overwritten by its predecessor's")
}

// The Will of a client discarded before its end ran is never stored back: the
// record DiscardClient dropped stays dropped.
func TestDiscardedClientsWillDoesNotRestoreItsRecord(t *testing.T) {
	s := mqtt.New(&mqtt.Options{})
	store := new(Hook)
	require.NoError(t, s.AddHook(store, &Options{Path: t.TempDir(), Mode: Sync}))
	cl := s.NewClient(nil, "tcp", "zen", false)
	cl.Properties.ProtocolVersion = 4
	cl.Properties.Will = mqtt.Will{Flag: 1, TopicName: "lwt", Payload: []byte("gone")}
	store.OnSessionEstablished(cl, packets.Packet{})
	s.Clients.Add(cl)
	cl.Stop(io.ErrClosedPipe) // stopped, its end (and its Will) not yet run

	require.True(t, s.DiscardClient(cl))
	store.OnWillSent(cl, packets.Packet{TopicName: "lwt"})

	stored, err := store.StoredClients()
	require.NoError(t, err)
	require.Empty(t, stored, "a discarded client's Will stored its record again")
}
