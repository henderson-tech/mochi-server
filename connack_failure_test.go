package mqtt

import (
	"io"
	"net"
	"testing"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

// endHook records each OnDisconnect and each Will sent. The test reads it only
// after EstablishConnection has returned, which orders every write before it.
type endHook struct {
	HookBase
	errs    []error
	expires []bool
	wills   []string
}

func (h *endHook) ID() string { return "end-hook" }

func (h *endHook) Provides(b byte) bool { return b == OnDisconnect || b == OnWillSent }

func (h *endHook) OnDisconnect(_ *Client, err error, expire bool) {
	h.errs = append(h.errs, err)
	h.expires = append(h.expires, expire)
}

func (h *endHook) OnWillSent(_ *Client, pk packets.Packet) {
	h.wills = append(h.wills, pk.TopicName)
}

// A client accepted by OnConnectAuthenticate but gone before its CONNACK is
// written (it reset the socket right after CONNECT) ends like any other
// connection: OnDisconnect is told, a session that ends with the connection
// leaves s.Clients, and the Will is published. Without that, a hook that
// starts tracking a session when it authenticates it never hears it end, and
// a v3 client's dead entry stays in s.Clients until the maximum session
// expiry, which defaults to never.
func TestConnackWriteFailureEndsTheConnection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		connect byte
		expire  bool
		wills   []string
	}{
		{name: "clean session with a will", connect: packets.TConnectUserPassLWT, expire: true, wills: []string{"lwt"}},
		{name: "persistent session", connect: packets.TConnectMqtt311, expire: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newServer()
			hook := new(endHook)
			require.NoError(t, s.AddHook(hook, nil))

			r, w := net.Pipe()
			o := make(chan error)
			go func() {
				o <- s.EstablishConnection("tcp", r)
			}()

			_, err := w.Write(packets.TPacketData[packets.Connect].Get(tc.connect).RawBytes)
			require.NoError(t, err)
			_ = w.Close()

			err = <-o
			require.ErrorIs(t, err, io.ErrClosedPipe)
			require.Len(t, hook.errs, 1, "OnDisconnect was not called once")
			require.ErrorIs(t, hook.errs[0], io.ErrClosedPipe)
			require.Equal(t, tc.expire, hook.expires[0])
			require.Equal(t, tc.wills, hook.wills)

			_, kept := s.Clients.Get("zen")
			require.Equal(t, !tc.expire, kept, "a session is kept only when it outlives its connection")

			_ = r.Close()
		})
	}
}

// A resumed session whose queued messages cannot be resent ends its
// connection the same way: OnDisconnect is told, and the session it resumed
// is kept for the next connection.
func TestResendFailureEndsTheConnection(t *testing.T) {
	s := newServer()
	hook := new(endHook)
	require.NoError(t, s.AddHook(hook, nil))

	existing, er, _ := newTestClient()
	existing.ID = "zen"
	existing.State.Inflight.Set(packets.Packet{PacketID: 2}) // no packet type, so it cannot be written
	s.Clients.Add(existing)
	go func() { _, _ = io.ReadAll(er) }()

	r, w := net.Pipe()
	o := make(chan error)
	go func() {
		o <- s.EstablishConnection("tcp", r)
	}()
	go func() { _, _ = io.ReadAll(w) }()
	_, err := w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt311).RawBytes)
	require.NoError(t, err)

	err = <-o
	require.ErrorIs(t, err, packets.ErrNoValidPacketAvailable)
	require.Len(t, hook.errs, 1, "OnDisconnect was not called once")
	require.ErrorIs(t, hook.errs[0], packets.ErrNoValidPacketAvailable)
	require.False(t, hook.expires[0])

	cl, kept := s.Clients.Get("zen")
	require.True(t, kept, "a persistent session outlives its connection")
	require.NotSame(t, existing, cl, "the session belongs to the connection that resumed it")

	_ = w.Close()
}
