package mqtt

import (
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

// publishHook answers OnPublish with err, after release is closed when it is
// set.
type publishHook struct {
	HookBase
	err     error
	entered chan struct{}
	release chan struct{}
}

func (h *publishHook) ID() string { return "publish-hook" }

func (h *publishHook) Provides(b byte) bool { return b == OnPublish }

func (h *publishHook) OnPublish(_ *Client, pk packets.Packet) (packets.Packet, error) {
	if h.release != nil {
		close(h.entered)
		<-h.release
	}
	return pk, h.err
}

// publisher is a 3.1.1 client of s whose writes the test reads from the
// returned end of a pipe.
func publisher(t *testing.T, s *Server) (*Client, net.Conn) {
	t.Helper()
	r, w := net.Pipe()
	t.Cleanup(func() { _ = r.Close(); _ = w.Close() })
	cl := s.NewClient(w, "tcp", "publisher", false)
	cl.State.Inflight.ResetReceiveQuota(10)
	return cl, r
}

// An embedder that commits a publish inside OnPublish relies on the PUBACK
// meaning "committed": it is written only once OnPublish has returned.
func TestPubackFollowsOnPublish(t *testing.T) {
	s := newServer()
	hook := &publishHook{entered: make(chan struct{}), release: make(chan struct{})}
	require.NoError(t, s.AddHook(hook, nil))
	cl, r := publisher(t, s)

	done := make(chan error, 1)
	go func() { done <- s.processPacket(cl, qos1Publish(9, "a/b")) }()
	<-hook.entered

	_ = r.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	_, err := r.Read(make([]byte, 1))
	require.ErrorIs(t, err, os.ErrDeadlineExceeded, "a PUBACK was written before OnPublish returned")

	close(hook.release)
	_ = r.SetReadDeadline(time.Now().Add(5 * time.Second))
	ack := make([]byte, 4)
	_, err = io.ReadFull(r, ack)
	require.NoError(t, err)
	require.Equal(t, []byte{packets.Puback << 4, 2, 0, 9}, ack)
	require.NoError(t, <-done)
}

// CodeSuccessIgnore acknowledges a publish but neither retains it nor
// delivers it to a subscriber.
func TestIgnoreSkipsRetainAndFanOut(t *testing.T) {
	s := newServer()
	require.NoError(t, s.AddHook(&publishHook{err: packets.CodeSuccessIgnore}, nil))
	sub, _, _ := newTestClient()
	sub.ID = "subscriber"
	s.Clients.Add(sub)
	require.True(t, s.Topics.Subscribe(sub.ID, packets.Subscription{Filter: "a/b", Qos: 1}))
	cl, r := publisher(t, s)

	pk := qos1Publish(9, "a/b")
	pk.FixedHeader.Retain = true
	done := make(chan error, 1)
	go func() { done <- s.processPacket(cl, pk) }()

	ack := make([]byte, 4)
	_ = r.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err := io.ReadFull(r, ack)
	require.NoError(t, err)
	require.Equal(t, []byte{packets.Puback << 4, 2, 0, 9}, ack, "an ignored publish is still acknowledged")
	require.NoError(t, <-done)

	require.Empty(t, s.Topics.Messages("a/b"), "an ignored publish was retained")
	require.Zero(t, atomic.LoadInt32(&sub.State.outboundQty), "an ignored publish reached a subscriber")
	require.Zero(t, sub.State.Inflight.Len())
}
