package mqtt

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

// A delivery held for send quota used to be marked Expiry -1, which threw
// its absolute expiry away. It keeps it now.
func TestHeldDeliveryKeepsItsExpiry(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	cl.State.Inflight.sendQuota = 0 // the client's Receive Maximum is used up

	pk := qos1Publish(0, "a/b")
	pk.Expiry = time.Now().Unix() + 30
	out, err := s.publishToClient(cl, packets.Subscription{Filter: "a/b", Qos: 1}, pk)
	require.NoError(t, err)

	held, ok := cl.State.Inflight.Get(out.PacketID)
	require.True(t, ok)
	require.True(t, cl.State.Inflight.isHeld(out.PacketID))
	require.Equal(t, pk.Expiry, held.Expiry)
}

// Released by an acknowledgement, a held delivery is in flight like any
// other, and its own PUBACK returns the quota. It used to be deleted once
// written, so its PUBACK found nothing and the quota never came back: every
// later delivery to that client stayed held.
func TestReleasedDeliveryReturnsItsQuotaOnItsPuback(t *testing.T) {
	s := newServer()
	r, w := net.Pipe()
	defer func() { _ = r.Close(); _ = w.Close() }()
	go func() { _, _ = io.Copy(io.Discard, r) }()
	cl := s.NewClient(w, "tcp", "rm1", false)
	cl.Properties.ProtocolVersion = 5
	cl.State.Inflight.ResetSendQuota(1) // the client's Receive Maximum
	sub := packets.Subscription{Filter: "t", Qos: 1}
	puback := func(id uint16) packets.Packet {
		return packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Puback}, PacketID: id}
	}

	a, err := s.publishToClient(cl, sub, qos1Publish(0, "t"))
	require.NoError(t, err)
	b, err := s.publishToClient(cl, sub, qos1Publish(0, "t"))
	require.NoError(t, err)
	require.True(t, cl.State.Inflight.isHeld(b.PacketID), "the second delivery waits for quota")

	require.NoError(t, s.processPacket(cl, puback(a.PacketID)))
	require.False(t, cl.State.Inflight.isHeld(b.PacketID), "the PUBACK released the held delivery")
	_, inFlight := cl.State.Inflight.Get(b.PacketID)
	require.True(t, inFlight, "a released delivery stays in flight until acknowledged")

	require.NoError(t, s.processPacket(cl, puback(b.PacketID)))
	c, err := s.publishToClient(cl, sub, qos1Publish(0, "t"))
	require.NoError(t, err)
	require.False(t, cl.State.Inflight.isHeld(c.PacketID), "the PUBACK returned the quota, so the next delivery goes out")
}

// A held delivery was never written, so a resumed session does not re-send
// it; it waits for quota. What is re-sent holds a quota slot again.
func TestResendSkipsAHeldDelivery(t *testing.T) {
	cl, r, w := newTestClient()
	require.NoError(t, cl.ops.hooks.Add(new(AllowHook), nil))
	cl.State.Inflight.Set(qos1Publish(1, "sent"))
	cl.State.Inflight.Set(qos1Publish(2, "held"))
	cl.State.Inflight.hold(2)

	go func() {
		require.NoError(t, cl.ResendInflightMessages(true))
		_ = w.Close()
	}()
	buf, err := io.ReadAll(r)
	require.NoError(t, err)

	require.True(t, bytes.Contains(buf, []byte("payload of sent")))
	require.False(t, bytes.Contains(buf, []byte("payload of held")), "a held delivery was re-sent")
	require.True(t, cl.State.Inflight.isHeld(2))
	require.Equal(t, int32(4), cl.State.Inflight.sendQuota, "the re-sent delivery holds a quota slot")
}
