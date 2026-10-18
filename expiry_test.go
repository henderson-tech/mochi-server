package mqtt

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A message's absolute expiry holds for a 3.1.1 client too: a session that
// sat offline past the deadline does not get the message on reconnect.
func TestA311ResendAfterExpiryIsNeverWritten(t *testing.T) {
	cl, r, w := newTestClient()
	require.Equal(t, byte(4), cl.Properties.ProtocolVersion)
	hook := &readACLHook{readable: "cmd"}
	require.NoError(t, cl.ops.hooks.Add(hook, nil))
	late := qos1Publish(1, "cmd")
	late.Expiry = time.Now().Unix() - 1
	cl.State.Inflight.Set(late)

	go func() {
		require.NoError(t, cl.ResendInflightMessages(true))
		_ = w.Close()
	}()
	buf, err := io.ReadAll(r)
	require.NoError(t, err)

	require.Empty(t, buf, "an expired message was written to a 3.1.1 client")
	require.Equal(t, []uint16{1}, hook.dropped)
	require.Zero(t, cl.State.Inflight.Len())
}

// WritePacket used to clamp an elapsed expiry to one second and send the
// message anyway; for 3.1.1, which carries no interval, as if on time.
func TestWritePacketRefusesAnExpiredPublish(t *testing.T) {
	for _, version := range []byte{4, 5} {
		cl, _, _ := newTestClient()
		cl.Properties.ProtocolVersion = version
		late := qos1Publish(1, "cmd")
		late.Expiry = time.Now().Unix()

		require.ErrorIs(t, cl.WritePacket(late), ErrPacketExpired, "protocol version %d", version)
	}
}

// Deleted on expiry, a delivery that took a slot of the client's Receive
// Maximum returns it, since no acknowledgement will. A held one took none.
func TestExpiredDeliveryReturnsItsQuotaSlot(t *testing.T) {
	cl, _, _ := newTestClient()
	now := time.Now().Unix()
	sent := qos1Publish(1, "cmd")
	sent.Created, sent.Expiry = now, now-1
	held := qos1Publish(2, "cmd")
	held.Created, held.Expiry = now, now-1
	cl.State.Inflight.Set(sent)
	cl.State.Inflight.Set(held)
	cl.State.Inflight.hold(2)
	cl.State.Inflight.sendQuota = 3

	require.ElementsMatch(t, []uint16{1, 2}, cl.ClearExpiredInflights(now, 0))
	require.Equal(t, int32(4), cl.State.Inflight.sendQuota)
}

// The expiry tick reports each dropped packet to the hooks once, with the
// whole packet.
func TestExpiryTickDropsEachPacketOnce(t *testing.T) {
	s := newServer()
	hook := new(readACLHook)
	require.NoError(t, s.AddHook(hook, nil))
	cl := s.NewClient(nil, "tcp", "c", false)
	now := time.Now().Unix()
	late := qos1Publish(1, "cmd")
	late.Created, late.Expiry = now, now-1
	cl.State.Inflight.Set(late)
	s.Clients.Add(cl)

	s.clearExpiredInflights(now)
	require.Equal(t, []uint16{1}, hook.dropped)
}
