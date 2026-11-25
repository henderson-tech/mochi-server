package mqtt

import (
	"bytes"
	"io"
	"testing"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

// readACLHook lets a client read one topic and records each dropped packet.
type readACLHook struct {
	HookBase
	readable string
	dropped  []uint16
}

func (h *readACLHook) ID() string { return "read-acl" }

func (h *readACLHook) Provides(b byte) bool { return b == OnACLCheck || b == OnQosDropped }

func (h *readACLHook) OnACLCheck(_ *Client, topic string, write bool) bool {
	return !write && topic == h.readable
}

func (h *readACLHook) OnQosDropped(_ *Client, pk packets.Packet) {
	h.dropped = append(h.dropped, pk.PacketID)
}

func qos1Publish(id uint16, topic string) packets.Packet {
	return packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		PacketID:    id,
		TopicName:   topic,
		Payload:     []byte("payload of " + topic),
	}
}

// A session resumed under its client id may be resumed by another principal,
// or by one whose permissions narrowed. Its queued messages are resent only
// where the client resuming it may read them; the rest are dropped
// (upstream #504).
func TestResendWithholdsWhatTheResumingClientMayNotRead(t *testing.T) {
	cl, r, w := newTestClient()
	hook := &readACLHook{readable: "mine"}
	require.NoError(t, cl.ops.hooks.Add(hook, nil))
	cl.State.Inflight.Set(qos1Publish(1, "theirs"))
	cl.State.Inflight.Set(qos1Publish(2, "mine"))

	go func() {
		require.NoError(t, cl.ResendInflightMessages(true))
		_ = w.Close()
	}()
	buf, err := io.ReadAll(r)
	require.NoError(t, err)

	require.False(t, bytes.Contains(buf, []byte("theirs")), "a message the resuming client may not read was resent")
	require.True(t, bytes.Contains(buf, []byte("payload of mine")), "a readable message was not resent")
	require.Equal(t, []uint16{1}, hook.dropped)
	_, kept := cl.State.Inflight.Get(1)
	require.False(t, kept, "a withheld message stays in the session")
	_, kept = cl.State.Inflight.Get(2)
	require.True(t, kept, "a resent QoS 1 message waits for its PUBACK")
}
