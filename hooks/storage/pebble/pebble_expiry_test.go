package pebble

import (
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

// A message restored from the store after a restart keeps its absolute
// expiry; without it the restored copy would be delivered at any age up to
// MaximumMessageExpiryInterval.
func TestStoredInflightMessageKeepsItsExpiry(t *testing.T) {
	h := new(Hook)
	h.SetOpts(logger, nil)
	require.NoError(t, h.Init(&Options{Path: t.TempDir()}))
	defer func() { _ = h.Stop() }()

	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		PacketID:    7,
		TopicName:   "a/b/c",
		Created:     time.Now().Unix(),
		Expiry:      time.Now().Unix() + 30,
	}
	h.OnQosPublish(client, pk, time.Now().Unix(), 0)

	stored, err := h.StoredInflightMessages()
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.Equal(t, pk.Expiry, stored[0].ToPacket().Expiry)
}
