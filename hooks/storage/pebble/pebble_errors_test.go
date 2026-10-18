package pebble

import (
	"testing"
	"time"

	pebbledb "github.com/cockroachdb/pebble"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

// A write the store refuses is only logged - the hook cannot refuse the
// packet whose state it was saving - so Errors is how an embedder learns of it.
func TestErrorsCountsTheWritesTheStoreRefused(t *testing.T) {
	path := t.TempDir()
	h := new(Hook)
	h.SetOpts(logger, nil)
	require.NoError(t, h.Init(&Options{Path: path}))
	require.NoError(t, h.Stop())

	h = new(Hook)
	h.SetOpts(logger, nil)
	require.NoError(t, h.Init(&Options{Path: path, Options: &pebbledb.Options{ReadOnly: true}}))
	defer func() { _ = h.Stop() }()
	require.Zero(t, h.Errors())

	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, PacketID: 1, TopicName: "a/b/c"}
	h.OnQosPublish(client, pk, time.Now().Unix(), 0)
	require.Equal(t, uint64(1), h.Errors(), "a refused Set is counted")

	h.OnQosComplete(client, pk)
	require.Equal(t, uint64(2), h.Errors(), "a refused Delete is counted")
}
