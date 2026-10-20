package pebble

import (
	"testing"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

// A server restoring its sessions writes none of them back. Each restored
// client is told OnDisconnect, and the hook used to rewrite its record from
// the restored client: in Sync mode one fsync per stored session on every
// boot, before the server serves, and every record lost what the restored
// client does not carry (its remote address).
func TestRestoringSessionsWritesNothingBack(t *testing.T) {
	path := t.TempDir()
	first := new(Hook)
	first.SetOpts(logger, nil)
	require.NoError(t, first.Init(&Options{Path: path, Mode: Sync}))
	maker := mqtt.New(&mqtt.Options{})
	for _, id := range []string{"zen", "gw-1", "gw-2"} {
		cl := maker.NewClient(nil, "tcp", id, false)
		cl.Net.Remote = "10.0.0.7:5000"
		cl.Properties.ProtocolVersion = 4
		first.OnSessionEstablished(cl, packets.Packet{})
	}
	require.NoError(t, first.Stop())

	s := mqtt.New(&mqtt.Options{})
	restoring := new(Hook)
	require.NoError(t, s.AddHook(restoring, &Options{Path: path, Mode: Sync}))
	before := restoring.db.Metrics().WAL.BytesIn
	require.NoError(t, s.Serve())
	written := restoring.db.Metrics().WAL.BytesIn - before
	_, ok := s.Clients.Get("zen")
	require.True(t, ok, "the stored session was not restored")
	require.NoError(t, s.Close())
	require.Zero(t, written, "restoring the sessions wrote to the store")

	after := new(Hook)
	after.SetOpts(logger, nil)
	require.NoError(t, after.Init(&Options{Path: path}))
	defer func() { _ = after.Stop() }()
	stored, err := after.StoredClients()
	require.NoError(t, err)
	require.Len(t, stored, 3)
	for _, c := range stored {
		require.Equal(t, "10.0.0.7:5000", c.Remote, "a restart rewrote %s's record without its remote address", c.ID)
	}
}
