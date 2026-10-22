package pebble

import (
	"fmt"
	"testing"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

// bootWrites stores sessions in a Sync-mode store at path, boots a server on
// it, and returns how many bytes the boot wrote to the store's WAL.
func bootWrites(t *testing.T, path string, sessions int) uint64 {
	t.Helper()
	first := new(Hook)
	first.SetOpts(logger, nil)
	require.NoError(t, first.Init(&Options{Path: path, Mode: Sync}))
	maker := mqtt.New(&mqtt.Options{})
	for i := 0; i < sessions; i++ {
		cl := maker.NewClient(nil, "tcp", fmt.Sprintf("gw-%d", i), false)
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
	require.Equal(t, sessions, s.Clients.Len(), "the stored sessions were not restored")
	require.NoError(t, s.Close())
	return written
}

// A server restoring its sessions writes none of them back. Each restored
// client is told OnDisconnect, and the hook used to rewrite its record from
// the restored client: in Sync mode one fsync per stored session on every
// boot, before the server serves, and every record lost what the restored
// client does not carry (its remote address). The boot still writes its
// $SYS record, so it is held to what booting an empty store writes.
func TestRestoringSessionsWritesNothingBack(t *testing.T) {
	empty := bootWrites(t, t.TempDir(), 0)
	path := t.TempDir()
	written := bootWrites(t, path, 300)
	require.LessOrEqual(t, written, empty+64, "booting 300 sessions wrote %d bytes, an empty store %d", written, empty)

	after := new(Hook)
	after.SetOpts(logger, nil)
	require.NoError(t, after.Init(&Options{Path: path}))
	defer func() { _ = after.Stop() }()
	stored, err := after.StoredClients()
	require.NoError(t, err)
	require.Len(t, stored, 300)
	for _, c := range stored {
		require.Equal(t, "10.0.0.7:5000", c.Remote, "a restart rewrote %s's record without its remote address", c.ID)
	}
}
