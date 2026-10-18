package mqtt

import (
	"bytes"
	"io"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// A fixed header may declare up to 256 MiB of remaining length with nothing
// behind it, before CONNECT is authenticated. The read must not allocate the
// declaration ahead of the bytes (upstream #502).
func TestReadRemainingAllocatesOnlyWhatArrives(t *testing.T) {
	const declared = 64 << 20
	sent := bytes.Repeat([]byte{'x'}, 10)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	p, err := readRemaining(bytes.NewReader(sent), declared)
	runtime.ReadMemStats(&after)

	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, sent, p)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20),
		"a 64 MiB declaration followed by 10 bytes must not allocate the declaration")
}

func TestReadRemainingReadsALargePacketWhole(t *testing.T) {
	want := make([]byte, 5*remainingReadStep+123)
	for i := range want {
		want[i] = byte(i)
	}

	got, err := readRemaining(bytes.NewReader(want), len(want))
	require.NoError(t, err)
	require.Equal(t, want, got)
}
