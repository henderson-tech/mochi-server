package mqtt

import (
	"io"
	"net"
	"testing"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

// A v5 SUBSCRIBE that ends right after its topic filter, without the
// subscription options byte, panicked the decoder and with it the whole
// process (upstream #501, fixed in #524). On the wire it must end only the
// connection that sent it.
func TestTruncatedV5SubscribeEndsOnlyItsConnection(t *testing.T) {
	s := newServer()
	defer s.Close()

	r, w := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.EstablishConnection("tcp", r) }()
	go func() { _, _ = io.Copy(io.Discard, w) }() // the CONNACK

	_, err := w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt5).RawBytes)
	require.NoError(t, err)
	// The bytes of the upstream report: packet id 1, no properties, filter "a",
	// and no options byte.
	_, err = w.Write([]byte{0x82, 0x06, 0x00, 0x01, 0x00, 0x00, 0x01, 'a'})
	require.NoError(t, err)

	require.ErrorIs(t, <-done, packets.ErrMalformedQos)
	_ = w.Close()

	// The server still serves the next client.
	r2, w2 := net.Pipe()
	go func() { _ = s.EstablishConnection("tcp", r2) }()
	_, err = w2.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes)
	require.NoError(t, err)
	connack := make([]byte, 4)
	_, err = io.ReadFull(w2, connack)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Connack].Get(packets.TConnackAcceptedNoSession).RawBytes, connack)
	_ = w2.Close()
}
