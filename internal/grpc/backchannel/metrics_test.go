package backchannel

import (
	"net"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestInstrumentedConn_Write(t *testing.T) {
	clientPipe, serverPipe := net.Pipe()
	defer clientPipe.Close()
	defer serverPipe.Close()

	instrumentedClient := newInstrumentedConn(clientPipe)

	testData := []byte("hello world")
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, len(testData))
		_, _ = serverPipe.Read(buf)
	}()

	n, err := instrumentedClient.Write(testData)
	require.NoError(t, err)
	require.Equal(t, len(testData), n)

	<-done

	require.Equal(t, 1, testutil.CollectAndCount(yamuxWriteSizeHistogram))
}

func TestInstrumentedConn_Read(t *testing.T) {
	clientPipe, serverPipe := net.Pipe()
	defer clientPipe.Close()
	defer serverPipe.Close()

	instrumentedServer := newInstrumentedConn(serverPipe)

	testData := []byte("hello world")
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = clientPipe.Write(testData)
	}()

	buf := make([]byte, len(testData))
	n, err := instrumentedServer.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(testData), n)
	require.Equal(t, testData, buf)

	<-done

	require.Equal(t, 1, testutil.CollectAndCount(yamuxReadSizeHistogram))
}

func TestInstrumentedConn_Passthrough(t *testing.T) {
	clientPipe, serverPipe := net.Pipe()
	defer clientPipe.Close()
	defer serverPipe.Close()

	instrumentedClient := newInstrumentedConn(clientPipe)

	require.Equal(t, clientPipe.LocalAddr(), instrumentedClient.LocalAddr())
	require.Equal(t, clientPipe.RemoteAddr(), instrumentedClient.RemoteAddr())
}
