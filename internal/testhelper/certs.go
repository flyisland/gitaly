package testhelper

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"
)

// Certificate is a generated certificate.
type Certificate struct {
	CertPath string
	KeyPath  string
}

// GenerateCertificate creates a certificate that can be used to establish TLS protected TCP
// connections.
func GenerateCertificate(tb testing.TB) Certificate {
	tb.Helper()

	rootCert := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(0, 0, 1),
		BasicConstraintsValid: true,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
	}

	rootKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(tb, err)

	rootBytes, err := x509.CreateCertificate(rand.Reader, rootCert, rootCert, &rootKey.PublicKey, rootKey)
	require.NoError(tb, err)

	entityKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(tb, err)

	entityCert := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(0, 0, 1),
		IPAddresses:  []net.IP{net.ParseIP("0.0.0.0"), net.ParseIP("127.0.0.1"), net.ParseIP("::1"), net.ParseIP("::")},
		DNSNames:     []string{"localhost"},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth,
		},
	}

	entityBytes, err := x509.CreateCertificate(rand.Reader, entityCert, rootCert, &entityKey.PublicKey, rootKey)
	require.NoError(tb, err)

	certFile, err := os.CreateTemp(testDirectory, "")
	require.NoError(tb, err)
	defer MustClose(tb, certFile)
	tb.Cleanup(func() {
		require.NoError(tb, os.Remove(certFile.Name()))
	})

	// create chained PEM file with CA and entity cert
	for _, cert := range [][]byte{entityBytes, rootBytes} {
		require.NoError(tb,
			pem.Encode(certFile, &pem.Block{
				Type:  "CERTIFICATE",
				Bytes: cert,
			}),
		)
	}

	keyFile, err := os.CreateTemp(testDirectory, "")
	require.NoError(tb, err)
	defer MustClose(tb, keyFile)
	tb.Cleanup(func() {
		require.NoError(tb, os.Remove(keyFile.Name()))
	})

	entityKeyBytes, err := x509.MarshalECPrivateKey(entityKey)
	require.NoError(tb, err)

	require.NoError(tb,
		pem.Encode(keyFile, &pem.Block{
			Type:  "ECDSA PRIVATE KEY",
			Bytes: entityKeyBytes,
		}),
	)

	return Certificate{
		CertPath: certFile.Name(),
		KeyPath:  keyFile.Name(),
	}
}

// TransportCredentials creates new transport credentials that contain the generated certificates.
func (c Certificate) TransportCredentials(tb testing.TB) credentials.TransportCredentials {
	return credentials.NewTLS(c.TLSConfig(tb))
}

// TLSConfig returns TLS configuration that contains the certificate.
func (c Certificate) TLSConfig(tb testing.TB) *tls.Config {
	return &tls.Config{
		RootCAs:    c.CertPool(tb),
		MinVersion: tls.VersionTLS12,
	}
}

// CertPool creates a new certificate pool containing the certificate.
func (c Certificate) CertPool(tb testing.TB) *x509.CertPool {
	tb.Helper()
	pem := MustReadFile(tb, c.CertPath)
	pool := x509.NewCertPool()
	require.True(tb, pool.AppendCertsFromPEM(pem))
	return pool
}

// Cert returns the parsed certificate.
func (c Certificate) Cert(tb testing.TB) tls.Certificate {
	tb.Helper()
	cert, err := tls.LoadX509KeyPair(c.CertPath, c.KeyPath)
	require.NoError(tb, err)
	return cert
}
