package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"mdai-dd-fidelity-validator/internal/validator"
)

func main() {
	adminAddr := envOrDefault("MDAI_ADMIN_ADDR", ":8080")
	receiverProxyAddr := envOrDefault("MDAI_RECEIVER_PROXY_ADDR", ":8081")
	exporterProxyAddr := envOrDefault("MDAI_EXPORTER_PROXY_ADDR", ":8082")
	datadogAPIAddr := envOrDefault("MDAI_DATADOG_API_ADDR", ":8443")
	datadogAPIHosts := csvEnvOrDefault("MDAI_DATADOG_API_TLS_HOSTS", []string{"api.datadoghq.local"})
	retention := durationEnvOrDefault("MDAI_RETENTION", 30*time.Minute)
	receiverUpstream := os.Getenv("MDAI_RECEIVER_UPSTREAM")
	exporterUpstream := os.Getenv("MDAI_EXPORTER_UPSTREAM")

	svc, err := validator.NewService(retention, receiverUpstream, exporterUpstream)
	if err != nil {
		log.Fatal(err)
	}

	adminServer := &http.Server{
		Addr:    adminAddr,
		Handler: svc.AdminRoutes(),
	}
	receiverServer := &http.Server{
		Addr:    receiverProxyAddr,
		Handler: svc.ProxyRoutes("receiver"),
	}
	exporterServer := &http.Server{
		Addr:    exporterProxyAddr,
		Handler: svc.ProxyRoutes("exporter"),
	}
	datadogAPIServer := &http.Server{
		Addr:    datadogAPIAddr,
		Handler: svc.DatadogAPIRoutes(),
	}
	datadogTLSConfig, err := selfSignedTLSConfig(datadogAPIHosts)
	if err != nil {
		log.Fatal(err)
	}
	datadogAPIServer.TLSConfig = datadogTLSConfig

	go serve("admin", adminServer)
	go serve("receiver-proxy", receiverServer)
	go serve("exporter-proxy", exporterServer)
	go serveHTTPOrTLS("datadog-api", datadogAPIServer, datadogTLSConfig)

	log.Printf("mdai-dd-fidelity-validator admin=%s receiver-proxy=%s exporter-proxy=%s datadog-api=%s", adminAddr, receiverProxyAddr, exporterProxyAddr, datadogAPIAddr)

	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-stopCtx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, server := range []*http.Server{adminServer, receiverServer, exporterServer, datadogAPIServer} {
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown error on %s: %v", server.Addr, err)
		}
	}
}

func serve(name string, server *http.Server) {
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("%s server failed: %v", name, err)
	}
}

func serveTLS(name string, server *http.Server) {
	if err := server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
		log.Fatalf("%s tls server failed: %v", name, err)
	}
}

func serveHTTPOrTLS(name string, server *http.Server, tlsConfig *tls.Config) {
	rootListener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		log.Fatalf("%s listener failed: %v", name, err)
	}

	httpListener := newSplitListener(rootListener.Addr())
	tlsListener := newSplitListener(rootListener.Addr())
	server.RegisterOnShutdown(func() {
		_ = rootListener.Close()
		_ = httpListener.Close()
		_ = tlsListener.Close()
	})

	go func() {
		if err := server.Serve(httpListener); err != nil && err != http.ErrServerClosed {
			log.Fatalf("%s http server failed: %v", name, err)
		}
	}()
	go func() {
		tlsServer := &http.Server{
			Addr:    server.Addr,
			Handler: server.Handler,
		}
		if err := tlsServer.Serve(tls.NewListener(tlsListener, tlsConfig)); err != nil && err != http.ErrServerClosed {
			log.Fatalf("%s tls server failed: %v", name, err)
		}
	}()

	for {
		conn, err := rootListener.Accept()
		if err != nil {
			if isClosedNetworkError(err) {
				return
			}
			log.Fatalf("%s accept failed: %v", name, err)
		}

		go func(conn net.Conn) {
			buffered := newBufferedConn(conn)
			if buffered.isTLS() {
				tlsListener.push(buffered)
				return
			}
			httpListener.push(buffered)
		}(conn)
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func durationEnvOrDefault(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		parsed, err := time.ParseDuration(value)
		if err == nil {
			return parsed
		}
		log.Printf("invalid duration for %s=%q, using default %s", key, value, fallback)
	}

	return fallback
}

func csvEnvOrDefault(key string, fallback []string) []string {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func selfSignedTLSConfig(hosts []string) (*tls.Config, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return nil, err
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: hosts[0],
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              hosts,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
	}, nil
}

type splitListener struct {
	addr   net.Addr
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func newSplitListener(addr net.Addr) *splitListener {
	return &splitListener{
		addr:   addr,
		conns:  make(chan net.Conn),
		closed: make(chan struct{}),
	}
}

func (l *splitListener) Accept() (net.Conn, error) {
	select {
	case conn, ok := <-l.conns:
		if !ok {
			return nil, net.ErrClosed
		}
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *splitListener) Close() error {
	l.once.Do(func() {
		close(l.closed)
		close(l.conns)
	})
	return nil
}

func (l *splitListener) Addr() net.Addr {
	return l.addr
}

func (l *splitListener) push(conn net.Conn) {
	select {
	case l.conns <- conn:
	case <-l.closed:
		_ = conn.Close()
	}
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func newBufferedConn(conn net.Conn) *bufferedConn {
	return &bufferedConn{
		Conn:   conn,
		reader: bufio.NewReader(conn),
	}
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *bufferedConn) isTLS() bool {
	peek, err := c.reader.Peek(1)
	if err != nil || len(peek) == 0 {
		return false
	}
	return peek[0] == 0x16
}

func isClosedNetworkError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "use of closed network connection")
}
