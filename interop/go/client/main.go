// Interop client using quic-go/webtransport-go.
//
// Speaks the same protocol as interop_client.erl.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/quic-go/webtransport-go"
)

func main() {
	host := flag.String("host", "localhost", "server host")
	port := flag.String("port", "443", "server port")
	testcase := flag.String("testcase", "handshake", "testcase")
	www := flag.String("www", "/app/www", "document root")
	flag.Parse()

	log.Printf("go interop client testcase=%s host=%s:%s", *testcase, *host, *port)

	d := &webtransport.Dialer{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h3"}},
	}
	url := fmt.Sprintf("https://%s:%s/interop", *host, *port)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, sess, err := d.Dial(ctx, url, http.Header{})
	if err != nil {
		log.Fatalf("TEST FAILED: %s: dial: %v", *testcase, err)
	}
	defer sess.CloseWithError(0, "")

	if err := run(*testcase, sess, *www); err != nil {
		log.Fatalf("TEST FAILED: %s: %v", *testcase, err)
	}
	fmt.Printf("TEST PASSED: %s\n", *testcase)
	os.Exit(0)
}

func run(tc string, sess *webtransport.Session, www string) error {
	switch tc {
	case "handshake":
		return nil
	case "transfer", "transfer-bidirectional":
		return bidiTransfer(sess, "/small.txt", www)
	case "transfer-unidirectional":
		return uniTransfer(sess, "/small.txt", www)
	case "transfer-datagram":
		return datagramEcho(sess)
	default:
		return fmt.Errorf("unknown testcase %q", tc)
	}
}

func bidiTransfer(sess *webtransport.Session, path, www string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := sess.OpenStreamSync(ctx)
	if err != nil {
		return fmt.Errorf("open bidi: %w", err)
	}
	if _, err := stream.Write([]byte("GET " + path + "\n")); err != nil {
		return err
	}
	stream.Close()

	data, err := io.ReadAll(stream)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	return verify(path, data, www)
}

func uniTransfer(sess *webtransport.Session, path, www string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := sess.OpenUniStreamSync(ctx)
	if err != nil {
		return fmt.Errorf("open uni: %w", err)
	}
	if _, err := out.Write([]byte("GET " + path + "\n")); err != nil {
		return err
	}
	out.Close()

	in, err := sess.AcceptUniStream(ctx)
	if err != nil {
		return fmt.Errorf("accept reply uni: %w", err)
	}
	data, err := io.ReadAll(in)
	if err != nil {
		return fmt.Errorf("read uni reply: %w", err)
	}
	return verify(path, data, www)
}

func datagramEcho(sess *webtransport.Session) error {
	payload := []byte("ping")
	if err := sess.SendDatagram(payload); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	msg, err := sess.ReceiveDatagram(ctx)
	if err != nil {
		return err
	}
	if string(msg) != string(payload) {
		return fmt.Errorf("datagram mismatch: got %q want %q", msg, payload)
	}
	return nil
}

func verify(path string, raw []byte, www string) error {
	name, payload, ok := parseResponse(raw)
	if !ok {
		return fmt.Errorf("bad response (%d bytes)", len(raw))
	}
	full := filepath.Join(www, strings.TrimPrefix(path, "/"))
	expected, err := os.ReadFile(full)
	if err != nil {
		return fmt.Errorf("load expected: %w", err)
	}
	if string(payload) != string(expected) {
		return fmt.Errorf("content mismatch: got %d bytes want %d", len(payload), len(expected))
	}
	log.Printf("verified %d bytes match %s (name=%s)", len(payload), full, name)
	return nil
}

func parseResponse(data []byte) (name string, payload []byte, ok bool) {
	const prefix = "PUSH "
	s := string(data)
	if !strings.HasPrefix(s, prefix) {
		return "", nil, false
	}
	rest := s[len(prefix):]
	nl := strings.IndexByte(rest, '\n')
	if nl < 0 {
		return "", nil, false
	}
	return rest[:nl], []byte(rest[nl+1:]), true
}
