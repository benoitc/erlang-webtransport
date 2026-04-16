// Interop server using quic-go/webtransport-go.
//
// Speaks the same request/response protocol as interop_server.erl:
//   request:  GET /path\n
//   response: PUSH <basename>\n<payload>
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

	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
)

func main() {
	port := flag.String("port", "443", "UDP port")
	certFile := flag.String("cert", "/app/certs/cert.pem", "TLS cert")
	keyFile := flag.String("key", "/app/certs/key.pem", "TLS key")
	www := flag.String("www", "/app/www", "document root")
	flag.Parse()

	cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		log.Fatalf("load cert: %v", err)
	}

	s := &webtransport.Server{
		H3: http3.Server{
			Addr: ":" + *port,
			TLSConfig: &tls.Config{
				Certificates: []tls.Certificate{cert},
				NextProtos:   []string{"h3"},
			},
		},
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/interop", func(w http.ResponseWriter, r *http.Request) {
		sess, err := s.Upgrade(w, r)
		if err != nil {
			log.Printf("upgrade: %v", err)
			return
		}
		go handleSession(sess, *www)
	})
	s.H3.Handler = mux

	log.Printf("go interop server listening on :%s", *port)
	if err := s.ListenAndServe(); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func handleSession(sess *webtransport.Session, www string) {
	ctx := sess.Context()
	go acceptBidi(ctx, sess, www)
	go acceptUni(ctx, sess, www)
	go echoDatagrams(ctx, sess)
	<-ctx.Done()
}

func acceptBidi(ctx context.Context, sess *webtransport.Session, www string) {
	for {
		stream, err := sess.AcceptStream(ctx)
		if err != nil {
			return
		}
		go serveBidi(stream, www)
	}
}

func serveBidi(stream *webtransport.Stream, www string) {
	data, err := io.ReadAll(stream)
	if err != nil {
		log.Printf("read bidi: %v", err)
		return
	}
	resp := buildResponse(data, www)
	if _, err := stream.Write(resp); err != nil {
		log.Printf("write bidi: %v", err)
	}
	stream.Close()
}

func acceptUni(ctx context.Context, sess *webtransport.Session, www string) {
	for {
		stream, err := sess.AcceptUniStream(ctx)
		if err != nil {
			return
		}
		go serveUni(sess, stream, www)
	}
}

func serveUni(sess *webtransport.Session, in *webtransport.ReceiveStream, www string) {
	data, err := io.ReadAll(in)
	if err != nil {
		log.Printf("read uni: %v", err)
		return
	}
	out, err := sess.OpenUniStream()
	if err != nil {
		log.Printf("open reply uni: %v", err)
		return
	}
	if _, err := out.Write(buildResponse(data, www)); err != nil {
		log.Printf("write uni: %v", err)
	}
	out.Close()
}

func echoDatagrams(ctx context.Context, sess *webtransport.Session) {
	for {
		msg, err := sess.ReceiveDatagram(ctx)
		if err != nil {
			return
		}
		if err := sess.SendDatagram(msg); err != nil {
			log.Printf("send datagram: %v", err)
			return
		}
	}
}

func buildResponse(request []byte, www string) []byte {
	path, ok := parseRequest(request)
	if !ok {
		return []byte("ERROR: Invalid request\n")
	}
	full := filepath.Join(www, strings.TrimPrefix(path, "/"))
	content, err := os.ReadFile(full)
	if err != nil {
		return []byte("ERROR: File not found\n")
	}
	name := filepath.Base(full)
	return []byte(fmt.Sprintf("PUSH %s\n%s", name, content))
}

func parseRequest(data []byte) (string, bool) {
	const prefix = "GET "
	s := string(data)
	if !strings.HasPrefix(s, prefix) {
		return "", false
	}
	rest := s[len(prefix):]
	nl := strings.IndexByte(rest, '\n')
	if nl < 0 {
		return "", false
	}
	return rest[:nl], true
}
