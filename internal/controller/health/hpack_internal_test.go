// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"runtime"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

const (
	http2Data         = 0
	http2Headers      = 1
	http2Settings     = 4
	http2Continuation = 9

	http2EndStream  = 0x1
	http2EndHeaders = 0x4
	http2Ack        = 0x1

	hpackStatus200   = 0x88
	hpackTableToZero = 0x20
)

func http2Frame(kind, flags byte, stream uint32, payload []byte) []byte {
	frame := []byte{byte(len(payload) >> 16), byte(len(payload) >> 8), byte(len(payload)), kind, flags, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(frame[5:], stream)
	return append(frame, payload...)
}

func hpackString(s string) []byte {
	const prefix = 127
	out := []byte{byte(min(len(s), prefix))}
	if len(s) >= prefix {
		for rest := len(s) - prefix; ; rest >>= 7 {
			if rest < 128 {
				out = append(out, byte(rest))
				break
			}
			out = append(out, byte(rest&127|128))
		}
	}
	return append(out, s...)
}

func hpackLiteral(name, value string) []byte {
	return append(append([]byte{0}, hpackString(name)...), hpackString(value)...)
}

func plainHTTP2Answer(stream uint32) [][]byte {
	head := append([]byte{hpackStatus200}, hpackLiteral("content-type", "application/json")...)
	return [][]byte{http2Frame(http2Headers, http2EndHeaders, stream, head), http2Frame(http2Data, http2EndStream, stream, []byte("{}"))}
}

func headOfFrames(stream uint32, fieldRunsAcross bool) [][]byte {
	field := hpackLiteral(strings.Repeat("#", budgetHTTP2), strings.Repeat("\xe9", budgetHTTP2))
	block := append([]byte{hpackStatus200}, field...)
	if !fieldRunsAcross {
		block = append([]byte{hpackStatus200}, bytes.Repeat([]byte{hpackTableToZero}, len(field))...)
	}
	var frames [][]byte
	for kind := byte(http2Headers); len(block) > 0; kind = http2Continuation {
		n := min(frameSize, len(block))
		var flags byte
		if kind == http2Headers {
			flags = http2EndStream
		}
		if n == len(block) {
			flags |= http2EndHeaders
		}
		frames = append(frames, http2Frame(kind, flags, stream, block[:n]))
		block = block[n:]
	}
	return frames
}

func serveHTTP2(c *tls.Conn, answers func(n int, stream uint32) [][]byte) {
	defer func() { _ = c.Close() }()
	if _, err := io.CopyN(io.Discard, c, int64(len("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"))); err != nil {
		return
	}
	if _, err := c.Write(http2Frame(http2Settings, 0, 0, nil)); err != nil {
		return
	}
	head := make([]byte, 9)
	requests := 0
	for {
		if _, err := io.ReadFull(c, head); err != nil {
			return
		}
		if _, err := io.CopyN(io.Discard, c, int64(head[0])<<16|int64(head[1])<<8|int64(head[2])); err != nil {
			return
		}
		switch head[3] {
		case http2Settings:
			if head[4]&http2Ack == 0 {
				if _, err := c.Write(http2Frame(http2Settings, http2Ack, 0, nil)); err != nil {
					return
				}
			}
		case http2Headers:
			requests++
			for _, frame := range answers(requests, binary.BigEndian.Uint32(head[5:])&(1<<31-1)) {
				if _, err := c.Write(frame); err != nil {
					return
				}
			}
		}
	}
}

func handHTTP2Client(t *testing.T, answers func(n int, stream uint32) [][]byte) (*http.Client, string) {
	t.Helper()
	server := httptest.NewUnstartedServer(http.NotFoundHandler())
	server.EnableHTTP2 = true
	server.Config.TLSNextProto = map[string]func(*http.Server, *tls.Conn, http.Handler){
		"h2": func(_ *http.Server, c *tls.Conn, _ http.Handler) { serveHTTP2(c, answers) },
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
	r := &NodesReconciler{Timeout: time.Minute, MaxNodes: 5000}
	if _, err := r.tenant(key, kubeconfigForTLS(t, server)); err != nil {
		t.Fatal(err)
	}
	return r.clients[key].transport, server.URL
}

func TestAnHTTP2ConnectionReadsNoFrameOverFrameSize(t *testing.T) {
	for _, c := range []struct {
		size int
		want bool
	}{{frameSize, true}, {frameSize + 1, false}} {
		client, url := handHTTP2Client(t, func(_ int, stream uint32) [][]byte {
			head := append([]byte{hpackStatus200}, hpackLiteral("content-type", "application/json")...)
			return [][]byte{http2Frame(http2Headers, http2EndHeaders, stream, head), http2Frame(http2Data, http2EndStream, stream, bytes.Repeat([]byte("a"), c.size))}
		})
		resp, err := client.Get(url + nodeLeasesPath)
		if err == nil {
			var read int64
			read, err = io.Copy(io.Discard, resp.Body)
			if err == nil && read != int64(c.size) {
				t.Errorf("a frame of %d bytes was read as %d", c.size, read)
			}
		}
		if (err == nil) != c.want || (err != nil && !strings.Contains(err.Error(), "frame too large")) {
			t.Errorf("a DATA frame of %d bytes was read with error %v, want it read: %t, or refused as too large", c.size, err, c.want)
		}
	}
}

func TestAnHTTP2ConnectionKeepsHpacksBufferOfAFieldRunningAcrossFramesForItsLifeAndNoMoreThanHpackBuffer(t *testing.T) {
	sizedForTheBounds(t)
	const slack = 40 << 10
	read := func(client *http.Client, url string) (reused bool, err error) {
		req, _ := http.NewRequestWithContext(httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused }}), http.MethodGet, url+nodeLeasesPath, nil)
		resp, err := client.Do(req)
		if err != nil {
			return reused, err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		return reused, resp.Body.Close()
	}
	held := func(split bool) int64 {
		return medianOf(func() int64 {
			client, url := handHTTP2Client(t, func(n int, stream uint32) [][]byte {
				if n == 1 {
					return headOfFrames(stream, split)
				}
				return plainHTTP2Answer(stream)
			})
			base := liveHeap()
			if _, err := read(client, url); split != (err != nil) || err != nil && !strings.Contains(err.Error(), "response header list larger than advertised limit") {
				t.Fatalf("the head of frames was read with error %v, want the stream's error of a head past the bound if its field runs across them: %t", err, split)
			}
			if reused, err := read(client, url); err != nil || !reused {
				t.Fatalf("the next read reused the connection: %t, error %v, want the connection to serve it", reused, err)
			}
			now := liveHeap()
			runtime.KeepAlive(client)
			return int64(now) - int64(base)
		})
	}
	whole, split := held(false), held(true)
	t.Logf("a connection that read a head of nine frames keeps %d bytes with no field running across its frames and %d with one past the bound, %d more; hpackBuffer is %d, connectionHTTP2 is %d", whole, split, split-whole, hpackBuffer, connectionHTTP2)
	if more := split - whole; more < hpackBuffer-slack || more > hpackBuffer+slack {
		t.Errorf("a field running across the frames leaves %d bytes more on the connection, want the hpackBuffer of %d within %d, the spread of TLS's buffers between two connections", more, hpackBuffer, slack)
	}
	if split > connectionHTTP2 {
		t.Errorf("the connection keeps %d bytes, more than connectionHTTP2, %d", split, connectionHTTP2)
	}
}

func TestAnHTTP2ConnectionThatSentTheLongestQueryHealthSendsKeepsItsEncoderWithinTheMargin(t *testing.T) {
	longest := "?limit=500&continue=" + strings.Repeat("%FF", 12288)
	held := func(query string) int64 {
		return medianOf(func() int64 {
			client, url := handHTTP2Client(t, func(_ int, stream uint32) [][]byte { return plainHTTP2Answer(stream) })
			base := liveHeap()
			resp, err := client.Get(url + nodeLeasesPath + query)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			now := liveHeap()
			runtime.KeepAlive(client)
			return int64(now) - int64(base)
		})
	}
	plain, long := held(""), held(longest)
	t.Logf("a connection keeps %d bytes after a plain request and %d after one whose query is %d bytes, %d more; connectionMargin is %d", plain, long, len(longest)-1, long-plain, connectionMargin)
	if more := long - plain; more < 64<<10 || more > connectionMargin {
		t.Errorf("the longest query leaves %d bytes of encoder on the connection, want 64 KiB to connectionMargin, %d", more, connectionMargin)
	}
}
