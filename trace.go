package httpx

import (
	"crypto/tls"
	"net/http/httptrace"
	"time"
)

func (t *TraceInfo) hooks() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) {
			t.mu.Lock()
			t.dnsStart = time.Now()
			t.mu.Unlock()
		},
		DNSDone: func(httptrace.DNSDoneInfo) {
			t.mu.Lock()
			if !t.dnsStart.IsZero() {
				t.DNSLookup += time.Since(t.dnsStart)
			}
			t.mu.Unlock()
		},
		ConnectStart: func(_, _ string) {
			t.mu.Lock()
			if t.connectStart.IsZero() {
				t.connectStart = time.Now()
			}
			t.mu.Unlock()
		},
		ConnectDone: func(_, _ string, _ error) {
			t.mu.Lock()
			if !t.connectStart.IsZero() && t.TCPConnTime == 0 {
				t.TCPConnTime = time.Since(t.connectStart)
			}
			t.mu.Unlock()
		},
		TLSHandshakeStart: func() {
			t.mu.Lock()
			t.tlsStart = time.Now()
			t.mu.Unlock()
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, _ error) {
			t.mu.Lock()
			if !t.tlsStart.IsZero() {
				t.TLSHandshake += time.Since(t.tlsStart)
			}
			if !t.connectStart.IsZero() && t.ConnTime == 0 {
				t.ConnTime = time.Since(t.connectStart)
			}
			t.mu.Unlock()
		},
		GotConn: func(info httptrace.GotConnInfo) {
			t.mu.Lock()
			t.IsConnReused = info.Reused
			t.IsConnWasIdle = info.WasIdle
			t.ConnIdleTime = info.IdleTime
			if info.Conn != nil && info.Conn.RemoteAddr() != nil {
				t.RemoteAddr = info.Conn.RemoteAddr().String()
			}
			if !t.connectStart.IsZero() && t.ConnTime == 0 {
				t.ConnTime = time.Since(t.connectStart)
			}
			t.gotConnAt = time.Now()
			t.mu.Unlock()
		},
		WroteRequest: func(httptrace.WroteRequestInfo) {
			t.mu.Lock()
			t.requestWritten = time.Now()
			t.mu.Unlock()
		},
		GotFirstResponseByte: func() {
			t.mu.Lock()
			t.firstByte = time.Now()
			if !t.requestWritten.IsZero() {
				t.ServerTime = t.firstByte.Sub(t.requestWritten)
			}
			t.mu.Unlock()
		},
	}
}
