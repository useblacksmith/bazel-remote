package grpcproxy

import (
	"errors"
	"io"
	"testing"
)

type fakeMsg struct {
	data []byte
}

func (m *fakeMsg) GetData() []byte {
	if m == nil {
		return nil
	}
	return m.data
}

// fakeRecvStream yields its messages in order, then returns its terminal
// error on every subsequent Recv — the same semantics as a
// grpc.ClientStream (io.EOF for a clean end, the status error otherwise).
type fakeRecvStream struct {
	msgs []*fakeMsg
	err  error
}

func (s *fakeRecvStream) Recv() (*fakeMsg, error) {
	if len(s.msgs) == 0 {
		return nil, s.err
	}
	m := s.msgs[0]
	s.msgs = s.msgs[1:]
	return m, nil
}

func (s *fakeRecvStream) CloseSend() error { return nil }

// Regression test: Read used to return n = -1 alongside any mid-stream
// error, violating the io.Reader contract ("n >= 0") and panicking
// io.ReadAll with "slice bounds out of range [:-1]". A stream that errors
// mid-read must instead surface the error with a non-negative count.
func TestStreamReadCloserMidStreamErrorReadAll(t *testing.T) {
	sentinel := errors.New("stream died mid-read")
	rc := &StreamReadCloser[*fakeMsg]{Stream: &fakeRecvStream{
		msgs: []*fakeMsg{{data: []byte("partial ")}, {data: []byte("payload")}},
		err:  sentinel,
	}}

	data, err := io.ReadAll(rc) // panicked before the fix
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want %v", err, sentinel)
	}
	if got := string(data); got != "partial payload" {
		t.Fatalf("data = %q, want the bytes delivered before the error", got)
	}
}

// A Recv error in the same Read call that drained buffered bytes must
// report those bytes, not discard them behind a negative count.
func TestStreamReadCloserErrorAfterBufferedBytes(t *testing.T) {
	sentinel := errors.New("recv failed")
	rc := &StreamReadCloser[*fakeMsg]{
		Stream: &fakeRecvStream{err: sentinel},
		buf:    []byte("abc"),
	}

	p := make([]byte, 8)
	n, err := rc.Read(p)
	if n != 3 || !errors.Is(err, sentinel) {
		t.Fatalf("Read = (%d, %v), want (3, %v)", n, err, sentinel)
	}
	if string(p[:n]) != "abc" {
		t.Fatalf("p[:n] = %q, want %q", p[:n], "abc")
	}
}

func TestStreamReadCloserImmediateErrorReturnsZeroN(t *testing.T) {
	sentinel := errors.New("recv failed")
	rc := &StreamReadCloser[*fakeMsg]{Stream: &fakeRecvStream{err: sentinel}}

	n, err := rc.Read(make([]byte, 4))
	if n != 0 || !errors.Is(err, sentinel) {
		t.Fatalf("Read = (%d, %v), want (0, %v)", n, err, sentinel)
	}
}

func TestStreamReadCloserCleanEOF(t *testing.T) {
	rc := &StreamReadCloser[*fakeMsg]{Stream: &fakeRecvStream{
		msgs: []*fakeMsg{{data: []byte("hello ")}, {data: []byte("world")}},
		err:  io.EOF,
	}}

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if got := string(data); got != "hello world" {
		t.Fatalf("data = %q, want %q", got, "hello world")
	}
}
