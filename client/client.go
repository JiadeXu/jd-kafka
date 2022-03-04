package client

import (
	"bytes"
	"errors"
)

const defaultScratchSize = 1024

var errBufTooSmall = errors.New("buffer is too small to fit the message")

// Simple represents an instance of client connected to a set of jd-kafka servers
type Simple struct {
	addr []string

	buf     bytes.Buffer
	restBuf bytes.Buffer
}

// NewSimple creates a new client for the jd-kafka server
func NewSimple(addr []string) *Simple {
	return &Simple{
		addr: addr,
	}
}

// Send sends the messages to the jd-kafka servers
func (s *Simple) Send(msgs []byte) error {
	_, err := s.buf.Write(msgs)
	return err
}

// Receive will either wait for new messages or return an
// error in case something goes wrong.
// THe scratch buffer can used to read the data
func (s *Simple) Receive(scratch []byte) ([]byte, error) {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)
	}

	startDff := 0

	if s.restBuf.Len() > 0 {
		if s.restBuf.Len() >= len(scratch) {
			return nil, errBufTooSmall
		}

		n, err := s.restBuf.Read(scratch)
		if err != nil {
			return nil, err
		}

		s.restBuf.Reset()
		startDff += n
	}

	n, err := s.buf.Read(scratch[startDff:])
	if err != nil {
		return nil, err
	}

	truncated, rest, err := cutToLastMessage(scratch[0 : n+startDff])

	if err != nil {
		return nil, err
	}

	s.restBuf.Reset()
	s.restBuf.Write(rest)

	_ = rest

	return truncated, nil
}

// cutToLastMessage 将最后一个字符不是\n的字符串切分 "100\n101\n10" -> "100\n101\n" "10"
func cutToLastMessage(res []byte) (truncated []byte, rest []byte, err error) {
	n := len(res)
	if n == 0 {
		return res, nil, nil
	}

	if res[n-1] == '\n' {
		return res, nil, nil
	}

	lastPos := bytes.LastIndexByte(res, '\n')
	if lastPos < 0 {
		return nil, nil, errBufTooSmall
	}
	return res[0 : lastPos+1], res[lastPos+1:], nil
}
