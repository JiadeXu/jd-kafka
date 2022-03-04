package client

import (
	"bytes"
	"testing"
)

func TestCutToLastMessage(t *testing.T) {
	res := []byte("100\n101\n10")
	wantTruncated, wantRest := []byte("100\n101\n"), []byte("10")
	gotTruncated, gotRest, err := cutToLastMessage(res)
	if err != nil {
		t.Errorf("CutToLastMessage(%q) error", err)
		return
	}

	if !bytes.Equal(gotTruncated, wantTruncated) || !bytes.Equal(gotRest, wantRest) {
		t.Errorf("CutToLastMessage(%q): got %q %q; want %q %q",
			string(res), string(gotTruncated),
			string(gotRest), string(wantTruncated), string(wantRest))
	}
}

func TestCutToLastMessageErrors(t *testing.T) {
	res := []byte("100000")
	_, _, err := cutToLastMessage(res)
	if err == nil {
		t.Errorf("CutToLastMessage(%q) no error", err)
		return
	}
}
