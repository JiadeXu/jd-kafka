package main

import (
	"bytes"
	"fmt"
	"io"
	"jd-kafka/client"
	"log"
	"strconv"
	"strings"
)

const maxN = 10000000
const maxBufferSize = 1024 * 1024

func main() {
	s := client.NewSimple([]string{"localhost"})
	want, err := send(s)
	if err != nil {
		log.Fatalf("Send error: %v", err)
	}
	fmt.Println(want)
	got, err := receive(s)
	if err != nil {
		log.Fatalf("Receive error: %v", err)
	}

	if want != got {
		log.Fatalf("want(%d) != got(%d)", want, got)
	}

	log.Printf("want(%d) == got(%d)", want, got)
}

func send(s *client.Simple) (sum int64, err error) {
	var b bytes.Buffer

	var i int64
	for i = 0; i <= maxN; i++ {
		fmt.Fprintf(&b, "%d\n", i)
		sum += i
		if b.Len() >= maxBufferSize {
			if err := s.Send(b.Bytes()); err != nil {
				//log.Fatalf("Send error: %v", err)
				return 0, err
			}
			b.Reset()
		}
	}

	if b.Len() != 0 {
		if err := s.Send(b.Bytes()); err != nil {
			//log.Fatalf("Send error: %v", err)
			return 0, err
		}
		b.Reset()
	}
	return sum, nil
}

func receive(s *client.Simple) (got int64, err error) {
	buf := make([]byte, maxBufferSize)
	for {
		res, err := s.Receive(buf)
		if err == io.EOF {
			return got, nil
		} else if err != nil {
			return 0, err
		}

		ints := strings.Split(string(res), "\n")
		for _, str := range ints {
			if str == "" {
				continue
			}
			i, err := strconv.Atoi(str)
			if err != nil {
				return 0, err
			}
			got += int64(i)
		}
	}
}
