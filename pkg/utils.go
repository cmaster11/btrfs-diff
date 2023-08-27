package pkg

import (
	"bufio"
	"github.com/pkg/errors"
)

// peekAndDiscard return n bytes from the stream buffer, if required increase its size
func peekAndDiscard(input *bufio.Reader, n int) ([]byte, error) {
	buffered := input.Buffered()
	if n > buffered {
		input = bufio.NewReaderSize(input, n)
	}
	data, err := input.Peek(n)
	if err != nil {
		return nil, errors.Wrap(err, "failed to peek on input")
	}
	if _, err := input.Discard(n); err != nil {
		return nil, errors.Wrap(err, "failed to discard input")
	}
	return data, nil
}

func ellipsis(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	if maxLen < 3 {
		maxLen = 3
	}
	return string(runes[0:maxLen-3]) + "..."
}
