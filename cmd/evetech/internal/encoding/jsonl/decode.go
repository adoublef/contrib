package jsonl

import (
	"encoding/json"
	"io"
	"iter"
)

func Decode[T any](r io.Reader) iter.Seq2[T, error] {
	d := json.NewDecoder(r)
	var zero T
	return func(yield func(T, error) bool) {
		if _, err := d.Token(); err != nil && !yield(zero, err) {
			return
		}
		for d.More() {
			var v T
			if err := d.Decode(&v); !yield(v, err) {
				return
			}
		}
		if _, err := d.Token(); err != nil && !yield(zero, err) {
			return
		}
	}
}
