// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package jsonscan reads the few fields a program keeps of a JSON document
// that encoding/json has validated, and decodes nothing else: a value is a
// slice of the document, and a scalar is decoded only after its size is checked
// against the bound its caller names. The documents are a hostile server's,
// whose size encoding/json's decoding multiplies: an array of empty objects
// becomes a struct each, an invalid byte in a string three, and a key that is
// skipped is unquoted and folded all the same.
//
// Every function takes a document for which json.Valid is true, and may panic
// on any other.
package jsonscan

import (
	"bytes"
	"encoding/json"
	"errors"
	"iter"
	"strconv"
)

// MaxKey is the longest key, in bytes between its quotes, that Walk unquotes
// when it carries an escape: no field a caller reads is named by a longer one.
const MaxKey = 256

// TooLong is the error of a token past its bound. It names the field and the
// bound and nothing of the value.
type TooLong struct {
	Field string
	Most  int
}

func (e *TooLong) Error() string {
	return e.Field + " is longer than " + strconv.Itoa(e.Most) + " bytes"
}

func NotA(field, kind string) error {
	return errors.New(field + " is not " + kind)
}

func Null(value []byte) bool {
	return string(value) == "null"
}

// Within says whether the raw token value is at most most bytes, a string
// without its quotes.
func Within(value []byte, most int) bool {
	if value[0] == '"' {
		return len(value)-2 <= most
	}
	return len(value) <= most
}

func Text(field string, value []byte, most int) (string, error) {
	switch {
	case Null(value):
		return "", nil
	case value[0] != '"':
		return "", NotA(field, "a string")
	case !Within(value, most):
		return "", &TooLong{field, most}
	}
	var s string
	if err := json.Unmarshal(value, &s); err != nil {
		return "", NotA(field, "a string")
	}
	return s, nil
}

// Word is Text for a field a caller only compares with words: a string past
// most bytes is none of them, and is neither decoded nor an error.
func Word(field string, value []byte, most int) (string, error) {
	if value[0] == '"' && !Within(value, most) {
		return "", nil
	}
	return Text(field, value, most)
}

func Integer(field string, value []byte, most int) (*int32, error) {
	switch {
	case Null(value):
		return nil, nil
	case !Within(value, most):
		return nil, &TooLong{field, most}
	}
	n, err := strconv.ParseInt(string(value), 10, 32)
	if err != nil {
		return nil, NotA(field, "an integer")
	}
	return new(int32(n)), nil
}

func Boolean(field string, value []byte) (*bool, error) {
	switch string(value) {
	case "null":
		return nil, nil
	case "true":
		return new(true), nil
	case "false":
		return new(false), nil
	}
	return nil, NotA(field, "a boolean")
}

// Walk calls visit with the key and the raw value of each member of the object
// in data, in order, and stops at its first error. A json.Decoder would copy
// each value into a buffer of its own, and a value may be most of the document.
// A key is matched on its raw bytes; one with an escape is unquoted when it is
// at most MaxKey bytes, and any other key is skipped without being copied.
// null has no members.
func Walk(field string, data []byte, visit func(key, value []byte) error) error {
	switch {
	case Null(data):
		return nil
	case data[0] != '{':
		return NotA(field, "an object")
	}
	i := skipSpace(data, 1)
	for data[i] != '}' {
		keyEnd := endOfString(data, i)
		key, named := keyOf(data[i:keyEnd])
		i = skipSpace(data, skipSpace(data, keyEnd)+1)
		end := endOfValue(data, i)
		if named {
			if err := visit(key, data[i:end]); err != nil {
				return err
			}
		}
		i = skipSpace(data, end)
		if data[i] == ',' {
			i = skipSpace(data, i+1)
		}
	}
	return nil
}

func keyOf(token []byte) ([]byte, bool) {
	raw := token[1 : len(token)-1]
	if bytes.IndexByte(raw, '\\') < 0 {
		return raw, true
	}
	var key string
	if len(raw) > MaxKey || json.Unmarshal(token, &key) != nil {
		return nil, false
	}
	return []byte(key), true
}

func Elements(array []byte) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		i := skipSpace(array, 1)
		for array[i] != ']' {
			end := endOfValue(array, i)
			if !yield(array[i:end]) {
				return
			}
			i = skipSpace(array, end)
			if array[i] == ',' {
				i = skipSpace(array, i+1)
			}
		}
	}
}

func skipSpace(data []byte, i int) int {
	for data[i] == ' ' || data[i] == '\t' || data[i] == '\n' || data[i] == '\r' {
		i++
	}
	return i
}

func endOfValue(data []byte, i int) int {
	switch data[i] {
	case '"':
		return endOfString(data, i)
	case '{', '[':
		for depth := 0; ; i++ {
			switch data[i] {
			case '"':
				i = endOfString(data, i) - 1
			case '{', '[':
				depth++
			case '}', ']':
				if depth--; depth == 0 {
					return i + 1
				}
			}
		}
	}
	for data[i] != ',' && data[i] != ']' && data[i] != '}' && data[i] != ' ' && data[i] != '\t' && data[i] != '\n' && data[i] != '\r' {
		i++
	}
	return i
}

func endOfString(data []byte, i int) int {
	for i++; data[i] != '"'; i++ {
		if data[i] == '\\' {
			i++
		}
	}
	return i + 1
}
