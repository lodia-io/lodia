// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package jsonscan_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"go.lodia.io/lodia/internal/jsonscan"
)

type member struct {
	raw, key string
	value    []byte
}

// membersByDecoder reads the members of the object in data as a json.Decoder
// does, which copies each value and unquotes each key.
func membersByDecoder(tb testing.TB, data []byte) []member {
	tb.Helper()
	dec := json.NewDecoder(bytes.NewReader(data))
	if _, err := dec.Token(); err != nil {
		tb.Fatal(err)
	}
	var out []member
	for dec.More() {
		start := dec.InputOffset()
		token, err := dec.Token()
		if err != nil {
			tb.Fatal(err)
		}
		quoted := bytes.TrimLeft(data[start:dec.InputOffset()], " \t\r\n,")
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			tb.Fatal(err)
		}
		out = append(out, member{raw: string(quoted[1 : len(quoted)-1]), key: token.(string), value: value})
	}
	return out
}

// expected is what Walk must yield: every member but one whose key has an escape
// and is over MaxKey bytes.
func expected(members []member) []member {
	var out []member
	for _, m := range members {
		switch {
		case !strings.Contains(m.raw, `\`):
			m.key = m.raw
		case len(m.raw) > jsonscan.MaxKey:
			continue
		}
		out = append(out, m)
	}
	return out
}

func walked(tb testing.TB, data []byte) []member {
	tb.Helper()
	var out []member
	err := jsonscan.Walk("the object", data, func(key, value []byte) error {
		if len(out) > len(data) {
			return errors.New("more members than bytes")
		}
		out = append(out, member{key: string(key), value: slices.Clone(value)})
		return nil
	})
	if err != nil {
		tb.Fatal(err)
	}
	return out
}

func sameMembers(tb testing.TB, data []byte) {
	tb.Helper()
	want, got := expected(membersByDecoder(tb, data)), walked(tb, data)
	if len(got) != len(want) {
		tb.Fatalf("Walk yields %d members of %q, want %d", len(got), data, len(want))
	}
	for i := range want {
		if got[i].key != want[i].key || !bytes.Equal(got[i].value, want[i].value) {
			tb.Fatalf("member %d of %q is %q: %q, want %q: %q", i, data, got[i].key, got[i].value, want[i].key, want[i].value)
		}
	}
}

func sameElements(tb testing.TB, data []byte) {
	tb.Helper()
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		tb.Fatal(err)
	}
	var got [][]byte
	for element := range jsonscan.Elements(data) {
		if len(got) > len(data) {
			tb.Fatalf("Elements yields more elements of %q than it has bytes", data)
		}
		got = append(got, element)
	}
	if len(got) != len(raw) {
		tb.Fatalf("Elements yields %d elements of %q, want %d", len(got), data, len(raw))
	}
	for i := range raw {
		if !bytes.Equal(got[i], raw[i]) {
			tb.Fatalf("element %d of %q is %q, want %q", i, data, got[i], raw[i])
		}
	}
}

func TestTheElementsOfAnArrayAreWhatEncodingJSONReadsOfIt(t *testing.T) {
	for _, data := range []string{
		`[]`,
		`[ ]`,
		`[{}]`,
		`[{},{}]`,
		"[ \n\t{ } ,\r\n { \"a\" : [ 1 , 2 ] } ]",
		`[1,22,-3.5e10,true,false,null]`,
		`[ 1 , true , null ]`,
		`["a","b,c","]","\"","\\","\\\"","[","{"]`,
		`[{"a":"}","b":"\"}","c":["]\\"]},[[]],[[[]]],{"a":{"b":{}}}]`,
		`["é😀","é😀"]`,
		`[[],[1],[[2]],{"a":[]}]`,
		`["a\\"]`,
		`[{"k":"v\\"},{"k":"w"}]`,
	} {
		sameElements(t, []byte(data))
	}
}

func TestTheMembersOfAnObjectAreWhatEncodingJSONReadsOfIt(t *testing.T) {
	long := func(n int) string { return strings.Repeat(`\n`, n/2) }
	for _, data := range []string{
		`{}`,
		`{ }`,
		`{"a":1}`,
		`{"a":true}`,
		`{"a":null}`,
		`{"a":-1.5e10,"b":false}`,
		`{"a":1 , "b" : 2 }`,
		"{ \n\t\"a\" : [ 1 , 2 ] ,\r\n \"b\" : { \"c\" : \"d\" } }",
		`{"a":"}","b":"\"}","c":["]\\"],"d":{"e":{"f":[{}]}}}`,
		`{"a":1,"a":2}`,
		`{"a":{"a":{"a":{}}},"a":[[[]]]}`,
		`{"na\u006de":"x","n\u0061me":"y","\u006eame":"z","name":"w"}`,
		`{"na\/me":1,"\"":2,"\\":3,"a\tb":4,"\ud83d\ude00":5}`,
		"{\"\xff\":1,\"a\xff\":2,\"\xc3\":3}",
		`{"` + long(jsonscan.MaxKey-2) + `":1}`,
		`{"` + long(jsonscan.MaxKey) + `":1}`,
		`{"` + long(jsonscan.MaxKey+2) + `":1,"b":2}`,
		`{"` + strings.Repeat("a", 4*jsonscan.MaxKey) + `":1,"b":2}`,
		`{"` + strings.Repeat("\xff", 4*jsonscan.MaxKey) + `":1,"b":2}`,
	} {
		sameMembers(t, []byte(data))
	}
}

func TestAKeyWithAnEscapeIsMatchedUpTo256BytesAndSkippedPast(t *testing.T) {
	for _, c := range []struct {
		name string
		raw  string
		want bool
	}{
		{"a plain key far past it", strings.Repeat("a", 2560), true},
		{"a plain key one byte past it", strings.Repeat("a", 257), true},
		{"a key with an escape at it", strings.Repeat("n", 254) + `\n`, true},
		{"a key with an escape one byte past it", strings.Repeat("n", 255) + `\n`, false},
		{"a key with an escape far past it", strings.Repeat(`\n`, 2560), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := walked(t, []byte(`{"`+c.raw+`":1,"after":2}`))
			want := 1
			if c.want {
				want = 2
			}
			if len(got) != want || got[len(got)-1].key != "after" {
				t.Errorf("Walk yields %d members, want %d ending with after", len(got), want)
			}
		})
	}
}

func TestWalkSkipsAnObjectThatIsNotOneOrIsNull(t *testing.T) {
	visit := func(key, value []byte) error { t.Errorf("visited %q", key); return nil }
	if err := jsonscan.Walk("the object", []byte(`null`), visit); err != nil {
		t.Errorf("null is an error: %v", err)
	}
	for _, data := range []string{`[]`, `"a"`, `1`, `true`} {
		if err := jsonscan.Walk("the object", []byte(data), visit); err == nil || !strings.Contains(err.Error(), "the object is not an object") {
			t.Errorf("%s is read as an object: %v", data, err)
		}
	}
}

func TestWalkStopsAtTheFirstErrorOfItsVisitor(t *testing.T) {
	stop := errors.New("stop")
	var seen []string
	err := jsonscan.Walk("the object", []byte(`{"a":1,"b":2,"c":3}`), func(key, _ []byte) error {
		seen = append(seen, string(key))
		if string(key) == "b" {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || !slices.Equal(seen, []string{"a", "b"}) {
		t.Errorf("Walk returned %v after %q", err, seen)
	}
}

func randomValue(rng *rand.Rand, depth int) any {
	switch n := rng.IntN(8); {
	case n == 0 || depth == 0 && n < 3:
		return rng.IntN(1000) - 500
	case n == 1:
		return rng.Float64() * 1e6
	case n == 2:
		return [...]any{true, false, nil}[rng.IntN(3)]
	case n == 3 || depth == 0:
		runes := []rune("ab\"\\[]{},: \n\t/é😀 ")
		var s strings.Builder
		for range rng.IntN(12) {
			s.WriteRune(runes[rng.IntN(len(runes))])
		}
		return s.String()
	case n < 6:
		out := make([]any, rng.IntN(5))
		for i := range out {
			out[i] = randomValue(rng, depth-1)
		}
		return out
	default:
		out := map[string]any{}
		for range rng.IntN(4) {
			out[strconv.Itoa(rng.IntN(100))+"\"]"] = randomValue(rng, depth-1)
		}
		return out
	}
}

func randomKey(rng *rand.Rand) string {
	switch rng.IntN(8) {
	case 0:
		return `\u006eame`
	case 1:
		return `n\u0061m\u0065`
	case 2:
		return strings.Repeat(`\n`, rng.IntN(jsonscan.MaxKey))
	case 3:
		return strings.Repeat(`\\`, jsonscan.MaxKey/2-2+rng.IntN(5))
	case 4:
		return strings.Repeat("\xff", rng.IntN(2*jsonscan.MaxKey))
	case 5:
		return `a\"b\\c\/d\u00e9\ud83d\ude00`
	case 6:
		return strings.Repeat("k", rng.IntN(2*jsonscan.MaxKey))
	}
	return [...]string{"name", "metadata", "items", "status", "spec"}[rng.IntN(5)]
}

func TestTheMembersOfAnyValidObjectAreWhatEncodingJSONReadsOfIt(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	spaces := [...]string{"", " ", "\n", "\t ", "\r\n"}
	documents := 0
	for range 20000 {
		var doc strings.Builder
		space := func() string { return spaces[rng.IntN(len(spaces))] }
		doc.WriteString("{" + space())
		for i := range rng.IntN(6) {
			if i > 0 {
				doc.WriteString(space() + "," + space())
			}
			value, err := json.Marshal(randomValue(rng, 4))
			if err != nil {
				t.Fatal(err)
			}
			doc.WriteString(`"` + randomKey(rng) + `"` + space() + ":" + space() + string(value))
		}
		doc.WriteString(space() + "}")
		sameMembers(t, []byte(doc.String()))
		documents++
	}
	t.Logf("%d documents", documents)
}

func TestTheElementsOfAnyValidArrayAreWhatEncodingJSONReadsOfIt(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range 2000 {
		values := make([]any, rng.IntN(8))
		for j := range values {
			values[j] = randomValue(rng, 4)
		}
		compact, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}
		spaced, err := json.MarshalIndent(values, " ", "\t")
		if err != nil {
			t.Fatalf("document %d: %v", i, err)
		}
		sameElements(t, compact)
		sameElements(t, spaced)
	}
}

func FuzzWalk(f *testing.F) {
	for _, seed := range []string{
		`{}`, `{"a":1}`, `{"a":true,"b":null}`, `{ "a" : [ 1 , { "b" : "}" } ] }`,
		`{"na\u006de":"x","a\"b":"\\"}`, "{\"\xff\":1}", `{"a":1,"a":2}`,
		`{"` + strings.Repeat(`\n`, jsonscan.MaxKey/2) + `":1}`, `{"` + strings.Repeat(`\n`, jsonscan.MaxKey/2+1) + `":1,"b":2}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if !json.Valid(data) || len(data) == 0 || data[0] != '{' {
			return
		}
		sameMembers(t, data)
	})
}

func FuzzElements(f *testing.F) {
	for _, seed := range []string{
		`[]`, `[1]`, `[1,"a",null,true]`, `[{},[],[[]]]`, `[ 1 , { "a" : "]" } ]`, `["\"","\\"]`, "[\"\xff\"]",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if !json.Valid(data) || len(data) == 0 || data[0] != '[' {
			return
		}
		sameElements(t, data)
	})
}

func TestTextDecodesWhatEncodingJSONDecodesUpToItsBound(t *testing.T) {
	const most = 8
	for _, c := range []struct {
		name  string
		value string
		want  string
		err   string
	}{
		{"null", `null`, "", ""},
		{"empty", `""`, "", ""},
		{"at the bound", `"12345678"`, "12345678", ""},
		{"one byte past it", `"123456789"`, "", "the field is longer than 8 bytes"},
		{"escapes count raw", `"\u00e9\u00e9"`, "", "the field is longer than 8 bytes"},
		{"escapes at the bound", `"\u00e9"`, "é", ""},
		{"invalid UTF-8 at the bound", "\"\xff\xff\xff\xff\xff\xff\xff\xff\"", strings.Repeat("\ufffd", 8), ""},
		{"a number", `1`, "", "the field is not a string"},
		{"an object", `{}`, "", "the field is not a string"},
		{"an array", `[]`, "", "the field is not a string"},
		{"a long number", strings.Repeat("1", 1<<20), "", "the field is not a string"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := jsonscan.Text("the field", []byte(c.value), most)
			if (err != nil) != (c.err != "") || err != nil && err.Error() != c.err || got != c.want {
				t.Errorf("Text returned %q, %v; want %q, %q", got, err, c.want, c.err)
			}
		})
	}
}

func TestATokenPastItsBoundIsAnErrorThatNamesNothingOfIt(t *testing.T) {
	value := `"` + strings.Repeat("SECRET", 1000) + `"`
	_, err := jsonscan.Text("metadata.name", []byte(value), 100)
	var tooLong *jsonscan.TooLong
	if !errors.As(err, &tooLong) || tooLong.Field != "metadata.name" || tooLong.Most != 100 {
		t.Fatalf("Text returned %v, want a TooLong of metadata.name and 100", err)
	}
	if strings.Contains(err.Error(), "SECRET") || len(err.Error()) > 100 {
		t.Errorf("the error %q carries the value", err)
	}
	_, err = jsonscan.Integer("code", []byte("1"+strings.Repeat("0", 1000)), 11)
	if !errors.As(err, &tooLong) || strings.Contains(err.Error(), "000") {
		t.Errorf("Integer returned %v", err)
	}
	_, err = jsonscan.Integer("code", []byte("99999999999"), 11)
	if err == nil || strings.Contains(err.Error(), "99999") {
		t.Errorf("Integer of a number out of range returned %v", err)
	}
}

func TestIntegerDecodesAnInt32OfAtMostItsBound(t *testing.T) {
	for _, c := range []struct {
		value string
		want  *int32
		fails bool
	}{
		{`null`, nil, false},
		{`0`, new(int32(0)), false},
		{`-2147483648`, new(int32(-2147483648)), false},
		{`2147483647`, new(int32(2147483647)), false},
		{`2147483648`, nil, true},
		{`1.5`, nil, true},
		{`1e2`, nil, true},
		{`"1"`, nil, true},
		{`true`, nil, true},
		{`{}`, nil, true},
	} {
		got, err := jsonscan.Integer("n", []byte(c.value), 64)
		if (err != nil) != c.fails || (got == nil) != (c.want == nil) || got != nil && *got != *c.want {
			t.Errorf("Integer of %s returned %v, %v", c.value, got, err)
		}
	}
	if _, err := jsonscan.Integer("n", []byte(strings.Repeat("0", 65)), 64); err == nil {
		t.Error("Integer of 65 bytes was read")
	}
}

func TestBooleanDecodesTheThreeLiterals(t *testing.T) {
	for _, c := range []struct {
		value string
		want  *bool
		fails bool
	}{
		{`null`, nil, false}, {`true`, new(true), false}, {`false`, new(false), false},
		{`"true"`, nil, true}, {`1`, nil, true}, {`{}`, nil, true}, {strings.Repeat("t", 1<<20), nil, true},
	} {
		got, err := jsonscan.Boolean("b", []byte(c.value))
		if (err != nil) != c.fails || (got == nil) != (c.want == nil) || got != nil && *got != *c.want {
			t.Errorf("Boolean of %.10s returned %v, %v", c.value, got, err)
		}
	}
}

func TestWithinCountsAStringWithoutItsQuotes(t *testing.T) {
	for _, c := range []struct {
		value string
		most  int
		want  bool
	}{
		{`""`, 0, true}, {`"a"`, 0, false}, {`"a"`, 1, true}, {`12`, 2, true}, {`123`, 2, false}, {`null`, 4, true}, {`null`, 3, false},
	} {
		if got := jsonscan.Within([]byte(c.value), c.most); got != c.want {
			t.Errorf("Within(%s, %d) is %t, want %t", c.value, c.most, got, c.want)
		}
	}
}
