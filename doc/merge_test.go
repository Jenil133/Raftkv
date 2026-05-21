package doc

import (
	"encoding/json"
	"testing"
)

// Test vectors from RFC 7386 appendix A.
func TestMergePatchRFC7386(t *testing.T) {
	cases := []struct{ orig, patch, want string }{
		{`{"a":"b"}`, `{"a":"c"}`, `{"a":"c"}`},
		{`{"a":"b"}`, `{"b":"c"}`, `{"a":"b","b":"c"}`},
		{`{"a":"b"}`, `{"a":null}`, `{}`},
		{`{"a":"b","b":"c"}`, `{"a":null}`, `{"b":"c"}`},
		{`{"a":["b"]}`, `{"a":"c"}`, `{"a":"c"}`},
		{`{"a":"c"}`, `{"a":["b"]}`, `{"a":["b"]}`},
		{`{"a":{"b":"c"}}`, `{"a":{"b":"d","c":null}}`, `{"a":{"b":"d"}}`},
		{`{"a":[{"b":"c"}]}`, `{"a":[1]}`, `{"a":[1]}`},
		{`{"e":null}`, `{"a":1}`, `{"e":null,"a":1}`},
		{`{}`, `{"a":{"bb":{"ccc":null}}}`, `{"a":{"bb":{}}}`},
	}
	for _, c := range cases {
		o, _ := decodeAny([]byte(c.orig))
		p, _ := decodeAny([]byte(c.patch))
		got, _ := json.Marshal(mergePatch(o, p))
		var g, w any
		json.Unmarshal(got, &g)
		json.Unmarshal([]byte(c.want), &w)
		gb, _ := json.Marshal(g)
		wb, _ := json.Marshal(w)
		if string(gb) != string(wb) {
			t.Errorf("merge(%s, %s) = %s, want %s", c.orig, c.patch, got, c.want)
		}
	}
}
