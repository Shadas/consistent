package consistent

import "testing"

func TestExample(t *testing.T) {
	c := New()
	c.Add("test")
	c.Add("test1")
	c.Get("xxx")
}
