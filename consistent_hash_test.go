package consistent

import "testing"

func TestExample(t *testing.T) {
	c := NewConsistentHash()
	c.Add("test")
	c.Add("test1")
	c.Get("xxx")
}

func TestReloadSortedHashItems(t *testing.T) {
	c := NewConsistentHash()
	c.circle[1000] = "1000"
	c.circle[10000] = "10000"
	c.reloadSortedHashItems()
	if len(c.sortedHashItems) != 2 {
		t.Errorf("wrong length with %d", len(c.sortedHashItems))
		return
	}
	if c.sortedHashItems[0] != 1000 || c.sortedHashItems[1] != 10000 {
		t.Errorf("wrong sortedHashItems with %v", c.sortedHashItems)
	}
	c.circle[5000] = "5000"
	c.reloadSortedHashItems()
	if len(c.sortedHashItems) != 3 {
		t.Errorf("wrong length with %d", len(c.sortedHashItems))
		return
	}
	if c.sortedHashItems[0] != 1000 || c.sortedHashItems[1] != 5000 || c.sortedHashItems[2] != 10000 {
		t.Errorf("wrong sortedHashItems with %v", c.sortedHashItems)
	}
}

func TestSearch(t *testing.T) {
	c := NewConsistentHash()
	c.sortedHashItems = uints{5000, 10000, 1000000}
	if idx := c.search(6000); idx != 10000 {
		t.Errorf("wrong search idx=%d", idx)
	}
	if idx := c.search(5000000); idx != 5000 {
		t.Errorf("wrong search idx=%d", idx)
	}
}
