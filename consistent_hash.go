package consistent

import (
	"errors"
	"hash/crc32"
	"sort"
	"sync"
)

var (
	ErrEmptyCircle = errors.New("empty hash circle")
)

type uints []uint32

func (u uints) Len() int {
	return len(u)
}

func (u uints) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

func (u uints) Less(i, j int) bool {
	return u[i] < u[j]
}

type ConsistentHash struct {
	circle map[uint32]string // hash环

	sortedHashItems uints // hash item的有序排列，用于查找映射的item

	mutex sync.RWMutex
}

func NewConsistentHash() *ConsistentHash {
	c := new(ConsistentHash)
	c.circle = make(map[uint32]string)
	return c
}

// 增加节点实例
func (c *ConsistentHash) Add(item string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	key := c.hashKey(item)
	c.circle[key] = item
	c.reloadSortedHashItems()
}

// 移除节点实例
func (c *ConsistentHash) Remove(item string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	key := c.hashKey(item)
	delete(c.circle, key)
	c.reloadSortedHashItems()
}

// 触发排序hash项列表的排序更新，数据从circle里获取，保持二者一致
func (c *ConsistentHash) reloadSortedHashItems() {
	sh := uints{}
	for k, _ := range c.circle {
		sh = append(sh, k)
	}
	sort.Sort(sh)
	c.sortedHashItems = sh
}

func (c *ConsistentHash) hashKey(item string) uint32 {
	return crc32.ChecksumIEEE([]byte(item))
}

func (c *ConsistentHash) Get(name string) (string, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if len(c.circle) == 0 {
		return "", ErrEmptyCircle
	}
	key := c.hashKey(name)     // 获取hash，但是很难直接命中，需要按照某个顺序挂靠
	searchKey := c.search(key) // 实际挂靠的key
	item := c.circle[searchKey]
	return item, nil
}

// 根据hashkey定位下一个位置的item的key
func (c *ConsistentHash) search(key uint32) uint32 {
	fn := func(n int) bool {
		return c.sortedHashItems[n] > key
	}
	n := sort.Search(len(c.sortedHashItems), fn)
	n %= len(c.sortedHashItems)
	return c.sortedHashItems[n]
}
