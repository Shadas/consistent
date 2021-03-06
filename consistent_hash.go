package consistent

import (
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
	"sync"
)

const (
	DefaultReplicaNum = 20
)

var (
	ErrEmptyCircle  = errors.New("empty hash circle")
	ErrFullCircle   = errors.New("circle has no place")
	ErrItemNotFound = errors.New("item not found")
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

type Node struct {
	Name string
	Load int64
}

type ConsistentHash struct {
	replicaNum int               // 虚拟节点复制数，用于提升平衡性
	circle     map[uint32]string // hash环
	nodeMap    map[string]*Node  // 节点映射，记录负载信息，以及其他后续扩展信息

	sortedHashItems uints // hash item的有序排列，用于查找映射的item

	hashFunc func(key string) uint32 // hash func，可自行指定

	totalLoad int64 // 总负载，用于计算有界负载的场景

	mutex sync.RWMutex
}

func NewConsistentHash(fns ...OptFunc) *ConsistentHash {
	c := new(ConsistentHash)
	c.hashFunc = func(key string) uint32 {
		return crc32.ChecksumIEEE([]byte(key))
	}
	c.replicaNum = DefaultReplicaNum
	c.circle = make(map[uint32]string)
	c.nodeMap = make(map[string]*Node)
	// run optional funcs
	for _, fn := range fns {
		fn(c)
	}
	return c
}

// 增加节点实例
func (c *ConsistentHash) Add(item string) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, ok := c.nodeMap[item]; ok { // 添加已有节点
		return
	}

	c.nodeMap[item] = &Node{Name: item}

	for i := 0; i < c.replicaNum; i++ {
		key := c.hashKey(c.replicaItem(i, item))
		oldKey := key
		for {
			if _, ok := c.circle[key]; ok {
				// conflict
				key++
				key %= math.MaxUint32
				if key == oldKey { // 无处可放
					return ErrFullCircle
				}
			} else {
				c.circle[key] = item
				break
			}
		}
	}

	c.reloadSortedHashItems()
	return
}

// 移除节点实例
// todo：删除改为逻辑删除
func (c *ConsistentHash) Remove(item string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i := 0; i < c.replicaNum; i++ {
		key := c.hashKey(c.replicaItem(i, item))
		delete(c.circle, key)
	}

	delete(c.nodeMap, item)

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

// 生成虚拟节点的hashkey输入值
func (c *ConsistentHash) replicaItem(i int, item string) string {
	return fmt.Sprintf("%d_%s", i, item)
}

// 生成hashkey
func (c *ConsistentHash) hashKey(item string) uint32 {
	return c.hashFunc(item)
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
