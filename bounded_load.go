package consistent

import (
	"math"
	"sync/atomic"
)

// 获取在平均负载以下的节点信息
func (c *ConsistentHash) GetLeast(name string) (item string, err error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if len(c.nodeMap) == 0 {
		err = ErrEmptyCircle
		return
	}

	key := c.hashKey(name)
	idx := c.search(key)

	for {
		name := c.circle[c.sortedHashItems[idx]]
		node := c.nodeMap[name]
		if c.loadOk(node) {
			return node.Name, nil
		}
		idx++
		idx %= uint32(len(c.sortedHashItems))
	}
	return
}

func (c *ConsistentHash) loadOk(node *Node) bool {
	if c.totalLoad < 0 {
		c.totalLoad = 0
	}

	var avgLoad float64
	avgLoad = float64(c.totalLoad+1) / float64(len(c.nodeMap))
	if avgLoad < 1 {
		avgLoad = 1
	}
	avgLoad = math.Ceil(avgLoad * 1.25)
	if float64(node.Load+1) <= avgLoad {
		return true
	}
	return false
}

// 选择使用对应node之后，更新其负载记录
func (c *ConsistentHash) IncrLoad(item string) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	node, ok := c.nodeMap[item]
	if !ok {
		err = ErrItemNotFound
		return
	}
	atomic.AddInt64(&node.Load, 1)
	atomic.AddInt64(&c.totalLoad, 1)

	return
}

// 取消从某node使用后，更新其负载记录
func (c *ConsistentHash) DecrLoad(item string) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	node, ok := c.nodeMap[item]
	if !ok {
		err = ErrItemNotFound
		return
	}
	atomic.AddInt64(&node.Load, -1)
	atomic.AddInt64(&c.totalLoad, -1)
	return
}

func (c *ConsistentHash) UpdateLoad(item string, load int64) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	_, ok := c.nodeMap[item]
	if !ok {
		err = ErrItemNotFound
		return
	}

	c.totalLoad -= c.nodeMap[item].Load
	c.nodeMap[item].Load = load
	c.totalLoad += c.nodeMap[item].Load

	return
}
