package consistent

type OptFunc func(c *ConsistentHash)

// 设置hash函数
func WithHashFunc(fn func(key string) uint32) OptFunc {
	return func(c *ConsistentHash) {
		if fn != nil {
			c.hashFunc = fn
		}
	}
}

// 设置用于虚拟节点的副本数
func WithReplicaNum(num int) OptFunc {
	return func(c *ConsistentHash) {
		if num >= 0 {
			c.replicaNum = num
		}
	}
}
