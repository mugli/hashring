package hashring

import (
	"crypto/md5"
	"fmt"
	"sort"
)

var defaultHashFunc = func() HashFunc {
	hashFunc, err := NewHash(md5.New).Use(NewInt64PairHashKey)
	if err != nil {
		panic(fmt.Sprintf("failed to create defaultHashFunc: %s", err.Error()))
	}
	return hashFunc
}()

// Node interface represents a member in consistent hash ring.
type Node interface {
	String() string
}

type HashKey interface {
	Less(other HashKey) bool
}
type HashKeyOrder []HashKey

func (h HashKeyOrder) Len() int      { return len(h) }
func (h HashKeyOrder) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h HashKeyOrder) Less(i, j int) bool {
	return h[i].Less(h[j])
}

type HashFunc func([]byte) HashKey

type HashRing struct {
	ring       map[HashKey]Node
	sortedKeys []HashKey
	nodes      []Node
	hashFunc   HashFunc
}

func New(nodes []Node) *HashRing {
	return NewWithHash(nodes, defaultHashFunc)
}

func NewWithHash(
	nodes []Node,
	hashKey HashFunc,
) *HashRing {
	hashRing := &HashRing{
		ring:       make(map[HashKey]Node),
		sortedKeys: make([]HashKey, 0),
		nodes:      nodes,
		hashFunc:   hashKey,
	}
	hashRing.generateCircle()
	return hashRing
}

func (h *HashRing) Size() int {
	return len(h.nodes)
}

func (h *HashRing) generateCircle() {
	for _, node := range h.nodes {
		nodeKey := node.String() + "-0"
		key := h.hashFunc([]byte(nodeKey))
		h.ring[key] = node
		h.sortedKeys = append(h.sortedKeys, key)
	}

	sort.Sort(HashKeyOrder(h.sortedKeys))
}

func (h *HashRing) GetNode(stringKey string) (node Node, ok bool) {
	pos, ok := h.GetNodePos(stringKey)
	if !ok {
		return nil, false
	}
	return h.ring[h.sortedKeys[pos]], true
}

func (h *HashRing) GetNodePos(stringKey string) (pos int, ok bool) {
	if len(h.ring) == 0 {
		return 0, false
	}

	key := h.GenKey(stringKey)

	nodes := h.sortedKeys
	pos = sort.Search(len(nodes), func(i int) bool { return key.Less(nodes[i]) })

	if pos == len(nodes) {
		// Wrap the search, should return First node
		return 0, true
	} else {
		return pos, true
	}
}

func (h *HashRing) GenKey(key string) HashKey {
	return h.hashFunc([]byte(key))
}

// GetNodes iterates over the hash ring and returns the nodes in the order
// which is determined by the key. GetNodes is thread safe if the hash
// which was used to configure the hash ring is thread safe.
func (h *HashRing) GetNodes(stringKey string, size int) (nodes []Node, ok bool) {
	pos, ok := h.GetNodePos(stringKey)
	if !ok {
		return nil, false
	}

	if size > len(h.nodes) {
		return nil, false
	}

	returnedValues := make(map[Node]bool, size)
	//mergedSortedKeys := append(h.sortedKeys[pos:], h.sortedKeys[:pos]...)
	resultSlice := make([]Node, 0, size)

	for i := pos; i < pos+len(h.sortedKeys); i++ {
		key := h.sortedKeys[i%len(h.sortedKeys)]
		val := h.ring[key]
		if !returnedValues[val] {
			returnedValues[val] = true
			resultSlice = append(resultSlice, val)
		}
		if len(returnedValues) == size {
			break
		}
	}

	return resultSlice, len(resultSlice) == size
}

func (h *HashRing) AddNode(node Node) *HashRing {
	return h.addNode(node)
}

func (h *HashRing) addNode(node Node) *HashRing {
	// TODO: Check nodes array instead to see if the node is already present
	// if _, ok := h.weights[node]; ok {
	// 	return h
	// }

	nodes := make([]Node, len(h.nodes), len(h.nodes)+1)
	copy(nodes, h.nodes)
	nodes = append(nodes, node)

	hashRing := &HashRing{
		ring:       make(map[HashKey]Node),
		sortedKeys: make([]HashKey, 0),
		nodes:      nodes,
		hashFunc:   h.hashFunc,
	}
	hashRing.generateCircle()
	return hashRing
}

func (h *HashRing) RemoveNode(node Node) *HashRing {
	/* if node isn't exist in hashring, don't refresh hashring */
	// TODO: Check nodes array instead to see if the node is already present
	// if _, ok := h.weights[node]; !ok {
	// 	return h
	// }

	nodes := make([]Node, 0)
	for _, eNode := range h.nodes {
		if eNode != node {
			nodes = append(nodes, eNode)
		}
	}

	hashRing := &HashRing{
		ring:       make(map[HashKey]Node),
		sortedKeys: make([]HashKey, 0),
		nodes:      nodes,
		hashFunc:   h.hashFunc,
	}
	hashRing.generateCircle()
	return hashRing
}
