package hashring

import (
	"crypto/md5"
	"fmt"
	"sort"
)

// Node interface represents a member in consistent hash ring.
type Node interface {
	String() string
}

// HashRing is a consistent hash ring
type HashRing struct {
	// nodeHashMap is used to get a Node from its hashkey
	nodeHashMap map[HashKey]Node
	// sortedKeys stores all hashed and sorted values of nodes, and ultimately used as the hashring
	sortedKeys []HashKey
	// nodes are members in consistent hash ring. this slice is kept sorted to perform binary search.
	nodes []Node
	// hashFunc returns a comparable HashKey
	hashFunc HashFunc
}

func New(nodes []Node) *HashRing {
	return NewWithHash(nodes, defaultHashFunc)
}

func NewWithHash(nodes []Node, hashKey HashFunc) *HashRing {
	hashRing := &HashRing{
		nodeHashMap: make(map[HashKey]Node),
		sortedKeys:  make([]HashKey, 0),
		nodes:       nodes,
		hashFunc:    hashKey,
	}
	hashRing.generateCircle()
	return hashRing
}

func (h *HashRing) Size() int {
	return len(h.nodes)
}

func (h *HashRing) generateCircle() {
	// generateCircle is called when nodes are added/removed/reset.
	// keep the list sorted
	sort.SliceStable(h.nodes, func(i, j int) bool {
		return h.nodes[i].String() < h.nodes[j].String()
	})

	for _, node := range h.nodes {
		nodeKey := node.String() + "-0" // "-0" is leftover from the weight feature that got removed. it's still here to make test assertions happy.
		hashKey := h.hashFunc([]byte(nodeKey))
		h.nodeHashMap[hashKey] = node
		h.sortedKeys = append(h.sortedKeys, hashKey)
	}

	sort.SliceStable(h.sortedKeys, func(i, j int) bool {
		return h.sortedKeys[i].Less(h.sortedKeys[j])
	})
}

func (h *HashRing) GetNode(stringKey string) (node Node, ok bool) {
	pos, ok := h.GetNodePos(stringKey)
	if !ok {
		return nil, false
	}
	return h.nodeHashMap[h.sortedKeys[pos]], true
}

func (h *HashRing) GetNodePos(stringKey string) (pos int, ok bool) {
	if len(h.nodeHashMap) == 0 {
		return 0, false
	}

	key := h.GenKey(stringKey)

	sortedKeys := h.sortedKeys
	pos = sort.Search(len(sortedKeys), func(i int) bool { return key.Less(sortedKeys[i]) })

	if pos == len(sortedKeys) {
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
	resultSlice := make([]Node, 0, size)

	for i := pos; i < pos+len(h.sortedKeys); i++ {
		key := h.sortedKeys[i%len(h.sortedKeys)]
		val := h.nodeHashMap[key]
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
	pos := sort.Search(len(h.nodes), func(i int) bool { return h.nodes[i].String() >= node.String() })
	if pos < len(h.nodes) && h.nodes[pos].String() == node.String() {
		// node is already present, just return
		return h
	}

	nodes := make([]Node, len(h.nodes), len(h.nodes)+1)
	copy(nodes, h.nodes)
	nodes = append(nodes, node)

	hashRing := &HashRing{
		nodeHashMap: make(map[HashKey]Node),
		sortedKeys:  make([]HashKey, 0),
		nodes:       nodes,
		hashFunc:    h.hashFunc,
	}
	hashRing.generateCircle()
	return hashRing
}

func (h *HashRing) RemoveNode(node Node) *HashRing {
	/* if node isn't exist in hashring, don't refresh hashring */
	pos := sort.Search(len(h.nodes), func(i int) bool { return h.nodes[i].String() >= node.String() })
	if !(pos < len(h.nodes) && h.nodes[pos].String() == node.String()) {
		// node is not present, just return
		return h
	}

	nodes := make([]Node, 0)
	for _, eNode := range h.nodes {
		if eNode != node {
			nodes = append(nodes, eNode)
		}
	}

	hashRing := &HashRing{
		nodeHashMap: make(map[HashKey]Node),
		sortedKeys:  make([]HashKey, 0),
		nodes:       nodes,
		hashFunc:    h.hashFunc,
	}
	hashRing.generateCircle()
	return hashRing
}

type HashKey interface {
	Less(other HashKey) bool
}

type HashFunc func([]byte) HashKey

var defaultHashFunc = func() HashFunc {
	hashFunc, err := NewHash(md5.New).Use(NewInt64PairHashKey)
	if err != nil {
		panic(fmt.Sprintf("failed to create defaultHashFunc: %s", err.Error()))
	}
	return hashFunc
}()
