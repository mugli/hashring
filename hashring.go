package hashring

import (
	"crypto/md5"
	"fmt"
	"sort"
	"sync"
)

// Node interface represents a member in consistent hash ring.
type Node interface {
	String() string
}

// HashRing is a consistent hash ring
type HashRing struct {
	nodeHashMap map[HashKey]Node // nodeHashMap is used to get a Node from its hashKey and return it in the GetNode like functions.
	sortedKeys  []HashKey        // sortedKeys stores all hashed and sorted values of nodes, and ultimately used as the hashring
	nodes       []Node           // nodes are members in consistent hash ring. this slice is kept sorted to perform binary search. nodes list is used to prevent duplicates for adding to the ring.
	hashFunc    HashFunc         // hashFunc returns a comparable HashKey
	mu          sync.RWMutex
}

func New(nodes []Node) *HashRing {
	return NewWithHash(nodes, defaultHashFunc)
}

func NewWithHash(nodes []Node, hashFunc HashFunc) *HashRing {
	if nodes == nil {
		panic("nodes cannot be nil")
	}

	hashRing := &HashRing{
		nodeHashMap: make(map[HashKey]Node),
		sortedKeys:  make([]HashKey, 0),
		nodes:       nodes,
		hashFunc:    hashFunc,
	}
	hashRing.generateCircle()
	return hashRing
}

// ensureStateReset cleans computed sortedKeys and nodeHashMap before generateCircle execution
func (h *HashRing) ensureStateReset() {
	if len(h.sortedKeys) > 0 || len(h.nodeHashMap) > 0 {
		panic("state is not reset")
	}
}

// generateCircle regenerates all hashKeys for all the nodes and then keep the keys sorted.
// generateCircle requires Lock(), make sure the caller is doing it
func (h *HashRing) generateCircle() {
	h.ensureStateReset()

	// generateCircle is called when nodes are added/removed.
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

// AddNode adds a node and generates a new hashring
func (h *HashRing) AddNode(node Node) *HashRing {
	h.mu.Lock()
	defer h.mu.Unlock()

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

// AddNode removes a node and generates a new hashring
func (h *HashRing) RemoveNode(node Node) *HashRing {
	h.mu.Lock()
	defer h.mu.Unlock()

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

func (h *HashRing) GetNode(stringKey string) (node Node, ok bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	pos, ok := h.getNodePos(stringKey)
	if !ok {
		return nil, false
	}
	return h.nodeHashMap[h.sortedKeys[pos]], true
}

// getNodePos requires RLock(), make sure the caller is doing it
func (h *HashRing) getNodePos(stringKey string) (pos int, ok bool) {
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

// GetNodesForReplicas iterates over the hash ring and returns a list of nodes to fulfill replication requirements.
// You can use this list of servers to store your key.
func (h *HashRing) GetNodesForReplicas(stringKey string, numberOfReplicas int) (nodes []Node, ok bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	pos, ok := h.getNodePos(stringKey)
	if !ok {
		return nil, false
	}

	if numberOfReplicas > len(h.nodes) {
		return nil, false
	}

	returnedValues := make(map[Node]bool, numberOfReplicas)
	resultSlice := make([]Node, 0, numberOfReplicas)

	for i := pos; i < pos+len(h.sortedKeys); i++ {
		key := h.sortedKeys[i%len(h.sortedKeys)]
		val := h.nodeHashMap[key]
		if !returnedValues[val] {
			returnedValues[val] = true
			resultSlice = append(resultSlice, val)
		}
		if len(returnedValues) == numberOfReplicas {
			break
		}
	}

	return resultSlice, len(resultSlice) == numberOfReplicas
}

func (h *HashRing) GenKey(key string) HashKey {
	return h.hashFunc([]byte(key))
}

func (h *HashRing) Size() int {
	return len(h.nodes)
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
