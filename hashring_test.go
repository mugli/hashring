package hashring

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type myNode string

func (m myNode) String() string {
	return string(m)
}

func stringSliceToNodeSlice(names []string) []Node {
	ret := make([]Node, 0, len(names))
	for _, name := range names {
		ret = append(ret, myNode(name))
	}

	return ret
}

type testPair struct {
	key  string
	node Node
}

type testNodes struct {
	key   string
	nodes []Node
}

func assert2Nodes(t *testing.T, prefix string, ring *HashRing, data []testNodes) {
	t.Run(prefix, func(t *testing.T) {
		allActual := make([]string, 0)
		allExpected := make([]string, 0)
		for _, pair := range data {
			nodes, ok := ring.GetNodes(pair.key, 2)
			if assert.True(t, ok) {
				allActual = append(allActual, fmt.Sprintf("%s - %v", pair.key, nodes))
				allExpected = append(allExpected, fmt.Sprintf("%s - %v", pair.key, pair.nodes))
			}
		}
		assert.Equal(t, allExpected, allActual)
	})
}

func assertNodes(t *testing.T, prefix string, ring *HashRing, allExpected []testPair) {
	t.Run(prefix, func(t *testing.T) {
		allActual := make([]testPair, 0)
		for _, pair := range allExpected {
			node, ok := ring.GetNode(pair.key)
			if assert.True(t, ok) {
				allActual = append(allActual, testPair{key: pair.key, node: node})
			}
		}
	})
}

func expectNodesABC(t *testing.T, prefix string, ring *HashRing) {

	assertNodes(t, prefix, ring, []testPair{
		{"test", myNode("a")},
		{"test", myNode("a")},
		{"test1", myNode("b")},
		{"test2", myNode("b")},
		{"test3", myNode("c")},
		{"test4", myNode("a")},
		{"test5", myNode("c")},
		{"aaaa", myNode("c")},
		{"bbbb", myNode("a")},
	})
}

func expectNodeRangesABC(t *testing.T, prefix string, ring *HashRing) {
	assert2Nodes(t, prefix, ring, []testNodes{
		{"test", stringSliceToNodeSlice([]string{"a", "c"})},
		{"test", stringSliceToNodeSlice([]string{"a", "c"})},
		{"test1", stringSliceToNodeSlice([]string{"b", "a"})},
		{"test2", stringSliceToNodeSlice([]string{"b", "a"})},
		{"test3", stringSliceToNodeSlice([]string{"c", "b"})},
		{"test4", stringSliceToNodeSlice([]string{"a", "c"})},
		{"test5", stringSliceToNodeSlice([]string{"c", "b"})},
		{"aaaa", stringSliceToNodeSlice([]string{"c", "b"})},
		{"bbbb", stringSliceToNodeSlice([]string{"a", "c"})},
	})
}

func expectNodesABCD(t *testing.T, prefix string, ring *HashRing) {
	assertNodes(t, prefix, ring, []testPair{
		{"test", myNode("d")},
		{"test", myNode("d")},
		{"test1", myNode("b")},
		{"test2", myNode("b")},
		{"test3", myNode("c")},
		{"test4", myNode("d")},
		{"test5", myNode("c")},
		{"aaaa", myNode("c")},
		{"bbbb", myNode("d")},
	})
}

func TestNew(t *testing.T) {
	nodes := stringSliceToNodeSlice([]string{"a", "b", "c"})
	ring := New(nodes)

	expectNodesABC(t, "TestNew_1_", ring)
	expectNodeRangesABC(t, "", ring)
}

func TestNewEmpty(t *testing.T) {
	nodes := stringSliceToNodeSlice([]string{})
	ring := New(nodes)

	node, ok := ring.GetNode("test")
	if ok || node != nil {
		t.Error("GetNode(test) expected (\"\", false) but got (", node, ",", ok, ")")
	}

	nodes, rok := ring.GetNodes("test", 2)
	if rok || !(len(nodes) == 0) {
		t.Error("GetNode(test) expected ( [], false ) but got (", nodes, ",", rok, ")")
	}
}

func TestForMoreNodes(t *testing.T) {
	nodes := stringSliceToNodeSlice([]string{"a", "b", "c"})
	ring := New(nodes)

	nodes, ok := ring.GetNodes("test", 5)
	if ok || nodes != nil {
		t.Error("GetNode(test) expected ( [], false ) but got (", nodes, ",", ok, ")")
	}
}

func TestForEqualNodes(t *testing.T) {
	nodes := stringSliceToNodeSlice([]string{"a", "b", "c"})
	ring := New(nodes)

	nodes, ok := ring.GetNodes("test", 3)
	if !ok && (len(nodes) == 3) {
		t.Error("GetNode(test) expected ( [a b c], true ) but got (", nodes, ",", ok, ")")
	}
}

func TestNewSingle(t *testing.T) {
	nodes := stringSliceToNodeSlice([]string{"a"})
	ring := New(nodes)

	assertNodes(t, "", ring, []testPair{
		{"test", myNode("a")},
		{"test", myNode("a")},
		{"test1", myNode("a")},
		{"test2", myNode("a")},
		{"test3", myNode("a")},

		{"test14", myNode("a")},

		{"test15", myNode("a")},
		{"test16", myNode("a")},
		{"test17", myNode("a")},
		{"test18", myNode("a")},
		{"test19", myNode("a")},
		{"test20", myNode("a")},
	})
}

func TestRemoveNode(t *testing.T) {
	nodes := stringSliceToNodeSlice([]string{"a", "b", "c"})
	ring := New(nodes)
	ring = ring.RemoveNode(myNode("b"))

	assertNodes(t, "", ring, []testPair{
		{"test", myNode("a")},
		{"test", myNode("a")},
		{"test1", myNode("a")},
		{"test2", myNode("a")},
		{"test3", myNode("c")},
		{"test4", myNode("a")},
		{"test5", myNode("c")},
		{"aaaa", myNode("c")},
		{"bbbb", myNode("a")},
	})

	assert2Nodes(t, "", ring, []testNodes{
		{"test", stringSliceToNodeSlice([]string{"a", "c"})},
	})
}

func TestAddNode(t *testing.T) {
	nodes := stringSliceToNodeSlice([]string{"a", "c"})
	ring := New(nodes)
	ring = ring.AddNode(myNode("b"))

	expectNodesABC(t, "TestAddNode_1_", ring)
}

func TestAddNode2(t *testing.T) {
	nodes := stringSliceToNodeSlice([]string{"a", "c"})
	ring := New(nodes)
	ring = ring.AddNode(myNode("b"))
	ring = ring.AddNode(myNode("b"))

	expectNodesABC(t, "TestAddNode2_", ring)
	expectNodeRangesABC(t, "", ring)
}

func TestAddNode3(t *testing.T) {
	nodes := stringSliceToNodeSlice([]string{"a", "b", "c"})
	ring := New(nodes)
	ring = ring.AddNode(myNode("d"))

	expectNodesABCD(t, "TestAddNode3_1_", ring)

	ring = ring.AddNode(myNode("e"))

	assertNodes(t, "TestAddNode3_2_", ring, []testPair{
		{"test", myNode("d")},
		{"test", myNode("d")},
		{"test1", myNode("b")},
		{"test2", myNode("e")},
		{"test3", myNode("c")},
		{"test4", myNode("d")},
		{"test5", myNode("c")},
		{"aaaa", myNode("c")},
		{"bbbb", myNode("d")},
	})

	assert2Nodes(t, "", ring, []testNodes{
		{"test", stringSliceToNodeSlice([]string{"d", "a"})},
	})

	ring = ring.AddNode(myNode("f"))

	assertNodes(t, "TestAddNode3_3_", ring, []testPair{
		{"test", myNode("d")},
		{"test", myNode("d")},
		{"test1", myNode("b")},
		{"test2", myNode("e")},
		{"test3", myNode("c")},
		{"test4", myNode("d")},
		{"test5", myNode("c")},
		{"aaaa", myNode("c")},
		{"bbbb", myNode("d")},
	})

	assert2Nodes(t, "", ring, []testNodes{
		{"test", stringSliceToNodeSlice([]string{"d", "a"})},
	})
}

func TestDuplicateNodes(t *testing.T) {
	nodes := stringSliceToNodeSlice([]string{"a", "a", "a", "a", "b"})
	ring := New(nodes)

	assertNodes(t, "TestDuplicateNodes_", ring, []testPair{
		{"test", myNode("a")},
		{"test", myNode("a")},
		{"test1", myNode("b")},
		{"test2", myNode("b")},
		{"test3", myNode("b")},
		{"test4", myNode("a")},
		{"test5", myNode("b")},
		{"aaaa", myNode("b")},
		{"bbbb", myNode("a")},
	})
}

func TestRemoveAddNode(t *testing.T) {
	nodes := stringSliceToNodeSlice([]string{"a", "b", "c"})
	ring := New(nodes)

	expectNodesABC(t, "1_", ring)
	expectNodeRangesABC(t, "2_", ring)

	ring = ring.RemoveNode(myNode("b"))

	assertNodes(t, "3_", ring, []testPair{
		{"test", myNode("a")},
		{"test", myNode("a")},
		{"test1", myNode("a")},
		{"test2", myNode("a")},
		{"test3", myNode("c")},
		{"test4", myNode("a")},
		{"test5", myNode("c")},
		{"aaaa", myNode("c")},
		{"bbbb", myNode("a")},
	})

	assert2Nodes(t, "4_", ring, []testNodes{
		{"test", stringSliceToNodeSlice([]string{"a", "c"})},
		{"test", stringSliceToNodeSlice([]string{"a", "c"})},
		{"test1", stringSliceToNodeSlice([]string{"a", "c"})},
		{"test2", stringSliceToNodeSlice([]string{"a", "c"})},
		{"test3", stringSliceToNodeSlice([]string{"c", "a"})},
		{"test4", stringSliceToNodeSlice([]string{"a", "c"})},
		{"test5", stringSliceToNodeSlice([]string{"c", "a"})},
		{"aaaa", stringSliceToNodeSlice([]string{"c", "a"})},
		{"bbbb", stringSliceToNodeSlice([]string{"a", "c"})},
	})

	ring = ring.AddNode(myNode("b"))

	expectNodesABC(t, "5_", ring)
	expectNodeRangesABC(t, "6_", ring)
}

func TestAddRemoveNode(t *testing.T) {
	nodes := stringSliceToNodeSlice([]string{"a", "b", "c"})
	ring := New(nodes)
	ring = ring.AddNode(myNode("d"))

	expectNodesABCD(t, "1_", ring)

	assert2Nodes(t, "2_", ring, []testNodes{
		{"test", stringSliceToNodeSlice([]string{"d", "a"})},
		{"test", stringSliceToNodeSlice([]string{"d", "a"})},
		{"test1", stringSliceToNodeSlice([]string{"b", "d"})},
		{"test2", stringSliceToNodeSlice([]string{"b", "d"})},
		{"test3", stringSliceToNodeSlice([]string{"c", "b"})},
		{"test4", stringSliceToNodeSlice([]string{"d", "a"})},
		{"test5", stringSliceToNodeSlice([]string{"c", "b"})},
		{"aaaa", stringSliceToNodeSlice([]string{"c", "b"})},
		{"bbbb", stringSliceToNodeSlice([]string{"d", "a"})},
	})

	ring = ring.AddNode(myNode("e"))

	assertNodes(t, "3_", ring, []testPair{
		{"test", myNode("a")},
		{"test", myNode("a")},
		{"test1", myNode("b")},
		{"test2", myNode("b")},
		{"test3", myNode("c")},
		{"test4", myNode("c")},
		{"test5", myNode("a")},
		{"aaaa", myNode("b")},
		{"bbbb", myNode("e")},
	})

	assert2Nodes(t, "4_", ring, []testNodes{
		{"test", stringSliceToNodeSlice([]string{"d", "a"})},
		{"test", stringSliceToNodeSlice([]string{"d", "a"})},
		{"test1", stringSliceToNodeSlice([]string{"b", "d"})},
		{"test2", stringSliceToNodeSlice([]string{"e", "b"})},
		{"test3", stringSliceToNodeSlice([]string{"c", "e"})},
		{"test4", stringSliceToNodeSlice([]string{"d", "a"})},
		{"test5", stringSliceToNodeSlice([]string{"c", "e"})},
		{"aaaa", stringSliceToNodeSlice([]string{"c", "e"})},
		{"bbbb", stringSliceToNodeSlice([]string{"d", "a"})},
	})

	ring = ring.AddNode(myNode("f"))

	assertNodes(t, "5_", ring, []testPair{
		{"test", myNode("a")},
		{"test", myNode("a")},
		{"test1", myNode("b")},
		{"test2", myNode("f")},
		{"test3", myNode("f")},
		{"test4", myNode("c")},
		{"test5", myNode("f")},
		{"aaaa", myNode("b")},
		{"bbbb", myNode("e")},
	})

	assert2Nodes(t, "6_", ring, []testNodes{
		{"test", stringSliceToNodeSlice([]string{"d", "a"})},
		{"test", stringSliceToNodeSlice([]string{"d", "a"})},
		{"test1", stringSliceToNodeSlice([]string{"b", "d"})},
		{"test2", stringSliceToNodeSlice([]string{"e", "f"})},
		{"test3", stringSliceToNodeSlice([]string{"c", "e"})},
		{"test4", stringSliceToNodeSlice([]string{"d", "a"})},
		{"test5", stringSliceToNodeSlice([]string{"c", "e"})},
		{"aaaa", stringSliceToNodeSlice([]string{"c", "e"})},
		{"bbbb", stringSliceToNodeSlice([]string{"d", "a"})},
	})

	ring = ring.RemoveNode(myNode("e"))

	assertNodes(t, "7_", ring, []testPair{
		{"test", myNode("a")},
		{"test", myNode("a")},
		{"test1", myNode("b")},
		{"test2", myNode("f")},
		{"test3", myNode("f")},
		{"test4", myNode("c")},
		{"test5", myNode("f")},
		{"aaaa", myNode("b")},
		{"bbbb", myNode("f")},
	})

	assert2Nodes(t, "8_", ring, []testNodes{
		{"test", stringSliceToNodeSlice([]string{"d", "a"})},
		{"test", stringSliceToNodeSlice([]string{"d", "a"})},
		{"test1", stringSliceToNodeSlice([]string{"b", "d"})},
		{"test2", stringSliceToNodeSlice([]string{"f", "b"})},
		{"test3", stringSliceToNodeSlice([]string{"c", "f"})},
		{"test4", stringSliceToNodeSlice([]string{"d", "a"})},
		{"test5", stringSliceToNodeSlice([]string{"c", "f"})},
		{"aaaa", stringSliceToNodeSlice([]string{"c", "f"})},
		{"bbbb", stringSliceToNodeSlice([]string{"d", "a"})},
	})

	ring = ring.RemoveNode(myNode("f"))

	expectNodesABCD(t, "TestAddRemoveNode_5_", ring)

	assert2Nodes(t, "", ring, []testNodes{
		{"test", stringSliceToNodeSlice([]string{"d", "a"})},
		{"test", stringSliceToNodeSlice([]string{"d", "a"})},
		{"test1", stringSliceToNodeSlice([]string{"b", "d"})},
		{"test2", stringSliceToNodeSlice([]string{"b", "d"})},
		{"test3", stringSliceToNodeSlice([]string{"c", "b"})},
		{"test4", stringSliceToNodeSlice([]string{"d", "a"})},
		{"test5", stringSliceToNodeSlice([]string{"c", "b"})},
		{"aaaa", stringSliceToNodeSlice([]string{"c", "b"})},
		{"bbbb", stringSliceToNodeSlice([]string{"d", "a"})},
	})

	ring = ring.RemoveNode(myNode("d"))

	expectNodesABC(t, "TestAddRemoveNode_6_", ring)
	expectNodeRangesABC(t, "", ring)
}
