package hashring

import (
	"fmt"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func generateNodes(n int) []Node {
	result := make([]Node, 0, n)
	for i := 0; i < n; i++ {
		result = append(result, myNode(fmt.Sprintf("%03d", i)))
	}
	return result
}

func TestListOf1000Nodes(t *testing.T) {
	testData := map[string]struct {
		ring *HashRing
	}{
		"nodes": {ring: New(generateNodes(1000))},
	}

	for testName, data := range testData {
		ring := data.ring
		t.Run(testName, func(t *testing.T) {
			nodes, ok := ring.GetNodesForReplicas("key", ring.Size())
			assert.True(t, ok)
			if !assert.Equal(t, ring.Size(), len(nodes)) {
				// print debug info on failure
				sort.SliceStable(nodes, func(i, j int) bool {
					return nodes[i].String() < nodes[j].String()
				})
				fmt.Printf("%v\n", nodes)
				return
			}

			// assert that each node shows up exatly once
			sort.SliceStable(nodes, func(i, j int) bool {
				return nodes[i].String() < nodes[j].String()
			})
			for i, node := range nodes {
				actual, err := strconv.ParseInt(node.String(), 10, 64)
				if !assert.NoError(t, err) {
					return
				}
				if !assert.Equal(t, int64(i), actual) {
					return
				}
			}
		})
	}
}
