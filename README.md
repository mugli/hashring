# hashring

This is a fork from https://github.com/serialx/hashring for a toy project I'm experimenting with.

**Changes:**

- Removed weight feature for the sake of simplicity
- Used `hashring.Node` interface instead of `string` endpoints so that custom node types can be used
- Used stable sorting now to avoid a potential bug (see https://github.com/serialx/hashring/issues/24)
- It's a go module now

The original readme (with updated examples for custom Node type) goes below:

---

Implements consistent hashing that can be used when
the number of server nodes can increase or decrease (like in memcached).
The hashing ring is built using the same algorithm as libketama.

This is a port of Python hash_ring library <https://pypi.python.org/pypi/hash_ring/>
in Go with the extra methods to add and remove nodes.

# Using

Importing ::

```go
import "github.com/mugli/hashring"
```

Basic example usage ::

```go
// In your code, you probably have a custom data type
// for your cluster nodes. Just add a String function to implement
// hashring.Node interface.
type myNode string

func (m myNode) String() string {
	return string(m)
}

memcacheServers := []Node{
                            myNode("192.168.0.246:11212"),
                            myNode("192.168.0.247:11212"),
                            myNode("192.168.0.249:11212")
                          }

ring := hashring.New(memcacheServers)
server, _ := ring.GetNode("my_key")
```

To fulfill replication requirements, you can also get a list of servers that should store your key.

```go
serversInRing := []Node{
                          myNode("192.168.0.246:11212"),
                          myNode("192.168.0.247:11212"),
                          myNode("192.168.0.249:11212")
                        }

replicaCount := 3
ring := hashring.New(serversInRing)
server, _ := ring.GetNodes("my_key", replicaCount)
```

Adding and removing nodes example ::

```go

memcacheServers := []Node{
                          myNode("192.168.0.246:11212"),
                          myNode("192.168.0.247:11212"),
                          myNode("192.168.0.249:11212")
                        }

ring := hashring.New(memcacheServers)
ring = ring.RemoveNode(myNode("192.168.0.246:11212"))
ring = ring.AddNode(myNode("192.168.0.250:11212"))
server, _ := ring.GetNode("my_key")
```
