package hashinghigh

import (
	"crypto/sha1"
	"sync"

	//	"hash"
	"math"
	"sort"
	"strconv"
)

const (
	//DefaultVirualSpots default virual spots
	DefaultVirualSpots = 400
)

type node struct {
	NodeKey   string
	SpotValue uint32
}

// NodesArray 节点集合
type NodesArray []node

// Len 统计节点总个数
func (p NodesArray) Len() int           { return len(p) }
func (p NodesArray) Less(i, j int) bool { return p[i].SpotValue < p[j].SpotValue }
func (p NodesArray) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p NodesArray) Sort()              { sort.Sort(p) }

//HashRing store nodes and weigths
type HashRing struct {
	virualSpots int            // 单个节点最多允许虚拟节点个数
	Nodes       NodesArray     // 真实节点和其在hash环上所在映射的所有虚拟节点位置
	weights     map[string]int // 真实节点的权重，权重决定其虚拟节点的个数
	mu          sync.RWMutex   // 读写锁
}

//NewHashRing create a hash ring with virual spots
func NewHashRing(spots int) *HashRing {
	if spots == 0 {
		spots = DefaultVirualSpots
	}

	h := &HashRing{
		virualSpots: spots,
		weights:     make(map[string]int),
	}
	return h
}

//AddNodes add nodes to hash ring
func (h *HashRing) AddNodes(nodeWeight map[string]int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for nodeKey, w := range nodeWeight {
		h.weights[nodeKey] = w
	}
	h.generate()
}

//AddNode add node to hash ring
func (h *HashRing) AddNode(nodeKey string, weight int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.weights[nodeKey] = weight
	h.generate()
}

//RemoveNode remove node
func (h *HashRing) RemoveNode(nodeKey string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.weights, nodeKey)
	h.generate()
}

//UpdateNode update node with weight
func (h *HashRing) UpdateNode(nodeKey string, weight int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.weights[nodeKey] = weight
	h.generate()
}

func (h *HashRing) generate() {
	// 统计所有权重总和
	var totalW int
	for _, w := range h.weights {
		totalW += w
	}

	// 统计虚拟节点总和
	totalVirtualSpots := h.virualSpots * len(h.weights)
	h.Nodes = NodesArray{}

	for nodeKey, w := range h.weights {
		// 根据权重来决定该节点虚拟节点的个数
		spots := int(math.Floor(float64(w) / float64(totalW) * float64(totalVirtualSpots)))
		for i := 1; i <= spots; i++ {
			hash := sha1.New()
			hash.Write([]byte(nodeKey + ":" + strconv.Itoa(i)))
			hashBytes := hash.Sum(nil)
			n := node{
				NodeKey:   nodeKey,
				SpotValue: genValue(hashBytes[6:10]),
			}
			h.Nodes = append(h.Nodes, n)
			hash.Reset()
		}
	}
	h.Nodes.Sort()
}

func genValue(bs []byte) uint32 {
	if len(bs) < 4 {
		return 0
	}

	// a << 24 相当于 a * 2^24
	// a << 24 | a << 16 相当于 a * 2^24 + a * 2^16
	v := (uint32(bs[3]) << 24) | (uint32(bs[2]) << 16) | (uint32(bs[1]) << 8) | (uint32(bs[0]))
	return v
}

//GetNode get node with key
func (h *HashRing) GetNode(s string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.Nodes) == 0 {
		return ""
	}

	hash := sha1.New()
	hash.Write([]byte(s))
	hashBytes := hash.Sum(nil)
	v := genValue(hashBytes[6:10])
	i := sort.Search(len(h.Nodes), func(i int) bool { return h.Nodes[i].SpotValue >= v })

	if i == len(h.Nodes) {
		i = 0
	}
	return h.Nodes[i].NodeKey
}
