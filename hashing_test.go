package main

import (
	"flag"
	"strconv"
	"testing"

	"github.com/consistent_hashing/hashinghigh"
	"github.com/consistent_hashing/hashingsimple"
)

/***************************** hahingsimple 测试 *******************************/

// TestConsistentHashSimple 测试
func TestConsistentHashSimple(t *testing.T) {
	// 创建一个一致性哈希实例，并自定义hash函数
	chash := hashingsimple.New(3, func(data []byte) uint32 {
		i, _ := strconv.Atoi(string(data))
		return uint32(i)
	})

	// 添加真实节点，为了方便说明，这里的节点名称只用数字进行表示
	chash.Add("4", "6", "2")

	testCases := map[string]string{
		"15": "6",
		"11": "2",
		"23": "4",
		"27": "2",
	}
	for k, v := range testCases {
		if chash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

	// 新增一个节点"8"，对应增加3个虚拟节点，分别为8,18,28
	chash.Add("8")

	// 此时如果查询的key为27，将会对应到虚拟节点28，也就是映射到真实节点8
	testCases["27"] = "8"

	for k, v := range testCases {
		if chash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}
}

// TestMigrateRatio 测试一致性哈希的数据迁移率
func TestMigrateRatio(t *testing.T) {
	flag.Parse()
	var keys = *keysPtr
	var nodes = *nodesPtr
	var newNodes = *newNodesPtr
	t.Logf("keys:%d, nodes:%d, newNodes:%d\n", keys, nodes, newNodes)

	c := hashingsimple.New(3, nil)
	for i := 0; i < nodes; i++ {
		c.Add(strconv.Itoa(i))
	}

	newC := hashingsimple.New(3, nil)
	for i := 0; i < newNodes; i++ {
		newC.Add(strconv.Itoa(i))
	}

	migrate := 0
	for i := 0; i < keys; i++ {
		server := c.Get(strconv.Itoa(i))
		newServer := newC.Get(strconv.Itoa(i))
		if server != newServer {
			migrate++
		}
	}
	migrateRatio := float64(migrate) / float64(keys)
	t.Logf("%f%%\n", migrateRatio*100)
}

var keysPtr = flag.Int("keys", 1000000, "key number")
var nodesPtr = flag.Int("nodes", 3, "node number of old cluster")
var newNodesPtr = flag.Int("new-nodes", 4, "node number of new cluster")

/***************************** hahinghigh 测试 *******************************/

const (
	node1 = "192.168.1.1"
	node2 = "192.168.1.2"
	node3 = "192.168.1.3"
)

func getNodesCount(nodes hashinghigh.NodesArray) (int, int, int) {
	node1Count := 0
	node2Count := 0
	node3Count := 0

	for _, node := range nodes {
		if node.NodeKey == node1 {
			node1Count++
		}
		if node.NodeKey == node2 {
			node2Count++

		}
		if node.NodeKey == node3 {
			node3Count++

		}
	}
	return node1Count, node2Count, node3Count
}

// TestHash hashing一致性高阶测试
func TestHash(t *testing.T) {

	nodeWeight := make(map[string]int)
	nodeWeight[node1] = 2
	nodeWeight[node2] = 2
	nodeWeight[node3] = 3
	vitualSpots := 100

	hash := hashinghigh.NewHashRing(vitualSpots)

	hash.AddNodes(nodeWeight)
	if hash.GetNode("1") != node3 {
		t.Fatalf("expetcd %v got %v", node3, hash.GetNode("1"))
	}
	if hash.GetNode("2") != node3 {
		t.Fatalf("expetcd %v got %v", node3, hash.GetNode("2"))
	}
	if hash.GetNode("3") != node2 {
		t.Fatalf("expetcd %v got %v", node2, hash.GetNode("3"))
	}
	c1, c2, c3 := getNodesCount(hash.Nodes)
	t.Logf("len of nodes is %v after AddNodes node1:%v, node2:%v, node3:%v", len(hash.Nodes), c1, c2, c3)

	hash.RemoveNode(node3)
	if hash.GetNode("1") != node1 {
		t.Fatalf("expetcd %v got %v", node1, hash.GetNode("1"))
	}
	if hash.GetNode("2") != node2 {
		t.Fatalf("expetcd %v got %v", node1, hash.GetNode("2"))
	}
	if hash.GetNode("3") != node2 {
		t.Fatalf("expetcd %v got %v", node2, hash.GetNode("3"))
	}
	c1, c2, c3 = getNodesCount(hash.Nodes)
	t.Logf("len of nodes is %v after RemoveNode node1:%v, node2:%v, node3:%v", len(hash.Nodes), c1, c2, c3)

	hash.AddNode(node3, 3)
	if hash.GetNode("1") != node3 {
		t.Fatalf("expetcd %v got %v", node3, hash.GetNode("1"))
	}
	if hash.GetNode("2") != node3 {
		t.Fatalf("expetcd %v got %v", node3, hash.GetNode("2"))
	}
	if hash.GetNode("3") != node2 {
		t.Fatalf("expetcd %v got %v", node2, hash.GetNode("3"))
	}
	c1, c2, c3 = getNodesCount(hash.Nodes)
	t.Logf("len of nodes is %v after AddNode node1:%v, node2:%v, node3:%v", len(hash.Nodes), c1, c2, c3)

}

func main() {
	t := &testing.T{}
	// TestConsistentHashSimple(t)

	// TestMigrateRatio()

	TestHash(t)
}
