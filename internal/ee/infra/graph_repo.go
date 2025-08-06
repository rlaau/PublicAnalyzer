package infra

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/ct"
)

// GraphRepository defines the interface for graph database operations
type GraphRepository interface {
	// Node operations
	SaveNode(node *domain.EOANode) error
	GetNode(addr shareddomain.Address) (*domain.EOANode, error)
	UpdateNodeLastSeen(addr shareddomain.Address) error

	// Edge operations
	SaveEdge(edge *domain.EOAEdge) error
	GetEdge(addrA, addrB shareddomain.Address) (*domain.EOAEdge, error)
	UpdateEdgeEvidence(addrA, addrB shareddomain.Address, txID shareddomain.TxId, infoCode uint8) error

	// Graph traversal operations
	GetConnectedEOAs(addr shareddomain.Address, maxDepth int) (*domain.EOASubgraph, error)
	GetEOAsUsingDeposit(depositAddr shareddomain.Address) ([]shareddomain.Address, error)
	FindShortestPath(addrA, addrB shareddomain.Address, maxDepth int) ([]*domain.EOAEdge, error)

	// Cleanup operations
	DeleteStaleNodes(threshold ct.ChainTime) error
	DeleteStaleEdges(threshold ct.ChainTime) error

	// Statistics
	GetGraphStats() (map[string]any, error)

	// Resource management
	Close() error
}

// BadgerGraphRepository implements GraphRepository using BadgerDB
type BadgerGraphRepository struct {
	db *badger.DB
}

// Key prefixes for different data types
const (
	NodePrefix = "n:"
	AdjPrefix  = "adj:"
)

// NewBadgerGraphRepository creates a new BadgerDB graph repository
func NewBadgerGraphRepository(dbPath string) (*BadgerGraphRepository, error) {
	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil // Disable badger logging

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	return &BadgerGraphRepository{
		db: db,
	}, nil
}

// Close closes the BadgerDB connection
func (r *BadgerGraphRepository) Close() error {
	return r.db.Close()
}

// nodeKey generates a key for storing EOA nodes
func nodeKey(addr shareddomain.Address) []byte {
	key := make([]byte, len(NodePrefix)+len(addr))
	copy(key, NodePrefix)
	copy(key[len(NodePrefix):], addr[:])
	return key
}

// adjacencyKey generates a key for storing adjacency list
func adjacencyKey(addr shareddomain.Address) []byte {
	key := make([]byte, len(AdjPrefix)+len(addr))
	copy(key, AdjPrefix)
	copy(key[len(AdjPrefix):], addr[:])
	return key
}

// parseAddressFromString converts hex string to Address
func parseAddressFromString(hexStr string) (shareddomain.Address, error) {
	var addr shareddomain.Address

	// Remove 0x prefix if present
	if strings.HasPrefix(hexStr, "0x") {
		hexStr = hexStr[2:]
	}

	if len(hexStr) != 40 {
		return addr, fmt.Errorf("invalid address length: %d", len(hexStr))
	}

	// Convert hex string to bytes
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return addr, fmt.Errorf("invalid hex string: %w", err)
	}

	copy(addr[:], bytes)
	return addr, nil
}

// parseTxIdFromString converts hex string to TxId
func parseTxIdFromString(hexStr string) (shareddomain.TxId, error) {
	var txID shareddomain.TxId

	// Remove 0x prefix if present
	if strings.HasPrefix(hexStr, "0x") {
		hexStr = hexStr[2:]
	}

	if len(hexStr) != 64 {
		return txID, fmt.Errorf("invalid tx ID length: %d", len(hexStr))
	}

	// Convert hex string to bytes
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return txID, fmt.Errorf("invalid hex string: %w", err)
	}

	copy(txID[:], bytes)
	return txID, nil
}

// NodeData represents serialized node data
type NodeData struct {
	Address   string `json:"address"`
	FirstSeen int64  `json:"firstSeen"`
	LastSeen  int64  `json:"lastSeen"`
}

// ConnectionData represents a connection to another EOA with minimal evidence
type ConnectionData struct {
	ConnectedAddr string `json:"connectedAddr"`
	DepositAddr   string `json:"depositAddr"`
	FirstDetectTx string `json:"firstDetectTx"` // First transaction that detected this connection
	TxCount       int64  `json:"txCount"`       // Number of transactions supporting this connection
	TotalVolume   string `json:"totalVolume"`   // Total transaction volume (as string to handle big numbers)
	FirstSeen     int64  `json:"firstSeen"`
	LastConfirmed int64  `json:"lastConfirmed"`
}

// AdjacencyData represents the adjacency list for a node
type AdjacencyData struct {
	Address     string           `json:"address"`
	Connections []ConnectionData `json:"connections"`
}

// SaveNode saves an EOA node to BadgerDB
func (r *BadgerGraphRepository) SaveNode(node *domain.EOANode) error {
	nodeData := NodeData{
		Address:   node.Address.String(),
		FirstSeen: node.FirstSeen.Unix(),
		LastSeen:  node.LastSeen.Unix(),
	}

	data, err := json.Marshal(nodeData)
	if err != nil {
		return fmt.Errorf("failed to marshal node data: %w", err)
	}

	key := nodeKey(node.Address)

	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

// GetNode retrieves an EOA node from BadgerDB
func (r *BadgerGraphRepository) GetNode(addr shareddomain.Address) (*domain.EOANode, error) {
	key := nodeKey(addr)
	var nodeData NodeData

	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &nodeData)
		})
	})

	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, fmt.Errorf("node not found")
		}
		return nil, err
	}

	return &domain.EOANode{
		Address:   addr,
		FirstSeen: ct.Unix(nodeData.FirstSeen, 0),
		LastSeen:  ct.Unix(nodeData.LastSeen, 0),
	}, nil
}

// UpdateNodeLastSeen updates the last seen timestamp of a node
func (r *BadgerGraphRepository) UpdateNodeLastSeen(addr shareddomain.Address) error {
	key := nodeKey(addr)

	return r.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		var nodeData NodeData
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &nodeData)
		})
		if err != nil {
			return err
		}
		//*조금 불안정하긴 함. 99%정확도는 보장하지만, 1%의 손실은 가능
		//* tx를 바로 분석하는게 아니라, 시간을 체인 시스템 시간으로부터 로드함
		nodeData.LastSeen = ct.Now().Unix()

		data, err := json.Marshal(nodeData)
		if err != nil {
			return err
		}

		return txn.Set(key, data)
	})
}

// SaveEdge saves an EOA edge by updating adjacency lists of both nodes
func (r *BadgerGraphRepository) SaveEdge(edge *domain.EOAEdge) error {
	// Calculate total volume from evidence (assuming we can derive it)
	totalVolume := "0" // TODO: Calculate from transaction data if available

	return r.db.Update(func(txn *badger.Txn) error {
		// Update adjacency list for addressA
		if err := r.addConnectionToAdjacency(txn, edge.AddressA, edge.AddressB, edge.DepositAddr, edge.Evidence, totalVolume, edge.FirstSeen, edge.LastConfirmed); err != nil {
			return err
		}

		// Update adjacency list for addressB
		return r.addConnectionToAdjacency(txn, edge.AddressB, edge.AddressA, edge.DepositAddr, edge.Evidence, totalVolume, edge.FirstSeen, edge.LastConfirmed)
	})
}

// addConnectionToAdjacency adds a connection to a node's adjacency list
func (r *BadgerGraphRepository) addConnectionToAdjacency(txn *badger.Txn, fromAddr, toAddr, depositAddr shareddomain.Address, evidence []domain.EdgeInfo, totalVolume string, firstSeen, lastConfirmed ct.ChainTime) error {
	key := adjacencyKey(fromAddr)

	var adjData AdjacencyData

	// Try to get existing adjacency data
	item, err := txn.Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return err
	}

	if err == badger.ErrKeyNotFound {
		// Create new adjacency list
		adjData = AdjacencyData{
			Address:     fromAddr.String(),
			Connections: []ConnectionData{},
		}
	} else {
		// Load existing adjacency list
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &adjData)
		})
		if err != nil {
			return err
		}
	}

	// Get first detection transaction (first evidence)
	firstDetectTx := ""
	if len(evidence) > 0 {
		firstDetectTx = evidence[0].TxID.String()
	}

	// Check if connection already exists
	connectionExists := false
	for i := range adjData.Connections {
		if adjData.Connections[i].ConnectedAddr == toAddr.String() {
			// Update existing connection
			adjData.Connections[i].TxCount = int64(len(evidence))
			adjData.Connections[i].TotalVolume = totalVolume
			adjData.Connections[i].LastConfirmed = lastConfirmed.Unix()
			connectionExists = true
			break
		}
	}

	if !connectionExists {
		// Add new connection
		newConnection := ConnectionData{
			ConnectedAddr: toAddr.String(),
			DepositAddr:   depositAddr.String(),
			FirstDetectTx: firstDetectTx,
			TxCount:       int64(len(evidence)),
			TotalVolume:   totalVolume,
			FirstSeen:     firstSeen.Unix(),
			LastConfirmed: lastConfirmed.Unix(),
		}
		adjData.Connections = append(adjData.Connections, newConnection)
	}

	// Save updated adjacency list
	data, err := json.Marshal(adjData)
	if err != nil {
		return err
	}

	return txn.Set(key, data)
}

// GetEdge retrieves an EOA edge by checking adjacency list
func (r *BadgerGraphRepository) GetEdge(addrA, addrB shareddomain.Address) (*domain.EOAEdge, error) {
	key := adjacencyKey(addrA)
	var adjData AdjacencyData

	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &adjData)
		})
	})

	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, fmt.Errorf("edge not found")
		}
		return nil, err
	}

	// Find connection to addrB
	for _, conn := range adjData.Connections {
		if conn.ConnectedAddr == addrB.String() {
			// Parse addresses
			depositAddr, err := parseAddressFromString(conn.DepositAddr)
			if err != nil {
				return nil, fmt.Errorf("failed to parse deposit address: %w", err)
			}

			// Create minimal evidence from stored data
			firstTxID, err := parseTxIdFromString(conn.FirstDetectTx)
			if err != nil {
				return nil, fmt.Errorf("failed to parse first detect tx: %w", err)
			}

			evidence := []domain.EdgeInfo{
				{
					TxID:     firstTxID,
					InfoCode: domain.SameDepositUsage,
				},
			}

			return &domain.EOAEdge{
				AddressA:      addrA,
				AddressB:      addrB,
				DepositAddr:   depositAddr,
				Evidence:      evidence,
				FirstSeen:     ct.Unix(conn.FirstSeen, 0),
				LastConfirmed: ct.Unix(conn.LastConfirmed, 0),
			}, nil
		}
	}

	return nil, fmt.Errorf("edge not found")
}

// UpdateEdgeEvidence adds new evidence to an existing edge
func (r *BadgerGraphRepository) UpdateEdgeEvidence(addrA, addrB shareddomain.Address, txID shareddomain.TxId, infoCode uint8) error {
	return r.db.Update(func(txn *badger.Txn) error {
		// Update both adjacency lists
		if err := r.updateConnectionEvidence(txn, addrA, addrB, txID, infoCode); err != nil {
			return err
		}

		return r.updateConnectionEvidence(txn, addrB, addrA, txID, infoCode)
	})
}

// updateConnectionEvidence updates evidence for a specific connection
func (r *BadgerGraphRepository) updateConnectionEvidence(txn *badger.Txn, fromAddr, toAddr shareddomain.Address, txID shareddomain.TxId, infoCode uint8) error {
	key := adjacencyKey(fromAddr)

	item, err := txn.Get(key)
	if err != nil {
		return err
	}

	var adjData AdjacencyData
	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &adjData)
	})
	if err != nil {
		return err
	}

	// Find and update connection
	for i := range adjData.Connections {
		if adjData.Connections[i].ConnectedAddr == toAddr.String() {
			adjData.Connections[i].TxCount++
			adjData.Connections[i].LastConfirmed = ct.Now().Unix()
			// TODO: Update total volume if transaction value is available
			break
		}
	}

	data, err := json.Marshal(adjData)
	if err != nil {
		return err
	}

	return txn.Set(key, data)
}

// GetConnectedEOAs retrieves all EOAs connected to the given address within maxDepth hops
func (r *BadgerGraphRepository) GetConnectedEOAs(addr shareddomain.Address, maxDepth int) (*domain.EOASubgraph, error) {
	subgraph := domain.NewEOASubgraph(addr, maxDepth)
	visited := make(map[string]bool)
	queue := []shareddomain.Address{addr}
	currentDepth := 0

	err := r.db.View(func(txn *badger.Txn) error {
		for currentDepth < maxDepth && len(queue) > 0 {
			nextQueue := []shareddomain.Address{}

			for _, currentAddr := range queue {
				if visited[currentAddr.String()] {
					continue
				}
				visited[currentAddr.String()] = true

				// Get adjacency list for current address
				connectedAddrs, edges, err := r.getAdjacencyList(txn, currentAddr)
				if err != nil {
					continue // Skip if no adjacency list found
				}

				for i, connectedAddr := range connectedAddrs {
					if !visited[connectedAddr.String()] {
						nextQueue = append(nextQueue, connectedAddr)

						// Add node to subgraph
						node, err := r.getNodeFromTxn(txn, connectedAddr)
						if err == nil {
							subgraph.AddNode(node)
						}

						// Add edge to subgraph
						subgraph.AddEdge(edges[i])
					}
				}
			}

			queue = nextQueue
			currentDepth++
		}
		return nil
	})

	return subgraph, err
}

// getAdjacencyList retrieves the adjacency list for a given address
func (r *BadgerGraphRepository) getAdjacencyList(txn *badger.Txn, addr shareddomain.Address) ([]shareddomain.Address, []*domain.EOAEdge, error) {
	key := adjacencyKey(addr)

	item, err := txn.Get(key)
	if err != nil {
		return nil, nil, err
	}

	var adjData AdjacencyData
	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &adjData)
	})
	if err != nil {
		return nil, nil, err
	}

	var connectedAddrs []shareddomain.Address
	var edges []*domain.EOAEdge

	for _, conn := range adjData.Connections {
		connectedAddr, err := parseAddressFromString(conn.ConnectedAddr)
		if err != nil {
			continue
		}

		depositAddr, err := parseAddressFromString(conn.DepositAddr)
		if err != nil {
			continue
		}

		// Create minimal evidence from stored data
		firstTxID, err := parseTxIdFromString(conn.FirstDetectTx)
		if err != nil {
			continue
		}

		evidence := []domain.EdgeInfo{
			{
				TxID:     firstTxID,
				InfoCode: domain.SameDepositUsage,
			},
		}

		connectedAddrs = append(connectedAddrs, connectedAddr)
		edges = append(edges, &domain.EOAEdge{
			AddressA:      addr,
			AddressB:      connectedAddr,
			DepositAddr:   depositAddr,
			Evidence:      evidence,
			FirstSeen:     ct.Unix(conn.FirstSeen, 0),
			LastConfirmed: ct.Unix(conn.LastConfirmed, 0),
		})
	}

	return connectedAddrs, edges, nil
}

// getNodeFromTxn retrieves a node within a transaction
func (r *BadgerGraphRepository) getNodeFromTxn(txn *badger.Txn, addr shareddomain.Address) (*domain.EOANode, error) {
	key := nodeKey(addr)
	var nodeData NodeData

	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}

	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &nodeData)
	})
	if err != nil {
		return nil, err
	}

	return &domain.EOANode{
		Address:   addr,
		FirstSeen: ct.Unix(nodeData.FirstSeen, 0),
		LastSeen:  ct.Unix(nodeData.LastSeen, 0),
	}, nil
}

// GetEOAsUsingDeposit retrieves all EOAs that use the given deposit address
func (r *BadgerGraphRepository) GetEOAsUsingDeposit(depositAddr shareddomain.Address) ([]shareddomain.Address, error) {
	var addresses []shareddomain.Address
	addressSet := make(map[string]bool) // Avoid duplicates

	err := r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(AdjPrefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			var adjData AdjacencyData
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &adjData)
			})
			if err != nil {
				continue
			}

			// Check if any connection uses the target deposit address
			for _, conn := range adjData.Connections {
				if conn.DepositAddr == depositAddr.String() {
					addressSet[adjData.Address] = true
					addressSet[conn.ConnectedAddr] = true
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Convert to address slice
	for addrStr := range addressSet {
		addr, err := parseAddressFromString(addrStr)
		if err == nil {
			addresses = append(addresses, addr)
		}
	}

	return addresses, nil
}

// FindShortestPath finds the shortest path between two EOAs using BFS
func (r *BadgerGraphRepository) FindShortestPath(addrA, addrB shareddomain.Address, maxDepth int) ([]*domain.EOAEdge, error) {
	//TODO: Implement BFS shortest path algorithm using adjacency lists
	return nil, fmt.Errorf("shortest path not implemented yet")
}

// DeleteStaleNodes removes nodes older than threshold
func (r *BadgerGraphRepository) DeleteStaleNodes(threshold ct.ChainTime) error {
	thresholdUnix := threshold.Unix()

	return r.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(NodePrefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		var keysToDelete [][]byte
		var addrsToDeleteAdj []shareddomain.Address

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			var nodeData NodeData
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &nodeData)
			})
			if err != nil {
				continue
			}

			if nodeData.LastSeen < thresholdUnix {
				keysToDelete = append(keysToDelete, item.KeyCopy(nil))
				addr, err := parseAddressFromString(nodeData.Address)
				if err == nil {
					addrsToDeleteAdj = append(addrsToDeleteAdj, addr)
				}
			}
		}

		// Delete stale nodes
		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		// Delete corresponding adjacency lists
		for _, addr := range addrsToDeleteAdj {
			adjKey := adjacencyKey(addr)
			if err := txn.Delete(adjKey); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}

		return nil
	})
}

// DeleteStaleEdges removes edges older than threshold from adjacency lists
func (r *BadgerGraphRepository) DeleteStaleEdges(threshold ct.ChainTime) error {
	thresholdUnix := threshold.Unix()

	return r.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(AdjPrefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		var adjUpdates []struct {
			key  []byte
			data AdjacencyData
		}

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			var adjData AdjacencyData
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &adjData)
			})
			if err != nil {
				continue
			}

			// Filter out stale connections
			var freshConnections []ConnectionData
			for _, conn := range adjData.Connections {
				if conn.LastConfirmed >= thresholdUnix {
					freshConnections = append(freshConnections, conn)
				}
			}

			// Update if connections were removed
			if len(freshConnections) != len(adjData.Connections) {
				adjData.Connections = freshConnections
				adjUpdates = append(adjUpdates, struct {
					key  []byte
					data AdjacencyData
				}{
					key:  item.KeyCopy(nil),
					data: adjData,
				})
			}
		}

		// Apply updates
		for _, update := range adjUpdates {
			data, err := json.Marshal(update.data)
			if err != nil {
				continue
			}

			if err := txn.Set(update.key, data); err != nil {
				return err
			}
		}

		return nil
	})
}

// GetGraphStats returns statistics about the graph
func (r *BadgerGraphRepository) GetGraphStats() (map[string]any, error) {
	var nodeCount, totalConnections int

	err := r.db.View(func(txn *badger.Txn) error {
		// Count nodes
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(NodePrefix)
		it := txn.NewIterator(opts)

		for it.Rewind(); it.Valid(); it.Next() {
			nodeCount++
		}
		it.Close()

		// Count connections from adjacency lists
		opts.Prefix = []byte(AdjPrefix)
		it = txn.NewIterator(opts)

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			var adjData AdjacencyData
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &adjData)
			})
			if err != nil {
				continue
			}

			totalConnections += len(adjData.Connections)
		}
		it.Close()

		return nil
	})

	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"total_nodes":       nodeCount,
		"total_connections": totalConnections,
		"total_edges":       totalConnections / 2, // Each edge is counted twice in adjacency lists
	}, nil
}
