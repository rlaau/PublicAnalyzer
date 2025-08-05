package domain

import (
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// EdgeInfo represents evidence information for an EOA relationship
type EdgeInfo struct {
	TxID     domain.TxId
	InfoCode uint8 // Code indicating the type of evidence
}

// Edge info codes
const (
	SameDepositUsage uint8 = 1 // Both EOAs used same deposit address
	// TODO: Add more evidence types as needed
)

// EOANode represents an EOA address node in the relationship graph
type EOANode struct {
	Address   domain.Address
	FirstSeen time.Time
	LastSeen  time.Time
}

// NewEOANode creates a new EOA node
func NewEOANode(addr domain.Address) *EOANode {
	now := time.Now()
	return &EOANode{
		Address:   addr,
		FirstSeen: now,
		LastSeen:  now,
	}
}

// UpdateLastSeen updates the last seen timestamp
func (n *EOANode) UpdateLastSeen() {
	n.LastSeen = time.Now()
}

// EOAEdge represents a relationship edge between two EOA addresses
// Connection is based on shared deposit address usage
type EOAEdge struct {
	AddressA      domain.Address // First EOA address
	AddressB      domain.Address // Second EOA address
	DepositAddr   domain.Address // The shared deposit address
	Evidence      []EdgeInfo     // List of evidence supporting this connection
	FirstSeen     time.Time      // When this relationship was first detected
	LastConfirmed time.Time      // When this relationship was last confirmed
}

// NewEOAEdge creates a new EOA relationship edge
func NewEOAEdge(addrA, addrB, depositAddr domain.Address, txID domain.TxId, infoCode uint8) *EOAEdge {
	now := time.Now()
	return &EOAEdge{
		AddressA:      addrA,
		AddressB:      addrB,
		DepositAddr:   depositAddr,
		Evidence:      []EdgeInfo{{TxID: txID, InfoCode: infoCode}},
		FirstSeen:     now,
		LastConfirmed: now,
	}
}

// AddEvidence adds new evidence to this edge
func (e *EOAEdge) AddEvidence(txID domain.TxId, infoCode uint8) {
	e.Evidence = append(e.Evidence, EdgeInfo{TxID: txID, InfoCode: infoCode})
	e.LastConfirmed = time.Now()
}

// EvidenceCount returns the number of supporting evidence entries
func (e *EOAEdge) EvidenceCount() int {
	return len(e.Evidence)
}

// IsStale checks if the edge is older than the window threshold
func (e *EOAEdge) IsStale(windowThreshold time.Duration) bool {
	return time.Since(e.LastConfirmed) > windowThreshold
}

// EOASubgraph represents a subgraph result from graph database queries
// This is what we get when we query for connected EOAs
type EOASubgraph struct {
	CenterAddress domain.Address   // The address we queried for
	ConnectedEOAs []domain.Address // Directly connected EOAs
	Nodes         []*EOANode       // All nodes in this subgraph
	Edges         []*EOAEdge       // All edges in this subgraph
	QueryDepth    int              // How many hops from center
	CreatedAt     time.Time        // When this subgraph was created
}

// NewEOASubgraph creates a new EOA subgraph
func NewEOASubgraph(centerAddr domain.Address, depth int) *EOASubgraph {
	return &EOASubgraph{
		CenterAddress: centerAddr,
		ConnectedEOAs: make([]domain.Address, 0),
		Nodes:         make([]*EOANode, 0),
		Edges:         make([]*EOAEdge, 0),
		QueryDepth:    depth,
		CreatedAt:     time.Now(),
	}
}

// AddNode adds a node to the subgraph
func (sg *EOASubgraph) AddNode(node *EOANode) {
	sg.Nodes = append(sg.Nodes, node)

	// Add to connected EOAs if it's not the center address
	if node.Address != sg.CenterAddress {
		sg.ConnectedEOAs = append(sg.ConnectedEOAs, node.Address)
	}
}

// AddEdge adds an edge to the subgraph
func (sg *EOASubgraph) AddEdge(edge *EOAEdge) {
	sg.Edges = append(sg.Edges, edge)
}

// GetStats returns statistics about this subgraph
func (sg *EOASubgraph) GetStats() map[string]interface{} {
	return map[string]any{
		"center_address":  sg.CenterAddress.String(),
		"connected_count": len(sg.ConnectedEOAs),
		"total_nodes":     len(sg.Nodes),
		"total_edges":     len(sg.Edges),
		"query_depth":     sg.QueryDepth,
		"created_at":      sg.CreatedAt,
	}
}
