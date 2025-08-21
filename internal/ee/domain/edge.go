package domain

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// OldEdgeInfo represents evidence information for an EOA relationship
type OldEdgeInfo struct {
	TxID     domain.TxId
	InfoCode uint8 // Code indicating the type of evidence
}

// Edge info codes
const (
	SameDepositUsage uint8 = 1 // Both EOAs used same deposit address
	// TODO: Add more evidence types as needed
)

// OldEOANode represents an EOA address node in the relationship graph
type OldEOANode struct {
	Address   domain.Address
	FirstSeen chaintimer.ChainTime
	LastSeen  chaintimer.ChainTime
}

// NewEOANode creates a new EOA node
func NewEOANode(addr domain.Address) *OldEOANode {
	//now := chaintimer.Now()
	return &OldEOANode{
		Address: addr,
		//FirstSeen: now,
		//LastSeen:  now,
	}
}

// UpdateLastSeen updates the last seen timestamp
func (n *OldEOANode) UpdateLastSeen() {
	//n.LastSeen = chaintimer.Now()
}

// OldEOAEdge represents a relationship edge between two EOA addresses
// Connection is based on shared deposit address usage
type OldEOAEdge struct {
	AddressA      domain.Address       // First EOA address
	AddressB      domain.Address       // Second EOA address
	DepositAddr   domain.Address       // The shared deposit address
	Evidence      []OldEdgeInfo        // List of evidence supporting this connection
	FirstSeen     chaintimer.ChainTime // When this relationship was first detected
	LastConfirmed chaintimer.ChainTime // When this relationship was last confirmed
}

// NewEOAEdge creates a new EOA relationship edge
func NewEOAEdge(addrA, addrB, depositAddr domain.Address, txID domain.TxId, infoCode uint8) *OldEOAEdge {
	//now := chaintimer.Now()
	return &OldEOAEdge{
		AddressA:    addrA,
		AddressB:    addrB,
		DepositAddr: depositAddr,
		Evidence:    []OldEdgeInfo{{TxID: txID, InfoCode: infoCode}},
		//FirstSeen:     now,
		//LastConfirmed: now,
	}
}

// AddEvidence adds new evidence to this edge
func (e *OldEOAEdge) AddEvidence(txID domain.TxId, infoCode uint8) {
	e.Evidence = append(e.Evidence, OldEdgeInfo{TxID: txID, InfoCode: infoCode})
	//e.LastConfirmed = chaintimer.Now()
}

// EvidenceCount returns the number of supporting evidence entries
func (e *OldEOAEdge) EvidenceCount() int {
	return len(e.Evidence)
}

// IsStale checks if the edge is older than the window threshold
func (e *OldEOAEdge) IsStale(windowThreshold chaintimer.ChainDuration) bool {
	//return chaintimer.Since(e.LastConfirmed) > windowThreshold
	return false
}

// EOASubgraph represents a subgraph result from graph database queries
// This is what we get when we query for connected EOAs
type EOASubgraph struct {
	CenterAddress domain.Address       // The address we queried for
	ConnectedEOAs []domain.Address     // Directly connected EOAs
	Nodes         []*OldEOANode        // All nodes in this subgraph
	Edges         []*OldEOAEdge        // All edges in this subgraph
	QueryDepth    int                  // How many hops from center
	CreatedAt     chaintimer.ChainTime // When this subgraph was created
}

// NewEOASubgraph creates a new EOA subgraph
func NewEOASubgraph(centerAddr domain.Address, depth int) *EOASubgraph {
	return &EOASubgraph{
		CenterAddress: centerAddr,
		ConnectedEOAs: make([]domain.Address, 0),
		Nodes:         make([]*OldEOANode, 0),
		Edges:         make([]*OldEOAEdge, 0),
		QueryDepth:    depth,
		//CreatedAt:     chaintimer.Now(),
	}
}

// AddNode adds a node to the subgraph
func (sg *EOASubgraph) AddNode(node *OldEOANode) {
	sg.Nodes = append(sg.Nodes, node)

	// Add to connected EOAs if it's not the center address
	if node.Address != sg.CenterAddress {
		sg.ConnectedEOAs = append(sg.ConnectedEOAs, node.Address)
	}
}

// AddEdge adds an edge to the subgraph
func (sg *EOASubgraph) AddEdge(edge *OldEOAEdge) {
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
