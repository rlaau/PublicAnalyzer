package domain

import "strings"

// CEXAddress represents a centralized exchange hot wallet address
type CEXAddress struct {
	Address string
}

// NewCEXAddress creates a new CEX address
func NewCEXAddress(address string) *CEXAddress {
	return &CEXAddress{
		Address: strings.ToLower(strings.TrimSpace(address)),
	}
}

// IsValid checks if the address is valid (basic validation)
func (c *CEXAddress) IsValid() bool {
	// Remove any trailing tabs or whitespace that might be in the file
	address := strings.TrimSpace(c.Address)
	return len(address) == 42 && strings.HasPrefix(address, "0x")
}

// CEXSet represents a collection of CEX addresses for fast lookup
type CEXSet struct {
	addresses map[string]bool
}

// NewCEXSet creates a new CEX address set
func NewCEXSet() *CEXSet {
	return &CEXSet{
		addresses: make(map[string]bool),
	}
}

// Add adds a CEX address to the set
func (s *CEXSet) Add(address string) {
	normalized := strings.ToLower(strings.TrimSpace(address))
	s.addresses[normalized] = true
}

// Contains checks if an address is in the CEX set
func (s *CEXSet) Contains(address string) bool {
	normalized := strings.ToLower(strings.TrimSpace(address))
	return s.addresses[normalized]
}

// Size returns the number of addresses in the set
func (s *CEXSet) Size() int {
	return len(s.addresses)
}

// GetAll returns all addresses in the set
func (s *CEXSet) GetAll() []string {
	addresses := make([]string, 0, len(s.addresses))
	for addr := range s.addresses {
		addresses = append(addresses, addr)
	}
	return addresses
}