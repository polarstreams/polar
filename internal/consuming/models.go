package consuming

import . "github.com/barcostreams/barco/internal/types"

// Represents a single consumer instance
type ConsumerInfo struct {
	Id     string   `json:"id"`    // A unique id within the consumer group
	Group  string   `json:"group"` // A group unique id
	Topics []string `json:"topics"`

	// Only used internally
	assignedTokens []Token
}
