package usage

import "encoding/json"

// QoderCredits is upstream billing metadata, independent of request success and USD pricing.
// Numbers retain their JSON decimal representation; missing fields are not zero.
type QoderCredits struct {
	Credits         json.Number `json:"credits,omitempty"`
	OriginalCredits json.Number `json:"original_credits,omitempty"`
	Billable        *bool       `json:"billable,omitempty"`
}
