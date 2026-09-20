package usage

import "encoding/json"

// WorkBuddyCredits preserves the upstream usage.credit amount without inferring discounts or USD.
type WorkBuddyCredits struct {
	Credits json.Number `json:"credits,omitempty"`
}
