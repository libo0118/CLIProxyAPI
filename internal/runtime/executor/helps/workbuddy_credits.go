package helps

import (
	"encoding/json"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	"github.com/tidwall/gjson"
	"math"
)

func parseWorkBuddyCredits(payload []byte) *usage.WorkBuddyCredits {
	if !gjson.ValidBytes(payload) {
		return nil
	}
	n := gjson.GetBytes(payload, "usage.credit")
	if !n.Exists() {
		n = gjson.GetBytes(payload, "response.usage.credit")
	}
	if n.Type != gjson.Number || n.Float() < 0 || math.IsInf(n.Float(), 0) || math.IsNaN(n.Float()) {
		return nil
	}
	return &usage.WorkBuddyCredits{Credits: json.Number(n.Raw)}
}
