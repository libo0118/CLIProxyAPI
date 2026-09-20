package helps

import (
	"encoding/json"
	"math"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	"github.com/tidwall/gjson"
)

func parseQoderCredits(payload []byte) *usage.QoderCredits {
	if !gjson.ValidBytes(payload) {
		return nil
	}
	node := gjson.GetBytes(payload, "usage")
	if !node.IsObject() {
		node = gjson.GetBytes(payload, "response.usage")
	}
	if !node.IsObject() {
		return nil
	}
	value := &usage.QoderCredits{}
	number := func(key string) json.Number {
		n := node.Get(key)
		if n.Type != gjson.Number || n.Float() < 0 || math.IsInf(n.Float(), 0) || math.IsNaN(n.Float()) {
			return ""
		}
		return json.Number(n.Raw)
	}
	value.Credits = number("credits")
	value.OriginalCredits = number("original_credits")
	flag := node.Get("billable")
	if flag.Type == gjson.True || flag.Type == gjson.False {
		billable := flag.Bool()
		value.Billable = &billable
	}
	if value.Credits == "" && value.OriginalCredits == "" && value.Billable == nil {
		return nil
	}
	return value
}
