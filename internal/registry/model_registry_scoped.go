package registry

import "sort"

// GetModelsForClients never reads or populates the global handler-only cache.
func (r *ModelRegistry) GetModelsForClients(clientIDs []string, handlerType string) []map[string]any {
	infos := r.GetModelInfosForClients(clientIDs)
	models := make([]map[string]any, 0, len(infos))
	for _, info := range infos {
		model := r.convertModelToMap(info, handlerType)
		if model == nil {
			continue
		}
		if handlerType == "openai" {
			if info.Thinking != nil {
				model["thinking"] = info.Thinking
			} else if info.ExplicitThinking {
				model["thinking"] = &ThinkingSupport{Levels: []string{}}
			}
			if len(info.SupportedInputModalities) > 0 {
				model["supported_input_modalities"] = append([]string(nil), info.SupportedInputModalities...)
			}
		}
		models = append(models, model)
	}
	return models
}

func (r *ModelRegistry) GetModelInfosForClients(clientIDs []string) []*ModelInfo {
	ids := append([]string(nil), clientIDs...)
	sort.Strings(ids)
	seen := map[string]bool{}
	out := []*ModelInfo{}
	for _, clientID := range ids {
		for _, info := range r.GetModelsForClient(clientID) {
			if info != nil && !seen[info.ID] {
				seen[info.ID] = true
				out = append(out, info)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *ModelRegistry) GetModelProvidersForClients(model string, clientIDs []string) []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	seen := map[string]bool{}
	out := []string{}
	for _, id := range clientIDs {
		for _, name := range r.clientModels[id] {
			if name == model {
				p := r.clientProviders[id]
				if p != "" && !seen[p] {
					seen[p] = true
					out = append(out, p)
				}
				break
			}
		}
	}
	sort.Strings(out)
	return out
}
