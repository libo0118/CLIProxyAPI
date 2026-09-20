package config

import (
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
)

func TestNativeDeepSeekThinkingDefaults(t *testing.T) {
	explicit := &registry.ThinkingSupport{Levels: []string{"high"}}
	for _, host := range []string{"https://api.deepseek.com", "https://api.deepseek.com/v1", "https://other.example/v1"} {
		cfg := Config{OpenAICompatibility: []OpenAICompatibility{{BaseURL: host, Models: []OpenAICompatibilityModel{
			{Name: "deepseek-flash", Alias: "deepseek/deepseek-flash"},
			{Name: "deepseek-v4-pro", Thinking: explicit},
			{Name: "unknown"},
		}}}}
		cfg.SanitizeOpenAICompatibility()
		models := cfg.OpenAICompatibility[0].Models
		if host == "https://other.example/v1" {
			if models[0].Thinking != nil {
				t.Fatal("guessed another gateway's contract")
			}
		} else if models[0].Thinking == nil || !models[0].Thinking.ZeroAllowed || !reflect.DeepEqual(models[0].Thinking.Levels, []string{"low", "high", "max"}) {
			t.Fatalf("wrong defaults: %#v", models[0].Thinking)
		}
		if models[1].Thinking != explicit || models[2].Thinking != nil {
			t.Fatal("overwrote explicit or unknown model capabilities")
		}
	}
}
