package chat

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"agi-assistant/config"
	"agi-assistant/internal/infrastructure/llm"
	"agi-assistant/internal/pkg/logger"
)

func TestSearchWebLogsTavilyFailureBeforeLLMFallback(t *testing.T) {
	cfg := &config.APIConfig{
		SearchConfig: config.SearchConfig{
			SearchAPIKey: "test-key",
			SearchAPIURL: "://invalid-url",
		},
	}
	agent := &UnifiedAgent{
		cfg:   cfg,
		llm:   llm.New(cfg),
		tools: newToolRegistry(nil),
	}

	previousLogger := logger.L()
	var logs bytes.Buffer
	logger.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer logger.SetDefault(previousLogger)

	agent.registerBuiltinTools()
	result, err := agent.tools.snapshot()["search_web"].Execute(map[string]interface{}{
		"query": "今天是几月几号",
	})

	if err != nil {
		t.Fatalf("search_web returned error: %v", err)
	}
	if result == "" {
		t.Fatal("expected LLM fallback result")
	}
	for _, want := range []string{
		"Tavily search failed, falling back to LLM",
		`"query":"今天是几月几号"`,
		"Tavily 请求失败",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("expected log to contain %q, got %s", want, logs.String())
		}
	}
}
