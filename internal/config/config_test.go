package config

import "testing"

func TestValidate_DefaultRateLimitsAreValid(t *testing.T) {
	c := Config{
		Addr:                  ":8080",
		StoreMode:             StoreModeMemory,
		RateLimitIPPerSec:     20,
		RateLimitIPBurst:      40,
		RateLimitTenantPerSec: 10,
		RateLimitTenantBurst:  20,
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("expected default-shaped rate limits to validate, got: %v", err)
	}
}

func TestValidate_ZeroRateLimitsDisableAndAreValid(t *testing.T) {
	// 0 means "disabled" (see Server.SetRateLimits), not invalid.
	c := Config{Addr: ":8080", StoreMode: StoreModeMemory}
	if err := c.Validate(); err != nil {
		t.Fatalf("expected all-zero rate limits to validate (disabled), got: %v", err)
	}
}

func TestValidate_NegativeRateLimitRejected(t *testing.T) {
	c := Config{Addr: ":8080", StoreMode: StoreModeMemory, RateLimitIPPerSec: -1}
	if err := c.Validate(); err == nil {
		t.Fatal("expected a negative rate limit to be rejected")
	}
}

func TestValidate_PostgresRequiresOperatorToken(t *testing.T) {
	c := Config{Addr: ":8080", StoreMode: StoreModePostgres, PostgresDSN: "postgres://x"}
	if err := c.Validate(); err == nil {
		t.Fatal("expected postgres mode without an operator token to be rejected")
	}
}

func TestValidate_UpstreamRequiresPostgres(t *testing.T) {
	c := Config{
		Addr:        ":8080",
		StoreMode:   StoreModeMemory,
		UpstreamURL: "https://api.example.com",
	}
	if err := c.Validate(); err == nil {
		t.Fatal("expected upstream with memory store to be rejected")
	}
}

func TestValidate_UpstreamAllowsPostgres(t *testing.T) {
	c := Config{
		Addr:          ":8080",
		StoreMode:     StoreModePostgres,
		PostgresDSN:   "postgres://example",
		OperatorToken: "01234567890123456789012345678901",
		UpstreamURL:   "https://api.example.com",
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("expected postgres upstream config to validate: %v", err)
	}
}

func TestValidate_WhitespaceUpstreamTreatedAsEmpty(t *testing.T) {
	c := Config{
		Addr:        ":8080",
		StoreMode:   StoreModeMemory,
		UpstreamURL: "   ",
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("whitespace-only upstream must not trip the guard: %v", err)
	}
}
