package money

import "testing"

func TestMinorUnits_RoundTrip(t *testing.T) {
	cases := []Micros{0, OneUnit, FromFloat(0.01), FromFloat(3.50), FromFloat(-1.25), Micros(1)}
	for _, m := range cases {
		v := ToMinorUnits(m)
		back := FromMinorUnits(v)
		if back != m {
			t.Errorf("round trip broke: %v -> %d -> %v", m, v, back)
		}
	}
}

// TestMinorUnits_IsIdentity locks in the specific mapping documented in
// money.go: minor_units in this codebase IS the Micros integer, not cents.
// If this ever needs to change, it should change loudly (this test
// failing), not silently.
func TestMinorUnits_IsIdentity(t *testing.T) {
	if got := ToMinorUnits(FromFloat(3.50)); got != 3_500_000 {
		t.Fatalf("expected $3.50 to be 3,500,000 minor units (micros), got %d", got)
	}
	if got := FromMinorUnits(3_500_000); got != FromFloat(3.50) {
		t.Fatalf("expected 3,500,000 minor units to be $3.50, got %s", got)
	}
}
