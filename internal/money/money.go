// Package money represents currency as integer micro-cents to avoid
// floating point rounding errors in financial accounting.
package money

import "fmt"

// Micros represents an amount in micro-units of currency (1 unit = 1,000,000 micros).
// Using integers avoids float64 rounding drift across millions of reservations.
type Micros int64

const OneUnit Micros = 1_000_000

// FromFloat converts a float64 dollar amount (e.g. 0.042) to Micros.
// Only use this at system boundaries (parsing config/JSON); do all arithmetic in Micros.
func FromFloat(f float64) Micros {
	return Micros(f * float64(OneUnit))
}

func (m Micros) Float() float64 {
	return float64(m) / float64(OneUnit)
}

func (m Micros) String() string {
	return fmt.Sprintf("$%.4f", m.Float())
}

func (m Micros) Add(o Micros) Micros { return m + o }
func (m Micros) Sub(o Micros) Micros { return m - o }

func Max(a, b Micros) Micros {
	if a > b {
		return a
	}
	return b
}

func Min(a, b Micros) Micros {
	if a < b {
		return a
	}
	return b
}

// --- Postgres boundary ---
//
// The `reservations`/`budget_accounts` schema (migrations/0002_...) stores
// amounts in a BIGINT column named `*_minor_units`. In this codebase that
// name is defined to mean exactly one thing: the same Micros integer used
// everywhere in Go, stored as-is — NOT the ISO 4217 sense of "minor unit"
// (e.g. cents, 2 decimal places). Money never leaves Go as anything other
// than Micros; these two functions are the single named seam where that
// int64 crosses into/out of a database column, so a future change to the
// on-disk scale (e.g. to true cents) only ever touches this one place
// instead of every call site that touches the DB.
func ToMinorUnits(m Micros) int64   { return int64(m) }
func FromMinorUnits(v int64) Micros { return Micros(v) }
