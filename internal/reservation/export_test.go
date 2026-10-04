package reservation

// IsolatedDSN exposes the throwaway-schema DSN created by TestMain so the
// external test package (reservation_test, which may import gateway without an
// import cycle) can run against the same isolated schema.
func IsolatedDSN() string { return isoDSN }
