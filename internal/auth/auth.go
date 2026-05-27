package auth

// Auth performs authentication and authorization.
// When Key is set, tokens are verified as JWTs signed by the public key.
// When Key is nil, all requests are allowed (debug/test mode).
type Auth struct {
	Key []byte // PEM-encoded public key; nil = auth disabled
	Iss string // expected issuer claim (optional)
	Aud string // expected audience claim (optional)
}

// Check verifies the token from the request head.
// Returns nil if the request is allowed, or an error with the appropriate
// status (401 unauthorized, 403 forbidden).
//
// TODO: implement JWT verification (RS256/ES256/EdDSA), role and prefix checks.
func (a *Auth) Check(token string) error {
	return nil
}
