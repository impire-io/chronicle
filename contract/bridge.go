package contract

// BridgeProfile is the hand-out that makes a bridge login possible
// (decision 0026) — where to connect, which GitHub App to log into, and
// the sentinel creds that trigger the callout. Public material only: the
// sentinel is public by design, worthless without a valid identity behind
// it. An install with the bridge enabled writes one beside its creds
// files, and it travels the same way.
type BridgeProfile struct {
	URL            string `json:"url"`
	GithubClientID string `json:"github_client_id"`
	Sentinel       string `json:"sentinel"`
}
