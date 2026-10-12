package cli

import "github.com/impire-io/chronicle/client"

// SetBridgeDial swaps how account sentences dial through a bridge and
// returns the undo — the tests observe the dial without a bridge to dial.
func SetBridgeDial(f func(profile, account string) (*client.Client, error)) (restore func()) {
	prev := dialThroughBridge
	dialThroughBridge = f
	return func() { dialThroughBridge = prev }
}

// SetIdentityDial swaps how the service-account sentences reach the
// install as the signed-in identity, and returns the undo.
func SetIdentityDial(f func(profile string) (IdentityPlane, error)) (restore func()) {
	prev := dialIdentityThroughBridge
	dialIdentityThroughBridge = f
	return func() { dialIdentityThroughBridge = prev }
}
