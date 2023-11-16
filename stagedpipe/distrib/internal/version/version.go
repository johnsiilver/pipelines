// Package version defines the version of the plugin.
package version

import (
	messages "github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/messages/proto"
)

// Semantic is the version of the plugin or the controller (depending on which is using it).
var Semantic = &messages.Version{
	Major: 0,
	Minor: 1,
	Patch: 0,
}

// CanUse returns true if the plugin can be used with the controller. It should only be used
// by the controller.
func CanUse(plugVersion *messages.Version) bool {
	if Semantic.Major == 0 {
		if plugVersion.Major != 0 {
			return false
		}
		if plugVersion.Minor > Semantic.Minor {
			return false
		}
		return true
	}
	if Semantic.Major < plugVersion.Major {
		return false
	}
	return true
}
