// Package version defines the version of the plugin.
package version

import (
	"github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/messages"
	msgproto "github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/messages/proto"
)

// Semantic is the version of the plugin or the controller (depending on which is using it).
var Semantic = &msgproto.Version{
	Major: 0,
	Minor: 1,
	Patch: 0,
}

var Semantic2 = messages.Version{
	Major: 0,
	Minor: 1,
	Patch: 0,
}

// CanUse returns true if the plugin can be used with the controller. It should only be used
// by the controller.
func CanUse(plugVersion *msgproto.Version) bool {
	if plugVersion.Major != Semantic.Major {
		return false
	}
	if plugVersion.Minor != Semantic.Minor {
		return false
	}
	if plugVersion.Patch != Semantic.Patch {
		return false
	}
	return true
}
