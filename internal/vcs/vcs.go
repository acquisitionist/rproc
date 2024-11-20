package vcs

import (
	"fmt"
	"runtime/debug"
)

type Info struct {
	Version   string
	Revision  string
	Modified  bool
	GoVersion string
}

// Get returns the VCS information using runtime build info
func Get() Info {
	info := Info{
		Version:   "dev",
		Revision:  "unavailable",
		GoVersion: "unknown",
	}

	bi, ok := debug.ReadBuildInfo()
	if ok {
		info.GoVersion = bi.GoVersion
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				info.Revision = s.Value
			case "vcs.modified":
				info.Modified = s.Value == "true"
			case "vcs.time":
				// Available but not used currently
			}
		}
	}

	return info
}

// String returns a formatted version string
func (i Info) String() string {
	if i.Revision == "unavailable" {
		return i.Revision
	}

	if i.Modified {
		return fmt.Sprintf("%s-dirty", i.Revision)
	}

	return i.Revision
}
