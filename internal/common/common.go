package common

import "github.com/spf13/afero"

// Context represents user defined configuration and derived reference configuration
type Context struct {
	Config Config
	// Groups    []AssetGroup
	Reference Reference
}

// Config represents user defined configuration
type Config struct {

	// Input configuration
	InputDir string `yaml:"input_dir"`
	// Output configuration
	OutputDir string `yaml:"output_dir"`

	// Processing configuration
	Field     string   `yaml:"field"`
	Value     string   `yaml:"value"`
	ValueList string   `yaml:"value_list"`
	Values    []string `yaml:"values"` // Loaded from ValueList if specified
	Threads   int      `yaml:"threads"`

	// Filter configuration
	FileFilter        string `yaml:"file_filter"`
	SplitIntermediate bool   `yaml:"split_intermediate"`
	ErrorRate         int    `yaml:"error_rate"`
	PartialMatch      bool   `yaml:"partial_match"`
	RegexMatch        bool   `yaml:"regex_match"`

	// Debug configuration
	Debug bool `yaml:"debug"`

	// State tracking
	StateFile string `yaml:"state_file"`
}

// Reference represents derived configuration for internal use from user defined configuration
type Reference struct {
}

// Dependencies represents references to external dependencies
type Dependencies struct {
	Fs afero.Fs
}
