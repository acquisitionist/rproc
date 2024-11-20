package env

import (
	"os"
	"strconv"
	"time"
)

// Getter provides methods to get environment variables with defaults
type Getter struct {
	prefix string
}

// New creates a new environment variable getter with an optional prefix
func New(prefix string) *Getter {
	return &Getter{prefix: prefix}
}

// prefixKey adds the prefix to the key if one is set
func (g *Getter) prefixKey(key string) string {
	if g.prefix == "" {
		return key
	}
	return g.prefix + "_" + key
}

func (g *Getter) GetDuration(key string, defaultValue time.Duration) (time.Duration, error) {
	value, exists := os.LookupEnv(g.prefixKey(key))
	if !exists {
		return defaultValue, nil
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		panic(err)
	}

	return duration, nil
}

func (g *Getter) GetString(key, defaultValue string) string {
	value, exists := os.LookupEnv(g.prefixKey(key))
	if !exists {
		return defaultValue
	}

	return value
}

func (g *Getter) GetInt(key string, defaultValue int) int {
	value, exists := os.LookupEnv(g.prefixKey(key))
	if !exists {
		return defaultValue
	}

	intValue, err := strconv.Atoi(value)
	if err != nil {
		panic(err)
	}

	return intValue
}

func (g *Getter) GetBool(key string, defaultValue bool) bool {
	value, exists := os.LookupEnv(g.prefixKey(key))
	if !exists {
		return defaultValue
	}

	boolValue, err := strconv.ParseBool(value)
	if err != nil {
		panic(err)
	}

	return boolValue
}
