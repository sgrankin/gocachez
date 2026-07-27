package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// defaultMaxAge is the default cutoff for age-based pruning, matching cmd/go's
// GOCACHE trim window.
const defaultMaxAge = 5 * 24 * time.Hour

type config struct {
	dir     string
	maxSize int64
	maxAge  time.Duration
	// maxRetainedAge expires retained go-list files, which exist only so paths
	// that escaped a finished build stay openable. Unlike a blob, losing one
	// costs nothing but a re-strip on the next use, so it is usually worth
	// expiring them far sooner than the cache proper. Read it through
	// retainedAge, not directly.
	maxRetainedAge time.Duration
	verbose        bool
	cpuProfile     string
	memProfile     string
}

// retainedAge reports when retained files expire: maxRetainedAge if it was set,
// otherwise maxAge, which is how gocachez behaved before the setting existed.
//
// Zero means "follow maxAge" here rather than "disabled" as it does for maxAge.
// The zero value of a config has to be the safe one — silently switching
// retained pruning off would leave escaped-path files accumulating until their
// cache entry went — and there is no use for keeping them longer than the
// entries that reference them anyway.
func (c config) retainedAge() time.Duration {
	if c.maxRetainedAge > 0 {
		return c.maxRetainedAge
	}
	return c.maxAge
}

type fileConfig struct {
	CacheDir       *string `json:"cacheDir"`
	MaxSize        *string `json:"maxSize"`
	MaxAge         *string `json:"maxAge"`
	MaxRetainedAge *string `json:"maxRetainedAge"`
	Verbose        *bool   `json:"verbose"`
}

func parseFlags(args []string) (config, error) {
	cfg, operands, err := parseFlagOperands(args)
	if err != nil {
		return config{}, err
	}
	if len(operands) != 0 {
		return config{}, &argError{fmt.Errorf("unexpected argument %q", operands[0])}
	}
	return cfg, nil
}

// argError marks a command-line parsing failure (an unknown flag or argument)
// as distinct from a configuration or value error, so callers can respond by
// printing usage.
type argError struct{ err error }

func (e *argError) Error() string { return e.err.Error() }
func (e *argError) Unwrap() error { return e.err }

func parseFlagOperands(args []string) (config, []string, error) {
	cfg, err := defaultConfig()
	if err != nil {
		return config{}, nil, err
	}
	configPath, configRequired := defaultConfigPath()

	var flagDir, flagMaxSize, flagMaxAge, flagMaxRetainedAge string
	var flagVerbose bool

	fs := flag.NewFlagSet("gocachez", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&configPath, "config", configPath, "JSON config file")
	fs.StringVar(&flagDir, "dir", "", "cache directory")
	fs.StringVar(&flagMaxSize, "max-size", "", "maximum compressed cache size, or 0 to disable pruning")
	fs.StringVar(&flagMaxAge, "max-age", "", "maximum age of unused entries, or 0 to disable age-based pruning")
	fs.StringVar(&flagMaxRetainedAge, "max-retained-age", "", "maximum age of unused retained go-list files; 0 or unset follows -max-age")
	fs.BoolVar(&flagVerbose, "v", false, "log cache maintenance to stderr")
	fs.StringVar(&cfg.cpuProfile, "cpuprofile", "", "write CPU profile to file")
	fs.StringVar(&cfg.memProfile, "memprofile", "", "write memory profile to file")
	if err := fs.Parse(args); err != nil {
		return config{}, nil, &argError{err}
	}

	visited := map[string]bool{}
	fs.Visit(func(f *flag.Flag) {
		visited[f.Name] = true
	})
	if visited["config"] {
		configRequired = true
	}

	if configPath != "" {
		if err := applyConfigFile(&cfg, configPath, configRequired); err != nil {
			return config{}, nil, err
		}
	}
	if value := os.Getenv("GOCACHEZ_DIR"); value != "" {
		cfg.dir = value
	}
	if value := os.Getenv("GOCACHEZ_MAX_SIZE"); value != "" {
		if cfg.maxSize, err = parseSize(value); err != nil {
			return config{}, nil, fmt.Errorf("parse GOCACHEZ_MAX_SIZE: %w", err)
		}
	}
	if value := os.Getenv("GOCACHEZ_MAX_AGE"); value != "" {
		if cfg.maxAge, err = parseAge(value); err != nil {
			return config{}, nil, fmt.Errorf("parse GOCACHEZ_MAX_AGE: %w", err)
		}
	}
	if value := os.Getenv("GOCACHEZ_MAX_RETAINED_AGE"); value != "" {
		if cfg.maxRetainedAge, err = parseAge(value); err != nil {
			return config{}, nil, fmt.Errorf("parse GOCACHEZ_MAX_RETAINED_AGE: %w", err)
		}
	}
	if value := os.Getenv("GOCACHEZ_VERBOSE"); value != "" {
		if cfg.verbose, err = strconv.ParseBool(value); err != nil {
			return config{}, nil, fmt.Errorf("parse GOCACHEZ_VERBOSE: %w", err)
		}
	}

	if visited["dir"] {
		cfg.dir = flagDir
	}
	if visited["max-size"] {
		if cfg.maxSize, err = parseSize(flagMaxSize); err != nil {
			return config{}, nil, fmt.Errorf("parse -max-size: %w", err)
		}
	}
	if visited["max-age"] {
		if cfg.maxAge, err = parseAge(flagMaxAge); err != nil {
			return config{}, nil, fmt.Errorf("parse -max-age: %w", err)
		}
	}
	if visited["max-retained-age"] {
		if cfg.maxRetainedAge, err = parseAge(flagMaxRetainedAge); err != nil {
			return config{}, nil, fmt.Errorf("parse -max-retained-age: %w", err)
		}
	}
	if visited["v"] {
		cfg.verbose = flagVerbose
	}

	abs, err := filepath.Abs(cfg.dir)
	if err != nil {
		return config{}, nil, fmt.Errorf("resolve cache dir: %w", err)
	}
	cfg.dir = abs
	return cfg, fs.Args(), nil
}

func defaultConfig() (config, error) {
	userCacheDir, err := os.UserCacheDir()
	if err != nil {
		return config{}, fmt.Errorf("find user cache dir: %w", err)
	}
	maxSize, err := parseSize("20GiB")
	if err != nil {
		return config{}, err
	}
	return config{
		dir:     filepath.Join(userCacheDir, "gocachez"),
		maxSize: maxSize,
		maxAge:  defaultMaxAge,
	}, nil
}

func defaultConfigPath() (string, bool) {
	if value := os.Getenv("GOCACHEZ_CONFIG"); value != "" {
		return value, true
	}
	userConfigDir, err := os.UserConfigDir()
	if err != nil {
		// A default config file is optional; absence of a config directory should not disable startup.
		return "", false
	}
	return filepath.Join(userConfigDir, "gocachez", "config.json"), false
}

func applyConfigFile(cfg *config, path string, required bool) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) && !required {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read config %s: %w", path, err)
	}
	var fc fileConfig
	if err := json.Unmarshal(data, &fc); err != nil {
		return fmt.Errorf("decode config %s: %w", path, err)
	}
	return applyFileConfig(cfg, fc)
}

func applyFileConfig(cfg *config, fc fileConfig) error {
	if fc.CacheDir != nil {
		cfg.dir = *fc.CacheDir
	}
	if fc.MaxSize != nil {
		maxSize, err := parseSize(*fc.MaxSize)
		if err != nil {
			return fmt.Errorf("parse config maxSize: %w", err)
		}
		cfg.maxSize = maxSize
	}
	if fc.MaxAge != nil {
		maxAge, err := parseAge(*fc.MaxAge)
		if err != nil {
			return fmt.Errorf("parse config maxAge: %w", err)
		}
		cfg.maxAge = maxAge
	}
	if fc.MaxRetainedAge != nil {
		maxRetainedAge, err := parseAge(*fc.MaxRetainedAge)
		if err != nil {
			return fmt.Errorf("parse config maxRetainedAge: %w", err)
		}
		cfg.maxRetainedAge = maxRetainedAge
	}
	if fc.Verbose != nil {
		cfg.verbose = *fc.Verbose
	}
	return nil
}
