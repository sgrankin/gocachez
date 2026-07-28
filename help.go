package main

import (
	"fmt"
	"io"
)

func helpMode(args []string) (runMode, bool) {
	for _, arg := range args {
		if arg != "-h" && arg != "--help" && arg != "-help" {
			continue
		}
		for _, candidate := range args {
			if isRunMode(candidate) {
				return runMode(candidate), true
			}
		}
		return runModeProtocol, true
	}
	return runModeProtocol, false
}

func writeHelp(w io.Writer, mode runMode) error {
	var text string
	switch mode {
	case runModeClean:
		text = cleanHelp
	case runModePrune:
		text = pruneHelp
	case runModeStatus:
		text = statusHelp
	default:
		text = rootHelp
	}
	if _, err := fmt.Fprint(w, text); err != nil {
		return fmt.Errorf("write help: %w", err)
	}
	return nil
}

const rootHelp = `Usage:
  gocachez [flags]
  gocachez <command> [flags]

Commands:
  clean    Remove cache data not currently in use
  prune    Run cache maintenance now
  status   Show cache state

Flags:
  -config path            JSON config file
  -dir path               cache directory
  -max-size size          maximum compressed blob size, or 0 to disable pruning
  -max-age dur            maximum age of unused entries, or 0 to disable
                          age-based pruning
  -max-retained-age dur   maximum age of unused retained go-list files; 0 or
                          unset follows -max-age
  -v                      log cache maintenance to stderr
  -h                      show help
`

const cleanHelp = `Usage:
  gocachez clean [flags]

Remove all cache data not currently in use. Data used by active gocachez
processes is preserved; run clean again after those processes exit to remove it.

Flags:
  -config path  JSON config file
  -dir path     cache directory
  -h            show help
`

const pruneHelp = `Usage:
  gocachez prune [flags]

Run cache maintenance now instead of waiting for the interval to come round.
Maintenance otherwise happens as a build's helper exits, which the go command
waits for; running it as its own step keeps that cost off a build.

Removing blobs and catalog entries needs a cache no build is using, so if any
are, prune reclaims unused live run directories and reports what it skipped.

Flags:
  -config path            JSON config file
  -dir path               cache directory
  -max-size size          maximum compressed blob size, or 0 to disable pruning
  -max-age dur            maximum age of unused entries, or 0 to disable
                          age-based pruning
  -max-retained-age dur   maximum age of unused retained go-list files; 0 or
                          unset follows -max-age
  -v                      log cache maintenance to stderr
  -h                      show help
`

const statusHelp = `Usage:
  gocachez status [flags]

Show cache state without modifying it.

Flags:
  -config path  JSON config file
  -dir path     cache directory
  -h            show help
`
