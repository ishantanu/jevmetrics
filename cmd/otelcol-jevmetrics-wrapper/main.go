package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "version", "--version", "-version", "-v":
			fmt.Printf("otelcol-jevmetrics %s\n", version)
			return
		}
	}

	exe, err := os.Executable()
	if err != nil {
		fatal(err)
	}
	coreName := "otelcol-jevmetrics-core"
	if runtime.GOOS == "windows" {
		coreName += ".exe"
	}
	core := filepath.Join(filepath.Dir(exe), coreName)

	if _, err := os.Stat(core); err != nil {
		fatal(fmt.Errorf("collector core not found at %s: %w", core, err))
	}

	cmd := exec.Command(core, os.Args[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			os.Exit(exitErr.ExitCode())
		}
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "otelcol-jevmetrics: %v\n", err)
	os.Exit(1)
}
