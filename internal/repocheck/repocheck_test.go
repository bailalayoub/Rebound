// Package repocheck_test checks repository files that no Go code reads: the compose files and the
// Docker build context. Paths are relative to this directory.
package repocheck_test

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const root = "../.."

// publishedPorts returns the entries of every `ports:` list in a compose file.
func publishedPorts(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(filepath.Join(root, path))
	require.NoError(t, err)
	defer func() { _ = f.Close() }() // read-only

	var ports []string
	portsIndent := -1
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		trimmed := strings.TrimSpace(line)
		indent := len(line) - len(strings.TrimLeft(line, " "))
		switch {
		case trimmed == "ports:":
			portsIndent = indent
		case portsIndent >= 0 && indent > portsIndent && strings.HasPrefix(trimmed, "- "):
			entry, _, _ := strings.Cut(strings.TrimPrefix(trimmed, "- "), "#")
			ports = append(ports, strings.Trim(strings.TrimSpace(entry), `"'`))
		case trimmed != "" && !strings.HasPrefix(trimmed, "#"):
			portsIndent = -1
		}
	}
	require.NoError(t, sc.Err())
	return ports
}

// Every port a compose file publishes is bound to 127.0.0.1. Without a host IP,
// Docker publishes on every interface, and these services have no authentication.
func TestComposePortsAreLocalOnly(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"docker-compose.yml", "deploy/docker-compose.yml", "playground/docker-compose.yml"} {
		ports := publishedPorts(t, path)
		require.NotEmpty(t, ports, "%s publishes no port: is the parser still right?", path)
		for _, p := range ports {
			assert.True(t, strings.HasPrefix(p, "127.0.0.1:"), "%s publishes %q on every interface", path, p)
		}
	}
}

// .env files, where the playground keeps the API key, never enter a Docker build
// context (deploy/Dockerfile copies the whole context into its build stage).
func TestDockerBuildContextExcludesEnvFiles(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(filepath.Join(root, ".dockerignore"))
	require.NoError(t, err)
	lines := strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")
	assert.Contains(t, lines, "**/.env")
	for _, l := range lines {
		assert.NotEqual(t, "!**/.env", l, ".env must not be re-included")
	}
}
