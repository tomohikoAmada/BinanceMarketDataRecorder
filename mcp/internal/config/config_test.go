package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFindVenvPythonAndEnvOverride(t *testing.T) {
	repo := t.TempDir()
	winPy := filepath.Join(repo, ".venv", "Scripts", "python.exe")
	unixPy := filepath.Join(repo, ".venv", "bin", "python")
	if err := os.MkdirAll(filepath.Dir(winPy), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(unixPy), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(winPy, []byte("exe"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(unixPy, []byte("bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got := findVenvPython(repo, "windows"); got != winPy {
		t.Fatalf("expected windows venv python %q, got %q", winPy, got)
	}
	if got := findVenvPython(repo, "darwin"); got != unixPy {
		t.Fatalf("expected unix venv python %q, got %q", unixPy, got)
	}

	t.Setenv("BINANCE_MCP_DATA_DIR", filepath.Join(t.TempDir(), "data"))
	t.Setenv("SBMDR_REPO_DIR", repo)
	t.Setenv("SBMDR_PYTHON_BIN", "/custom/python")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load with explicit override: %v", err)
	}
	if cfg.PythonBin != "/custom/python" {
		t.Fatalf("expected explicit SBMDR_PYTHON_BIN override, got %q", cfg.PythonBin)
	}
}
