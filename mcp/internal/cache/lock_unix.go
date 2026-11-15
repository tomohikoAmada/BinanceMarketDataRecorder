//go:build unix

package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// acquireFileLock takes an exclusive cross-process flock for destPath under DataDir/.locks.
// The lock file inode is intentionally retained on unlock to avoid unlink/recreate races.
func (m *Manager) acquireFileLock(ctx context.Context, destPath string) (func(), error) {
	lockDir := filepath.Join(m.DataDir, ".locks")
	if err := os.MkdirAll(lockDir, 0o755); err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(destPath))
	lockPath := filepath.Join(lockDir, hex.EncodeToString(sum[:])+".lock")

	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() {
				_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
				_ = f.Close()
			}, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			_ = f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			return nil, ctx.Err()
		case <-time.After(15 * time.Millisecond):
		}
	}
}
