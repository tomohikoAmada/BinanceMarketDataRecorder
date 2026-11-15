//go:build !unix

package cache

import "context"

func (m *Manager) acquireFileLock(_ context.Context, _ string) (func(), error) {
	return func() {}, nil
}
