//go:build !linux

package monitoring

import "errors"

func DiskBytes(string) (available, total uint64, err error) {
	return 0, 0, errors.New("filesystem capacity metrics are available on Linux")
}
