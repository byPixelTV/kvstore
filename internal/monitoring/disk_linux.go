//go:build linux

package monitoring

import "golang.org/x/sys/unix"

func DiskBytes(path string) (available, total uint64, err error) {
	var stats unix.Statfs_t
	if err := unix.Statfs(path, &stats); err != nil {
		return 0, 0, err
	}
	return stats.Bavail * uint64(stats.Bsize), stats.Blocks * uint64(stats.Bsize), nil
}
