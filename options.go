package bigcat

const (
	// DefaultMaxFileSize is the default maximum size of an active data file (64MB)
	// before it is sealed and rotated to a new file.
	DefaultMaxFileSize int64 = 64 * 1024 * 1024
)

// Options configures the BigCat database instance.
type Options struct {
	// SyncOnWrite dictates whether every write (Put/Delete) is flushed to disk via fsync immediately.
	// Setting this to true provides stronger durability guarantees at the cost of write throughput.
	SyncOnWrite bool

	// DirPath is the filesystem directory where data files are stored.
	DirPath string

	// MaxFileSize is the maximum size (in bytes) an active data file can reach before rotating.
	MaxFileSize int64
}

// Option is a functional option pattern helper for configuring BigCat Options.
type Option func(*Options)

// DefaultOptions returns the default production configuration.
func DefaultOptions(dirPath string) Options {
	return Options{
		SyncOnWrite: true,
		DirPath:     dirPath,
		MaxFileSize: DefaultMaxFileSize,
	}
}

// WithSyncOnWrite sets whether BigCat flushes to disk after every write.
func WithSyncOnWrite(sync bool) Option {
	return func(o *Options) {
		o.SyncOnWrite = sync
	}
}

// WithMaxFileSize sets the maximum size in bytes before an active data file is rotated.
func WithMaxFileSize(size int64) Option {
	return func(o *Options) {
		if size > 0 {
			o.MaxFileSize = size
		}
	}
}
