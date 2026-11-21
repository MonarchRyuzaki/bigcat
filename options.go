package bigcat

// Options configures the BigCat database instance.
type Options struct {
	// SyncOnWrite dictates whether every write (Put/Delete) is flushed to disk via fsync immediately.
	// Setting this to true provides stronger durability guarantees at the cost of write throughput.
	SyncOnWrite bool

	// DirPath is the filesystem directory where data files are stored.
	DirPath string
}

// Option is a functional option pattern helper for configuring BigCat Options.
type Option func(*Options)

// DefaultOptions returns the default production configuration.
func DefaultOptions(dirPath string) Options {
	return Options{
		SyncOnWrite: false,
		DirPath:     dirPath,
	}
}

// WithSyncOnWrite sets whether BigCat flushes to disk after every write.
func WithSyncOnWrite(sync bool) Option {
	return func(o *Options) {
		o.SyncOnWrite = sync
	}
}
