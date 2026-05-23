package cache

import "context"

type BinlogCacheBackend interface {
	Write(ctx context.Context, tableID string, event *CacheEvent) error
	Read(ctx context.Context, tableID string, fromGTID string, fromEventSeq int64) (<-chan *CacheEvent, error)
	Delete(ctx context.Context, tableID string) error
	Size(ctx context.Context, tableID string) (int64, error)
	TotalSize(ctx context.Context) (int64, error)
	Exists(ctx context.Context, tableID string) (bool, error)
	Close() error
}
