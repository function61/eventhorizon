package cachegen

import (
	"sync"

	"github.com/cheekybits/genny/generic"
)

type CacheItemType generic.Type

type Cache struct {
	items   map[string]CacheItemType
	itemsMu sync.Mutex
}

func NewCache() *Cache {
	return &Cache{
		items: map[string]CacheItemType{},
	}
}

func (c *Cache) Get(
	cacheKey string,
	factory func() CacheItemType,
) CacheItemType {
	if c == nil {
		return factory()
	}

	c.itemsMu.Lock()
	defer c.itemsMu.Unlock()

	item := c.items[cacheKey]

	if item == nil {
		item = factory()

		c.items[cacheKey] = item
	}

	return item
}
