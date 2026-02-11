package db

type IndexEntry struct {
	Offset int64
	Length int
}

type Index struct {
	m map[string]IndexEntry
}

func NewIndex() *Index {
	return &Index{m: make(map[string]IndexEntry)}
}

func (idx *Index) Set(key string, offset int64, length int) {
	idx.m[key] = IndexEntry{Offset: offset, Length: length}
}

func (idx *Index) Get(key string) (IndexEntry, bool) {
	e, ok := idx.m[key]
	return e, ok
}

func (idx *Index) Delete(key string) {
	delete(idx.m, key)
}
