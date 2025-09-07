pub enum PutResult<K, V> {
    Eviction { key: K, val: V },
    Update { key: K, val: V },
    Insert,
}
