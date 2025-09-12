use sharded_cachemap::DHShardedCacheMap;
use sharded_cachemap::EvictionPolicy;
use sharded_cachemap::PutResult;
use sharded_cachemap::ShardedCacheMap;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
async fn basic_put() {
    const SHARDS: usize = 20;
    const SLOTS: usize = 10;
    let scm = Arc::new(ShardedCacheMap::new(
        SHARDS,
        Some(SLOTS),
        EvictionPolicy::FIFO,
    ));

    // Since tasks handle their await points in sequence (context switching happens *between*
    // tasks at await points, not within the same task)
    // this put result should give me the previous
    let put_handler = tokio::spawn({
        let scm_clone = scm.clone();
        async move {
            scm_clone.put("hi", 0).await;
            scm_clone.put("hi", 1).await
        }
    });

    let res = put_handler.await.unwrap();
    scm.print_cache();
    match res {
        PutResult::Update { key, val } => {
            assert_eq!(key, "hi");
            assert_eq!(val, 0);
        }
        _ => {
            panic!("This shouldn't have happened")
        }
    }
    // assert_eq!(matches!(res, PutResult::Update { key: "hi", val: 1 }), true);
}

#[tokio::test(flavor = "multi_thread")]
async fn basic_get() {
    const SHARDS: usize = 20;
    const SLOTS: usize = 10;
    let scm = Arc::new(ShardedCacheMap::<&str, usize>::new(
        SHARDS,
        Some(SLOTS),
        EvictionPolicy::FIFO,
    ));

    // Since tasks handle their await points in sequence (context switching happens *between*
    // tasks at await points, not within the same task)
    // this put result should give me the previous
    let get_handler = tokio::spawn(
        // let scm_clone = scm.clone();
        async move { scm.get(&"hi").await.cloned() },
    );

    let res = get_handler.await.unwrap();
    // scm.print_cache();

    assert_eq!(None, res);
}

#[tokio::test(flavor = "multi_thread")]
async fn basic_put_and_get() {
    const SHARDS: usize = 20;
    const SLOTS: usize = 10;
    let scm = Arc::new(ShardedCacheMap::<&'static str, usize>::new(
        SHARDS,
        Some(SLOTS),
        EvictionPolicy::FIFO,
    ));

    // Since tasks handle their await points in sequence (context switching happens *between*
    // tasks at await points, not within the same task)
    // this put result should give me the previous
    let cache_handler = tokio::spawn(
        // let scm_clone = scm.clone();
        async move {
            scm.put("hi", 0).await;
            scm.get(&"hi").await.cloned()
        },
    );

    let res = cache_handler.await.unwrap();
    // scm.print_cache();

    assert_eq!(Some(0), res);
}

#[tokio::test(flavor = "multi_thread")]
async fn multitask_put_and_get() {
    const SHARDS: usize = 20;
    const SLOTS: usize = 10;
    const NUM_TASKS: usize = 10;
    let scm = Arc::new(ShardedCacheMap::<&'static str, usize>::new(
        SHARDS,
        Some(SLOTS),
        EvictionPolicy::FIFO,
    ));
    let mut cache_vecs = Vec::new();
    // Since tasks handle their await points in sequence (context switching happens *between*
    // tasks at await points, not within the same task)
    // this put result should give me the previous

    for _ in 0..NUM_TASKS {
        let cache_handler = tokio::spawn({
            let scm_clone = scm.clone();
            async move {
                scm_clone.put("hi", 0).await;
                scm_clone.get("hi").await.cloned()
            }
        });

        cache_vecs.push(cache_handler)
    }

    for handler in cache_vecs {
        let res = handler.await.unwrap();
        assert_eq!(Some(0), res);
    }
    // scm.print_cache();

    // assert_eq!(Some(0), res);
}

#[tokio::test(flavor = "multi_thread")]
async fn dh_multitask_put_and_get() {
    const SHARDS: usize = 20;
    const SLOTS: usize = 10;
    const NUM_TASKS: usize = 10;
    let scm = Arc::new(DHShardedCacheMap::<&'static str, usize, ()>::new(
        SHARDS,
        Some(SLOTS),
        None,
    ));
    let mut cache_vecs = Vec::new();
    // Since tasks handle their await points in sequence (context switching happens *between*
    // tasks at await points, not within the same task)
    // this put result should give me the previous

    for _ in 0..NUM_TASKS {
        let cache_handler = tokio::spawn({
            let scm_clone = scm.clone();
            async move {
                scm_clone.put("hi", 0).await;
                scm_clone.get("hi").await.cloned()
            }
        });

        cache_vecs.push(cache_handler)
    }

    for handler in cache_vecs {
        let res = handler.await.unwrap();
        assert_eq!(Some(0), res);
    }
    // scm.print_cache();

    // assert_eq!(Some(0), res);
}
