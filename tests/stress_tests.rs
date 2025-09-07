use rand::Rng;
use rand::rng;
use sharded_cachemap::DHShardedCacheMap;
use sharded_cachemap::EvictionPolicy;
use sharded_cachemap::PutResult;
use sharded_cachemap::ShardedCacheMap;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
async fn multitask_multiple_puts_and_gets_1() {
    const SHARDS: usize = 20;
    const SLOTS: usize = 10;
    const NUM_TASKS: usize = 10;
    let scm = Arc::new(ShardedCacheMap::new(
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
                for i in 0..1000 {
                    let key = format!("hi{i}");
                    scm_clone.put(key.clone(), i).await;
                    let _ = scm_clone.get(&key).await;
                }
            }
        });

        cache_vecs.push(cache_handler)
    }

    for handler in cache_vecs {
        handler.await.unwrap();
    }
    // scm.print_cache();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn multitask_multiple_puts_and_gets_2() {
    const SHARDS: usize = 1000;
    const SLOTS: usize = 4;
    const NUM_TASKS: usize = 10;
    const NUM_OPS: f64 = 5000000.0;
    let scm = Arc::new(ShardedCacheMap::new(
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
            let mut misses: f64 = 0.0;
            async move {
                for i in 0..NUM_OPS as usize {
                    let rand_key = rng().random_range(0..250000);
                    let res = scm_clone.put(rand_key, i).await;
                    match res {
                        PutResult::Update { key: _, val: _ } => {}
                        _ => {
                            misses += 1.0;
                        }
                    }
                    let rand_key = rng().random_range(0..250000);
                    let res = scm_clone.get(&rand_key).await;
                    if res.is_none() {
                        misses += 1.0;
                    }
                }
                misses
            }
        });

        cache_vecs.push(cache_handler)
    }

    let mut total_misses = 0.0;
    for handler in cache_vecs {
        total_misses += handler.await.unwrap();
        // assert_eq!(Some(0), res);
    }
    println!(
        "Miss Rate: {}",
        total_misses / (NUM_OPS * NUM_TASKS as f64 * 2.0)
    );
    // scm.print_cache();
}

#[tokio::test(flavor = "multi_thread")]
async fn multitask_random_puts_and_gets_2() {
    const SHARDS: usize = 10;
    const SLOTS: usize = 10;
    const NUM_TASKS: usize = 10;
    const NUM_OPS: f64 = 1.0;
    let scm = Arc::new(ShardedCacheMap::new(
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
            let mut misses: f64 = 0.0;
            async move {
                for i in 0..NUM_OPS as usize {
                    let rand_key = rng().random_range(0..250000);
                    if i % 3 == 0 {
                        let res = scm_clone.put(rand_key, i).await;
                        match res {
                            PutResult::Update { key: _, val: _ } => {}
                            _ => {
                                misses += 1.0;
                            }
                        }
                    }
                    let res = scm_clone.get(&rand_key).await;
                    if res.is_none() {
                        misses += 1.0;
                    }
                }
                misses
            }
        });

        cache_vecs.push(cache_handler)
    }

    // let mut total_misses = 0.0;
    for handler in cache_vecs {
        // total_misses += handler.await.unwrap();
        handler.await.unwrap();
        // assert_eq!(Some(0), res);
    }
    // println!(
    // "Miss Rate: {}",
    // total_misses / (NUM_OPS * NUM_TASKS as f64)
    // );
    // scm.print_cache();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn multitask_multiple_puts_and_gets_dh() {
    const SHARDS: usize = 200;
    const SLOTS: usize = 4;
    const NUM_TASKS: usize = 10;
    const NUM_OPS: f64 = 10000.0;
    let scm = Arc::new(DHShardedCacheMap::<i32, usize, ()>::new(
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
            let mut misses: f64 = 0.0;
            async move {
                for i in 0..NUM_OPS as usize {
                    let rand_key = rng().random_range(0..250000);
                    let res = scm_clone.put(rand_key, i).await;
                    match res {
                        PutResult::Update { key: _, val: _ } => {}
                        _ => {
                            misses += 1.0;
                        }
                    }
                    let rand_key = rng().random_range(0..250000);
                    let res = scm_clone.get(&rand_key).await;
                    if res.is_none() {
                        misses += 1.0;
                    }
                }
                misses
            }
        });

        cache_vecs.push(cache_handler)
    }

    let mut total_misses = 0.0;
    for handler in cache_vecs {
        total_misses += handler.await.unwrap();
        // assert_eq!(Some(0), res);
    }
    println!(
        "Miss Rate: {}",
        total_misses / (NUM_OPS * NUM_TASKS as f64 * 2.0)
    );
    // scm.print_cache();
}
