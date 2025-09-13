// TODO: Add Criterion benchmarking here to test how long operations take with this
// cache data structure
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::{Rng, rng};
use sharded_cachemap::{DHShardedCacheMap, DoubleHashPolicy, EvictionPolicy, SieveShardedCacheMap};
use sharded_cachemap::{PutResult, ShardedCacheMap};
use tinyufo::TinyUfo;
use std::sync::Arc;
use tokio::spawn;

async fn bench_cache(
    shards: usize,
    slots: usize,
    evict_policy: EvictionPolicy,
    task_count: usize,
    iter_per_task: usize,
    data: &Vec<(String, usize)>,
) {
    let scm = ShardedCacheMap::new_with_slots(
        shards,
        slots,
        evict_policy,
    );
    let mut cache_vecs = Vec::new();
    // Since tasks handle their await points in sequence (context switching happens *between*
    // tasks at await points, not within the same task)
    // this put result should give me the previous

    for _ in 0..task_count {
        let cache_handler = spawn({
            let scm_clone = scm.clone();
            let mut misses: f64 = 0.0;
            let data = data.clone();
            async move {
                for _ in 0..iter_per_task {
                    let rand_key = rng().random_range(0..iter_per_task) as usize;

                    let get_or_put = rng().random_bool(0.50);

                    if get_or_put {
                        let res = scm_clone
                            .put(data[rand_key].0.clone(), data[rand_key].1)
                            .await;
                        match res {
                            PutResult::Update { key: _, val: _ } => {}
                            _ => {
                                misses += 1.0;
                            }
                        }
                    } else {
                        let res = scm_clone.get(&data[rand_key].0).await;
                        if res.is_none() {
                            misses += 1.0;
                        }
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
    }
    // println!(
    //     "{:?} Policy on Artificial Data Miss Rate: {}",
    //     evict_policy,
    //     total_misses / (task_count as f64 * iter_per_task as f64)
    // );
}

async fn bench_dh_cache(
    shards: usize,
    slots: usize,
    evict_policy: Option<DoubleHashPolicy<()>>,
    task_count: usize,
    iter_per_task: usize,
    data: &Vec<(String, usize)>,
) {
    let scm = Arc::new(DHShardedCacheMap::<String, usize, ()>::new(
        shards,
        Some(slots),
        evict_policy,
    ));
    let mut cache_vecs = Vec::new();
    // Since tasks handle their await points in sequence (context switching happens *between*
    // tasks at await points, not within the same task)
    // this put result should give me the previous

    for _ in 0..task_count {
        let cache_handler = spawn({
            let scm_clone = scm.clone();
            let mut misses: f64 = 0.0;
            let data = data.clone();
            async move {
                for _ in 0..iter_per_task {
                    let rand_key = rng().random_range(0..iter_per_task) as usize;

                    let get_or_put = rng().random_bool(0.50);

                    if get_or_put {
                        let res = scm_clone
                            .put(data[rand_key].0.clone(), data[rand_key].1)
                            .await;
                        match res {
                            PutResult::Update { key: _, val: _ } => {}
                            _ => {
                                misses += 1.0;
                            }
                        }
                    } else {
                        let res = scm_clone.get(&data[rand_key].0).await;
                        if res.is_none() {
                            misses += 1.0;
                        }
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
    }

    // println!(
    //     "Double Hash Policy on Artificial Data Miss Rate: {}",
    //     total_misses / (task_count as f64 * iter_per_task as f64)
    // );
}

async fn bench_sieve_cache(
    shards: usize,
    slots: usize,
    task_count: usize,
    iter_per_task: usize,
    data: &Vec<(String, usize)>,
) {
    let scm = Arc::new(SieveShardedCacheMap::new(shards, Some(slots)));
    let mut cache_vecs = Vec::new();
    // Since tasks handle their await points in sequence (context switching happens *between*
    // tasks at await points, not within the same task)
    // this put result should give me the previous

    for _ in 0..task_count {
        let cache_handler = spawn({
            let scm_clone = scm.clone();
            let mut misses: f64 = 0.0;
            let data = data.clone();
            async move {
                for _ in 0..iter_per_task {
                    let rand_key = rng().random_range(0..iter_per_task) as usize;

                    let get_or_put = rng().random_bool(0.50);

                    if get_or_put {
                        let res = scm_clone
                            .put(data[rand_key].0.clone(), data[rand_key].1)
                            .await;
                        match res {
                            PutResult::Update { key: _, val: _ } => {}
                            _ => {
                                misses += 1.0;
                            }
                        }
                    } else {
                        let res = scm_clone.get(&data[rand_key].0).await;
                        if res.is_none() {
                            misses += 1.0;
                        }
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
    }
    // println!(
    //     "Sieve Policy on Artificial Data Miss Rate: {}",
    //     total_misses / (task_count as f64 * iter_per_task as f64)
    // );
}

async fn bench_tinyufo_cache(
    shards: usize,
    slots: usize,
    task_count: usize,
    iter_per_task: usize,
    data: &Vec<(String, usize)>,
) {
    let scm = Arc::new(TinyUfo::new(5*10*10, shards * slots));
    let mut cache_vecs = Vec::new();
    // Since tasks handle their await points in sequence (context switching happens *between*
    // tasks at await points, not within the same task)
    // this put result should give me the previous

    for _ in 0..task_count {
        let cache_handler = spawn({
            let scm_clone = scm.clone();
            let mut misses: f64 = 0.0;
            let data = data.clone();
            async move {
                for _ in 0..iter_per_task {
                    let rand_key = rng().random_range(0..iter_per_task) as usize;

                    let get_or_put = rng().random_bool(0.50);

                    if get_or_put {
                        let res = scm_clone
                            .put(data[rand_key].0.clone(), data[rand_key].1, 1);
                        // match res {
                        //     PutResult::Update { key: _, val: _ } => {}
                        //     _ => {
                        //         misses += 1.0;
                        //     }
                        // }
                    } else {
                        let res = scm_clone.get(&data[rand_key].0);
                        if res.is_none() {
                            misses += 1.0;
                        }
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
    }
    println!(
        "Sieve Policy on Artificial Data Miss Rate: {}",
        total_misses / (task_count as f64 * iter_per_task as f64)
    );
}


fn benchmark_scm(c: &mut Criterion) {
    const SHARDS: usize = 1000;
    const SLOTS: usize = 10 * 5;
    const THREAD_NUM: usize = 8;
    const TASK_COUNT: usize = 100;
    const ITER_PER_TASK: usize = 100000;
    let evict_fifo = EvictionPolicy::FIFO;
    let evict_lifo = EvictionPolicy::LIFO;
    let mut artificial_data = vec![];
    let mut random_artificial_data = vec![];
    for i in 0..ITER_PER_TASK {
        artificial_data.push((format!("hi{i}"), i));
    }

    for i in 0..ITER_PER_TASK {
        let rand_key = rng().random_range(0..1000);
        random_artificial_data.push((rand_key, i));
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(THREAD_NUM)
        .build()
        .unwrap();

    // c.bench_with_input(
    //     BenchmarkId::new("FIFO Cache with Artificial Data", SHARDS),
    //     &(SHARDS),
    //     |b, &_| {
    //         // Insert a call to `to_async` to convert the bencher to async mode.
    //         // The timing loops are the same as with the normal bencher.
    //         b.to_async(&runtime).iter(async || {
    //             bench_cache(
    //                 SHARDS,
    //                 SLOTS,
    //                 evict_fifo,
    //                 TASK_COUNT,
    //                 ITER_PER_TASK,
    //                 &artificial_data,
    //             )
    //             .await;
    //         });
    //     },
    // );

    c.bench_with_input(
        BenchmarkId::new("TinyLFU Cache with Artificial Data", SHARDS),
        &(SHARDS),
        |b, &_| {
            // Insert a call to `to_async` to convert the bencher to async mode.
            // The timing loops are the same as with the normal bencher.
            b.to_async(&runtime).iter(async || {
                bench_tinyufo_cache(
                    SHARDS,
                    SLOTS,
                    TASK_COUNT,
                    ITER_PER_TASK,
                    &artificial_data,
                )
                .await;
            });
        },
    );

    // c.bench_with_input(
    //     BenchmarkId::new("LIFO Cache with Artificial Data", SHARDS),
    //     &(SHARDS),
    //     |b, &_| {
    //         // Insert a call to `to_async` to convert the bencher to async mode.
    //         // The timing loops are the same as with the normal bencher.
    //         b.to_async(&runtime).iter(async || {
    //             bench_cache(
    //                 SHARDS,
    //                 SLOTS,
    //                 evict_lifo,
    //                 TASK_COUNT,
    //                 ITER_PER_TASK,
    //                 &artificial_data,
    //             )
    //             .await;
    //         });
    //     },
    // );

    // c.bench_with_input(
    //     BenchmarkId::new("Sieve Cache with Artificial Data", SHARDS),
    //     &(SHARDS),
    //     |b, &_| {
    //         // Insert a call to `to_async` to convert the bencher to async mode.
    //         // The timing loops are the same as with the normal bencher.
    //         b.to_async(&runtime).iter(async || {
    //             bench_sieve_cache(
    //                 SHARDS,
    //                 SLOTS,
    //                 TASK_COUNT,
    //                 ITER_PER_TASK,
    //                 &artificial_data,
    //             )
    //             .await;
    //         });
    //     },
    // );

    // c.bench_with_input(
    //     BenchmarkId::new("Double Hash Cache with Artificial Data", SHARDS),
    //     &(SHARDS),
    //     |b, &_| {
    //         // Insert a call to `to_async` to convert the bencher to async mode.
    //         // The timing loops are the same as with the normal bencher.
    //         b.to_async(&runtime).iter(async || {
    //             bench_dh_cache(
    //                 SHARDS,
    //                 SLOTS,
    //                 None,
    //                 TASK_COUNT,
    //                 ITER_PER_TASK,
    //                 &artificial_data,
    //             )
    //             .await;
    //         });
    //     },
    // );
}

criterion_group!(benches, benchmark_scm);
criterion_main!(benches);
