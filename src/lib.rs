//! dogpile-cache provides a cache which holds values that expire on a timer
//! and will make an effort to premtively refresh the value before it is
//! expired and do so in a way that requires little waiting.
//!
//! Example
//! ```rust
//! use std::time::{Duration, Instant};
//! use dogpile_cache_rs::{DogpileCache, CacheData};
//!
//! #[tokio::main]
//! async fn main() {
//!     async fn num(v: i32) -> Result<CacheData<i32>, ()> {
//!         let valid_length = Duration::from_millis(20);
//!         Ok(CacheData::new(
//!             v,
//!             Instant::now() + valid_length,
//!             Instant::now() + valid_length/2,
//!         ))
//!     }
//!     let c = DogpileCache::<i32>::create(num, 1).await;
//!     assert_eq!(c.read().await.value, 1);
//! }
//! ```

use log::{debug, warn};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::sync::{Notify, RwLock, RwLockReadGuard};
use tokio::time;

/// A simple cache that holds some data that expires and has a task to attempt to refresh
/// the value before it expires. This struct uses `Arc` internally to store its data so clones of
/// a cache will point to the same data.
pub struct DogpileCache<T> {
    cache_data: Arc<RwLock<CacheData<T>>>,
    refreshed: Arc<Notify>,
}

/// The value the DogpileCache will store and how often it expires and should be refreshed.
pub struct CacheData<T> {
    pub value: T,
    pub expire_time: Instant,
    pub refresh_time: Instant,
}

struct CacheRefresher<T, A, F> {
    cache: DogpileCache<T>,
    refresh_fn: fn(A) -> F,
    refresh_arg: A,
    backoff: Duration,
}

impl<T> Clone for DogpileCache<T> {
    fn clone(&self) -> Self {
        Self {
            cache_data: self.cache_data.clone(),
            refreshed: self.refreshed.clone(),
        }
    }
}

#[allow(dead_code)]
impl<T: Default + Send + Sync + 'static> DogpileCache<T> {
    /// Create the dogpile cache, this starts a task to eagerly refresh the value at intervals
    /// specified by the refresh_fn return data. We require T to impl `Default` so we can generate
    /// an initial value before the cache is primed, if your type does not `impl Default` you
    /// should wrap it in an `Option`. `refresh_arg` will be cloned and passed to
    /// `refresh_fn`, if your refresh_arg is expensive to clone you should use an `Arc<V>` and if you
    /// need mutability you should use `Arc<RwLock<V>>` or `Arc<Mutex<V>>`.
    pub async fn create<
        A: Clone + Send + Sync + 'static,
        F: Future<Output = Result<CacheData<T>, ()>> + Send + 'static,
    >(
        refresh_fn: fn(A) -> F,
        refresh_arg: A,
    ) -> Self {
        let mut expire_time = Instant::now();
        let mut refresh_time = Instant::now();
        let cache_data = Arc::new(RwLock::new(CacheData {
            value: T::default(),
            expire_time,
            refresh_time,
        }));
        let refreshed = Arc::new(Notify::new());
        let cache = Self {
            cache_data,
            refreshed,
        };
        let c = cache.clone();
        tokio::spawn(async move {
            let mut refresher = CacheRefresher::create(c, refresh_fn, refresh_arg);
            loop {
                select! {
                    _ = time::sleep_until(time::Instant::from_std(refresh_time)) => {
                        debug!("Starting refresh");
                        let times = refresher.refresh().await;
                        debug!("Refresh finished");
                        expire_time = times.0;
                        refresh_time = times.1;
                    }
                    _ = time::sleep_until(time::Instant::from_std(expire_time)) => {
                        debug!("Starting refresh");
                        let times = refresher.refresh().await;
                        debug!("Refresh finished");
                        expire_time = times.0;
                        refresh_time = times.1;
                    }
                }
            }
        });
        cache
    }

    /// Checks the cache value to see if it's expired, if not it returns a read lock to the data,
    /// if it is expired the task waits until the value is refreshed. Note that this returns a
    /// `RwLockReadGuard`, refreshing cannot happen until these are freed, therefore if you must
    /// hold onto the data for a significant amount of time you should clone the data and drop the
    /// lock.
    pub async fn read(&self) -> RwLockReadGuard<'_, CacheData<T>> {
        // Register a notification, this has to be done before grabbing the read lock
        let n = self.refreshed.notified();
        if self.cache_data.read().await.expire_time <= Instant::now() {
            // Wait to be notified of the updated cache value
            debug!("Value is expired, waiting for refresh");
            n.await;
            debug!("reader woken");
        }
        self.cache_data.read().await
    }
}

impl<T> CacheData<T> {
    pub fn new(value: T, expire_time: Instant, refresh_time: Instant) -> Self {
        Self {
            value,
            expire_time,
            refresh_time,
        }
    }
}

impl<
        T,
        A: Clone + Send + Sync + 'static,
        F: Future<Output = Result<CacheData<T>, ()>> + Send + 'static,
    > CacheRefresher<T, A, F>
{
    fn create(cache: DogpileCache<T>, refresh_fn: fn(A) -> F, refresh_arg: A) -> Self {
        Self {
            cache,
            refresh_fn,
            refresh_arg,
            backoff: Duration::from_millis(10),
        }
    }
    async fn refresh(&mut self) -> (Instant, Instant) {
        // We need to hold the refresh lock so only one task will attempt to generate the new value
        if let Ok(CacheData {
            value: new_value,
            expire_time: new_expire_time,
            refresh_time: new_refresh_time,
        }) = (self.refresh_fn)(self.refresh_arg.clone()).await
        {
            debug!("Acquiring writer lock");
            self.backoff = Duration::from_millis(10);
            let mut cd_writer = self.cache.cache_data.write().await;
            cd_writer.value = new_value;
            cd_writer.expire_time = new_expire_time;
            cd_writer.refresh_time = new_refresh_time;
            debug!("notifying waiters");
            self.cache.refreshed.notify_waiters();
            return (new_expire_time, new_refresh_time);
        }
        warn!("Refresh fn failed");
        let ret = (Instant::now() + self.backoff, Instant::now() + self.backoff);
        self.backoff *= 2;
        ret
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::time::sleep;
    #[tokio::test]
    async fn test_cache_basics() {
        env_logger::init();
        let sleep_length = Duration::from_millis(11);
        async fn num(v: i32) -> Result<CacheData<i32>, ()> {
            let valid_length = Duration::from_millis(20);
            Ok(CacheData::new(
                v,
                Instant::now() + valid_length,
                Instant::now() + valid_length / 2,
            ))
        }
        let c1 = DogpileCache::<i32>::create(num, 1).await;
        let c2 = DogpileCache::<i32>::create(num, 1).await;
        assert_eq!(c1.read().await.value, 1);
        c1.cache_data.write().await.value = 2;
        assert_eq!(c1.read().await.value, 2);
        tokio::spawn(async move {
            sleep(sleep_length).await;
            assert_eq!(c2.read().await.value, 1);
        });
        sleep(sleep_length).await;
        assert_eq!(c1.read().await.value, 1);
    }
}
