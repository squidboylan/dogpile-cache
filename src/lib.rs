//! dogpile-cache provides a cache which holds values that expire on a timer
//! and will make an effort to premtively refresh the value before it is
//! expired and do so in a way that requires little waiting.
//!
//! Simple tested example
//! ```rust
//! use std::time::{Duration, Instant};
//! use dogpile_cache_rs::{DogpileCache, CacheData, ExponentialBackoff};
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
//!     let backoff = ExponentialBackoff {
//!         current_interval: Duration::from_millis(50),
//!         initial_interval: Duration::from_millis(50),
//!         randomization_factor: 0.0,
//!         ..backoff::ExponentialBackoff::default()
//!     };
//!     let c = DogpileCache::<i32>::create(num, 1, backoff).await;
//!     assert_eq!(c.read().await.value, 1);
//! }
//! ```
//!
//! More realistic usecase for caching a token:
//! ```ignore
//! use std::time::{Duration, Instant};
//! use dogpile_cache_rs::{DogpileCache, CacheData, ExponentialBackoff};
//! use reqwest::Client;
//!
//! #[tokio::main]
//! async fn main() {
//!     async fn gen_token(c: Client) -> reqwest::Result<CacheData<Token>> {
//!         let token = generate_token_using_client(c)?;
//!         Ok(CacheData::new(
//!             token,
//!             Instant::now() + token.valid_length,
//!             Instant::now() + token.valid_length/2,
//!         ))
//!     }
//!     let backoff = ExponentialBackoff {
//!         current_interval: Duration::from_millis(50),
//!         initial_interval: Duration::from_millis(50),
//!         randomization_factor: 0.0,
//!         ..backoff::ExponentialBackoff::default()
//!     };
//!     let client = Client::new()
//!     let c = DogpileCache::<i32>::create(num, client, backoff).await;
//!     let token_lock = c.read().await;
//! }
//! ```

pub use backoff::backoff::Backoff;
pub use backoff::backoff::Constant as ConstantBackoff;
pub use backoff::exponential::ExponentialBackoff;
pub use backoff::ExponentialBackoff as ExponentialBackoffSystemClock;
use parking_lot::{RwLock, RwLockReadGuard};
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;
use tokio::select;
use tokio::sync::Notify;
use tokio::time;

/// A simple cache that holds some data that expires and has a task to attempt to refresh
/// the value before it expires. This struct uses `Arc` internally to store its data so clones of
/// a cache will point to the same data.
pub struct DogpileCache<T> {
    cache_data: Arc<RwLock<CacheData<T>>>,
    notifiers: Arc<Notifiers>,
}

struct Notifiers {
    refreshed: Notify,
    expired: Notify,
    once: std::sync::Once,
}

/// The value the DogpileCache will store and how often it expires and should be refreshed.
pub struct CacheData<T> {
    pub value: T,
    pub expire_time: Instant,
    pub refresh_time: Instant,
}

struct CacheRefresher<T, A, F, B> {
    cache: DogpileCache<T>,
    refresh_fn: fn(A) -> F,
    refresh_arg: A,
    backoff: B,
    next_wake: Instant,
}

impl<T> Clone for DogpileCache<T> {
    fn clone(&self) -> Self {
        Self {
            cache_data: self.cache_data.clone(),
            notifiers: self.notifiers.clone(),
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
    ///
    /// Note: The ExponentialBackoff can return `None` when calling `next_backoff()` because it has a
    /// `max_elapsed_time` field. For the DogpileCache this does not make sense as the refresh task
    /// is expected to always retry. For this reason if `next_backoff()` returns `None` the
    /// previous value will be used.
    pub async fn create<
        A: Clone + Send + Sync + 'static,
        E,
        F: Future<Output = Result<CacheData<T>, E>> + Send + 'static,
        B: Backoff + Send + 'static,
    >(
        refresh_fn: fn(A) -> F,
        refresh_arg: A,
        backoff: B,
    ) -> Self {
        let expire_time = Instant::now();
        let refresh_time = Instant::now();
        let cache_data = Arc::new(RwLock::new(CacheData {
            value: T::default(),
            expire_time,
            refresh_time,
        }));
        let notifiers = Arc::new(Notifiers {
            expired: Notify::default(),
            refreshed: Notify::default(),
            once: std::sync::Once::new(),
        });
        let cache = Self {
            cache_data,
            notifiers,
        };
        let c = cache.clone();
        tokio::spawn(async move {
            let refresher = CacheRefresher::create(c, refresh_fn, refresh_arg, backoff);
            refresher.run().await;
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
        let n = self.notifiers.refreshed.notified();
        {
            let read_lock = self.cache_data.read();
            if read_lock.expire_time > Instant::now() {
                return read_lock;
            }
        }
        self.refresh();
        n.await;
        self.cache_data.read()
    }

    /// Trigger a cach refresh and wait to be notified of the updated cache value
    fn refresh(&self) {
        // The first time we do this we need to call notify() as the refresher task may not be
        // listening already
        self.notifiers
            .once
            .call_once(|| self.notifiers.expired.notify_one());
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
        E,
        F: Future<Output = Result<CacheData<T>, E>> + Send + 'static,
        B: Backoff + Send + 'static,
    > CacheRefresher<T, A, F, B>
{
    fn create(cache: DogpileCache<T>, refresh_fn: fn(A) -> F, refresh_arg: A, backoff: B) -> Self {
        Self {
            cache,
            refresh_fn,
            refresh_arg,
            backoff,
            next_wake: Instant::now(),
        }
    }
    #[allow(unused_assignments)]
    async fn run(mut self) -> ! {
        select! {
            _ = self.cache.notifiers.expired.notified() => {
                self.refresh().await;
            }
        }
        loop {
            select! {
                _ = time::sleep_until(time::Instant::from_std(self.next_wake)) => {
                    self.refresh().await;
                }
            }
        }
    }
    async fn refresh(&mut self) {
        // We need to hold the refresh lock so only one task will attempt to generate the new value
        if let Ok(CacheData {
            value: new_value,
            expire_time: new_expire_time,
            refresh_time: new_refresh_time,
        }) = (self.refresh_fn)(self.refresh_arg.clone()).await
        {
            self.backoff.reset();
            let mut cd_writer = self.cache.cache_data.write();
            cd_writer.value = new_value;
            cd_writer.expire_time = new_expire_time;
            cd_writer.refresh_time = new_refresh_time;
            self.next_wake = std::cmp::min(new_expire_time, new_refresh_time);
            self.cache.notifiers.refreshed.notify_waiters();
        } else if let Some(dur) = self.backoff.next_backoff() {
            self.next_wake = Instant::now() + dur;
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::time::sleep;
    #[tokio::test]
    async fn test_cache_basics() {
        let sleep_length = Duration::from_millis(10);
        let backoff = ExponentialBackoff {
            max_elapsed_time: None,
            current_interval: Duration::from_millis(50),
            initial_interval: Duration::from_millis(50),
            randomization_factor: 0.0,
            ..backoff::ExponentialBackoff::default()
        };

        async fn num(v: Arc<Mutex<i32>>) -> Result<CacheData<i32>, ()> {
            let valid_length = Duration::from_millis(20);
            let mut l = v.lock().unwrap();
            *l += 1;
            Ok(CacheData::new(
                *l,
                Instant::now() + valid_length,
                Instant::now() + valid_length / 2,
            ))
        }
        let c1 = DogpileCache::<i32>::create(num, Arc::new(Mutex::new(0)), backoff).await;
        let c2 = c1.clone();
        sleep(Duration::from_millis(6)).await;
        assert_eq!(c1.read().await.value, 1);
        c1.cache_data.write().value = 10;
        assert_eq!(c1.read().await.value, 10);
        sleep(Duration::from_millis(5)).await;
        assert_eq!(c1.read().await.value, 10);
        tokio::spawn(async move {
            sleep(sleep_length).await;
            assert_eq!(c2.read().await.value, 2);
            sleep(sleep_length).await;
            assert_eq!(c2.read().await.value, 3);
            sleep(sleep_length).await;
            sleep(sleep_length).await;
            assert_eq!(c2.read().await.value, 5);
        });
        sleep(sleep_length).await;
        assert_eq!(c1.read().await.value, 2);
    }
}
