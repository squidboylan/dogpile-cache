use log::{debug, warn};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::sync::{Notify, RwLock, RwLockReadGuard};
use tokio::time;

struct DogpileCache<T, F: Future<Output = Result<(T, Instant, Instant), ()>> + Send + 'static> {
    cache_data: Arc<RwLock<CacheData<T>>>,
    refreshed: Arc<Notify>,
    refresh_fn: fn() -> F,
}

pub struct CacheData<T> {
    pub value: T,
    expire_time: Instant,
    refresh_time: Instant,
}

impl<T, F: Future<Output = Result<(T, Instant, Instant), ()>> + Send + 'static> Clone
    for DogpileCache<T, F>
{
    fn clone(&self) -> Self {
        Self {
            cache_data: self.cache_data.clone(),
            refreshed: self.refreshed.clone(),
            refresh_fn: self.refresh_fn.clone(),
        }
    }
}

#[allow(dead_code)]
impl<
        T: Send + Sync + 'static,
        F: Future<Output = Result<(T, Instant, Instant), ()>> + Send + 'static,
    > DogpileCache<T, F>
{
    pub async fn create(init: T, refresh_fn: fn() -> F) -> Self {
        let mut expire_time = Instant::now();
        let mut refresh_time = Instant::now();
        let cache_data = Arc::new(RwLock::new(CacheData {
            value: init,
            expire_time,
            refresh_time,
        }));
        let refreshed = Arc::new(Notify::new());
        let cache = Self {
            cache_data,
            refreshed,
            refresh_fn,
        };
        let c = cache.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = time::sleep_until(time::Instant::from_std(refresh_time)) => {
                        debug!("Starting refresh");
                        let times = c.refresh().await;
                        debug!("Refresh finished");
                        expire_time = times.0;
                        refresh_time = times.1;
                    }
                    _ = time::sleep_until(time::Instant::from_std(expire_time)) => {
                        debug!("Starting refresh");
                        let times = c.refresh().await;
                        debug!("Refresh finished");
                        expire_time = times.0;
                        refresh_time = times.1;
                    }
                }
            }
        });
        cache
    }
    pub async fn read<'a>(&'a self) -> RwLockReadGuard<'a, CacheData<T>> {
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
    async fn refresh(&self) -> (Instant, Instant) {
        // We need to hold the refresh lock so only one task will attempt to generate the new value
        if let Ok((new_value, new_expire_time, new_refresh_time)) = (self.refresh_fn)().await {
            debug!("Acquiring writer lock");
            let mut cd_writer = self.cache_data.write().await;
            cd_writer.value = new_value;
            cd_writer.expire_time = new_expire_time;
            cd_writer.refresh_time = new_refresh_time;
            debug!("notifying waiters");
            self.refreshed.notify_waiters();
            return (new_expire_time, new_refresh_time);
        }
        warn!("Refresh fn failed");
        (
            Instant::now() + Duration::from_millis(500),
            Instant::now() + Duration::from_millis(500),
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::time::sleep;
    #[tokio::test]
    async fn test1() {
        env_logger::init();
        let sleep_length = Duration::from_millis(21);
        async fn num() -> Result<(i32, Instant, Instant), ()> {
            let valid_length = Duration::from_millis(20);
            Ok((
                1,
                Instant::now() + valid_length,
                Instant::now() + valid_length,
            ))
        }
        let c = DogpileCache::<i32, _>::create(0, num).await;
        println!("Created dogpile cache");
        assert_eq!(c.read().await.value, 1);
        c.cache_data.write().await.value = 2;
        assert_eq!(c.read().await.value, 2);
        assert_eq!(c.read().await.value, 2);
        sleep(sleep_length).await;
        assert_eq!(c.read().await.value, 1);
    }
}
