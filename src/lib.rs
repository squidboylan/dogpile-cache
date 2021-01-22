use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, RwLock, RwLockReadGuard};

//type RFuncRet<T> = Future<Output = Result<(T, Instant, Instant), ()>>;

struct DogpileCache<T> {
    cache_data: Arc<RwLock<CacheData<T>>>,
    refreshed: Arc<Notify>,
    refresh_fn: fn() -> Box<dyn Future<Output = Result<(T, Instant, Instant), ()>> + Unpin>,
}

pub struct CacheData<T> {
    pub value: T,
    expire_time: Instant,
}

impl<T> Clone for DogpileCache<T> {
    fn clone(&self) -> Self {
        Self {
            cache_data: self.cache_data.clone(),
            refreshed: self.refreshed.clone(),
            refresh_fn: self.refresh_fn.clone(),
        }
    }
}

impl<T: Send + Sync + 'static> DogpileCache<T> {
    pub async fn create(
        init: T,
        refresh_fn: fn() -> Box<dyn Future<Output = Result<(T, Instant, Instant), ()>> + Unpin>,
    ) -> Self {
        let cache_data = Arc::new(RwLock::new(CacheData {
            value: init,
            expire_time: Instant::now(),
        }));
        let refreshed = Arc::new(Notify::new());
        let cache = Self {
            cache_data,
            refreshed,
            refresh_fn: refresh_fn,
        };
        cache
    }
    pub async fn read<'a>(&'a self) -> RwLockReadGuard<'a, CacheData<T>> {
        //let n = self.refreshed.notified();
        if self.cache_data.read().await.expire_time <= Instant::now() {
            let mut cd_writer = self.cache_data.write().await;
            if cd_writer.expire_time <= Instant::now() {
                if let Ok((new_value, new_expire_time, _)) = (self.refresh_fn)().await {
                    cd_writer.value = new_value;
                    cd_writer.expire_time = new_expire_time;
                }
                //n.notify_waiters();
            }
        }
        self.cache_data.read().await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::time::sleep;
    #[tokio::test]
    async fn test1() {
        //let valid_length = Duration::from_millis(20);
        let sleep_length = Duration::from_millis(21);
        fn num() -> Box<dyn Future<Output = Result<(i32, Instant, Instant), ()>> + Unpin> {
            let valid_length = Duration::from_millis(20);
            Box::new(std::future::ready(Ok((
                1,
                Instant::now() + valid_length,
                Instant::now() + valid_length,
            ))))
        }
        let c = DogpileCache::<i32>::create(0, num).await;
        assert_eq!(c.read().await.value, 1);
        c.cache_data.write().await.value = 2;
        assert_eq!(c.read().await.value, 2);
        sleep(sleep_length).await;
        assert_eq!(c.read().await.value, 1);
    }
}
