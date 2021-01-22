use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

struct DogpileCache<T> {
    pub value: Arc<RwLock<T>>,
}

impl<T> Clone for DogpileCache<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
        }
    }
}

impl<T: Send + Sync + 'static> DogpileCache<T> {
    pub async fn create<F: Future<Output = T> + Send + 'static>(
        refresh_fn: fn() -> F,
        length_valid: Duration,
    ) -> Self {
        let value = Arc::new(RwLock::new(refresh_fn().await));
        let cache = Self { value };
        let c = cache.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(length_valid).await;
                let new_value = refresh_fn().await;
                let mut write_lock = c.value.write().await;
                *write_lock = new_value;
            }
        });
        cache
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[tokio::test]
    async fn test1() {
        let valid_length = Duration::from_millis(20);
        let sleep_length = Duration::from_millis(21);
        async fn num() -> i32 {
            1
        }
        let c = DogpileCache::create(num, valid_length).await;
        assert_eq!(*c.value.read().await, 1);
        *c.value.write().await = 2;
        assert_eq!(*c.value.read().await, 2);
        tokio::time::sleep(sleep_length).await;
        assert_eq!(*c.value.read().await, 1);
    }
}
