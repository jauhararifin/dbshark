use std::ops::{Deref, DerefMut};

pub trait Runtime: 'static {
    type Mutex<T: Send + Sync>: Mutex<T>;
    type RwMutex<T: Send + Sync>: RwMutex<T>;
}

pub trait Mutex<T: Send + Sync>: Send + Sync {
    type Guard<'a>: DerefMut<Target = T>
    where
        Self: 'a;

    fn new(data: T) -> Self;
    async fn lock(&self) -> Self::Guard<'_>;
    async fn try_lock(&self) -> Option<Self::Guard<'_>>;
}

pub trait MutexGuard<T: Send + Sync>: DerefMut<Target = T> {
    async fn unlock(self);
}

pub trait RwMutex<T: Send + Sync>: Send + Sync {
    type ReadGuard<'a>: Deref<Target = T> + From<Self::WriteGuard<'a>>
    where
        Self: 'a;
    type WriteGuard<'a>: DerefMut<Target = T>
    where
        Self: 'a;

    fn new(data: T) -> Self;

    async fn read(&self) -> Self::ReadGuard<'_>;

    async fn write(&self) -> Self::WriteGuard<'_>;

    async fn try_write(&self) -> Option<Self::WriteGuard<'_>>;
}

pub trait RwMutexReadGuard<'a, T> {
    async fn unlock(self);
}
