use crate::runtime::{Mutex, MutexGuard, Runtime, RwMutex, RwMutexReadGuard};
use parking_lot;
use std::ops::{Deref, DerefMut};

pub struct TokioRuntime;

impl Runtime for TokioRuntime {
    type Mutex<T: Send + Sync> = TokioMutex<T>;
    type RwMutex<T: Send + Sync> = TokioRwMutex<T>;
}

pub struct TokioMutex<T> {
    inner: parking_lot::Mutex<T>,
}

impl<T: Send + Sync> Mutex<T> for TokioMutex<T> {
    type Guard<'a> = TokioGuard<'a, T>
    where
        Self: 'a;

    fn new(data: T) -> Self {
        Self {
            inner: parking_lot::Mutex::new(data),
        }
    }

    async fn lock(&self) -> Self::Guard<'_> {
        TokioGuard {
            inner: Some(self.inner.lock()),
        }
    }

    async fn try_lock(&self) -> Option<Self::Guard<'_>> {
        self.inner
            .try_lock()
            .map(|guard| TokioGuard { inner: Some(guard) })
    }
}

pub struct TokioGuard<'a, T> {
    inner: Option<parking_lot::MutexGuard<'a, T>>,
}

impl<'a, T> Deref for TokioGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap().deref()
    }
}

impl<'a, T> DerefMut for TokioGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().unwrap().deref_mut()
    }
}

impl<'a, T: Send + Sync> MutexGuard<T> for TokioGuard<'a, T> {
    async fn unlock(mut self) {
        self.inner.take();
    }
}

impl<'a, T> Drop for TokioGuard<'a, T> {
    fn drop(&mut self) {
        if self.inner.is_none() {
            panic!("mutex guard is dropped without unlock")
        }
    }
}

pub struct TokioRwMutex<T> {
    inner: Option<parking_lot::RwLock<T>>,
}

impl<T: Send + Sync> RwMutex<T> for TokioRwMutex<T> {
    type ReadGuard<'a> = TokioReadGuard<'a, T>
    where
        Self: 'a;
    type WriteGuard<'a> = TokioWriteGuard<'a,T>
    where
        Self: 'a;

    fn new(data: T) -> Self {
        Self {
            inner: Some(parking_lot::RwLock::new(data)),
        }
    }

    async fn read(&self) -> Self::ReadGuard<'_> {
        TokioReadGuard {
            inner: Some(self.inner.as_ref().unwrap().read()),
        }
    }

    async fn write(&self) -> Self::WriteGuard<'_> {
        TokioWriteGuard {
            inner: Some(self.inner.as_ref().unwrap().write()),
        }
    }

    async fn try_write(&self) -> Option<Self::WriteGuard<'_>> {
        let guard = self.inner.as_ref().unwrap().try_write()?;
        Some(TokioWriteGuard { inner: Some(guard) })
    }
}

pub struct TokioReadGuard<'a, T> {
    inner: Option<parking_lot::RwLockReadGuard<'a, T>>,
}

impl<'a, T> From<TokioWriteGuard<'a, T>> for TokioReadGuard<'a, T> {
    fn from(mut value: TokioWriteGuard<'a, T>) -> Self {
        Self {
            inner: Some(parking_lot::RwLockWriteGuard::downgrade(
                value.inner.take().unwrap(),
            )),
        }
    }
}

impl<'a, T> Deref for TokioReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap().deref()
    }
}

impl<'a, T: Send + Sync> RwMutexReadGuard<'a, T> for TokioReadGuard<'a, T> {
    async fn unlock(mut self) {
        self.inner.take();
    }
}

impl<'a, T> Drop for TokioReadGuard<'a, T> {
    fn drop(&mut self) {
        if self.inner.is_none() {
            panic!("rwmutex read guard is dropped without unlock")
        }
    }
}

pub struct TokioWriteGuard<'a, T> {
    inner: Option<parking_lot::RwLockWriteGuard<'a, T>>,
}

impl<'a, T> Deref for TokioWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap().deref()
    }
}

impl<'a, T> DerefMut for TokioWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().unwrap().deref_mut()
    }
}
