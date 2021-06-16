// Copyright 2021 John Spray All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ffi::c_void;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::error::RadosResult;
use crate::rados::{
    rados_aio_create_completion2, rados_aio_get_return_value, rados_aio_is_complete,
    rados_aio_release, rados_completion_t,
};

struct Completion {
    inner: rados_completion_t,

    // - Arc to provide a stable address for completion_complete callback, and
    // refcounting to allow the librados completion to call into completion_complete
    // safely even if Completion has been dropped.
    // - Mutex for memory fencing when writing from poll() and reading from completion_complete()
    waker: Arc<Mutex<Option<std::task::Waker>>>,
}

unsafe impl Send for Completion {}

#[no_mangle]
pub extern "C" fn completion_complete(_cb: rados_completion_t, arg: *mut c_void) -> () {
    let waker = unsafe { Arc::from_raw(arg as *mut Mutex<Option<Waker>>) };

    let locked = waker.lock().unwrap().take();
    if let Some(w) = locked {
        w.wake()
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        // If we had a waker registered, forget it.  Any subsequent calls up from
        // librados into completion_complete will be no-ops apart from decrementing
        // the reference count on the waker Arc to allow it to drop.
        *self.waker.lock().unwrap() = None;

        unsafe {
            rados_aio_release(self.inner);
        }
    }
}

impl std::future::Future for Completion {
    type Output = crate::error::RadosResult<i32>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let am_complete = unsafe { rados_aio_is_complete(self.inner) } != 0;

        if am_complete {
            let r = unsafe { rados_aio_get_return_value(self.inner) };
            let result = if r < 0 { Err(r.into()) } else { Ok(r) };
            std::task::Poll::Ready(result)
        } else {
            // Register a waker
            *self.as_mut().waker.lock().unwrap() = Some(cx.waker().clone());

            std::task::Poll::Pending
        }
    }
}

fn with_completion_impl<F>(f: F) -> RadosResult<Completion>
where
    F: FnOnce(rados_completion_t) -> libc::c_int,
{
    let waker = Arc::new(Mutex::new(None));

    let (completion, waker_p) = unsafe {
        let mut completion: rados_completion_t = std::ptr::null_mut();

        // Leak a reference to waker.  This will get cleaned up by completion_complete,
        // or later in this function if there's an error dispatching the I/O.
        let p = Arc::into_raw(waker.clone()) as *mut c_void;

        let r = rados_aio_create_completion2(p, Some(completion_complete), &mut completion);
        if r != 0 {
            panic!("Error {} allocating RADOS completion: out of memory?", r);
        }
        assert!(!completion.is_null());

        (completion, p)
    };

    let ret_code = f(completion);
    if ret_code < 0 {
        // On error dispatching I/O, drop the unused rados_completion_t and decrement
        // the refcount on waker
        unsafe {
            Arc::from_raw(waker_p);
            rados_aio_release(completion);
        }
        Err(ret_code.into())
    } else {
        // Pass the rados_completion_t into a Future-implementing wrapper and await it.

        let wrapped = Completion {
            inner: completion,
            waker: waker.clone(),
        };

        Ok(wrapped)
    }
}
/// Completions are only created via this wrapper, in order to ensure
/// that the Completion struct is only constructed around 'armed' rados_completion_t
/// instances (i.e. those that have been used to start an I/O).
pub async fn with_completion<F>(f: F) -> RadosResult<i32>
where
    F: FnOnce(rados_completion_t) -> libc::c_int,
{
    // Hide c_void* temporaries in a non-async function so that the future generated
    // by this function isn't encumbered by their non-Send-ness.
    let completion = with_completion_impl(f)?;

    completion.await
}

#[cfg(test)]
mod test {
    use crate::completion::with_completion;
    use crate::error::RadosError::ApiError;
    use futures::FutureExt;

    #[test]
    /// Test that on early errors, we do not block or panic
    fn dispatch_err() {
        let wait_result = with_completion(|_c| -5).now_or_never();

        match wait_result {
            Some(Err(ApiError(e))) => assert_eq!(e, nix::errno::Errno::EIO),
            _ => panic!("Bad IO result"),
        };
    }
}
