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

use crate::ceph::IoCtx;
use crate::error::RadosResult;
use crate::rados::{
    rados_aio_cancel, rados_aio_create_completion2, rados_aio_get_return_value,
    rados_aio_is_complete, rados_aio_release, rados_aio_wait_for_complete_and_cb,
    rados_completion_t,
};

struct Completion {
    inner: rados_completion_t,

    // Box to provide a stable address for completion_complete callback
    // Mutex to make Sync-safe for write from poll() vs read from completion_complete
    waker: Box<std::sync::Mutex<Option<std::task::Waker>>>,

    // A reference to the IOCtx is required to issue a cancel on
    // the operation if we are dropped before ready.  This needs
    // to be an Arc rather than a raw rados_ioctx_t because otherwise
    // there would be nothing to stop the rados_ioctx_t being invalidated
    // during the lifetime of this Completion.
    // (AioCompletionImpl does hold a reference to IoCtxImpl for writes, but
    //  not for reads.)
    ioctx: Arc<IoCtx>,
}

unsafe impl Send for Completion {}

#[no_mangle]
pub extern "C" fn completion_complete(_cb: rados_completion_t, arg: *mut c_void) -> () {
    let waker = unsafe {
        let p = arg as *mut Mutex<Option<Waker>>;
        p.as_mut().unwrap()
    };

    let waker = waker.lock().unwrap().take();
    match waker {
        Some(w) => w.wake(),
        None => {}
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        // Ensure that after dropping the Completion, the AIO callback
        // will not be called on our dropped waker Box.  Only necessary
        // if we got as far as successfully starting an operation using
        // the completion.
        let am_complete = unsafe { rados_aio_is_complete(self.inner) } != 0;
        if !am_complete {
            unsafe {
                rados_aio_cancel(self.ioctx.ioctx, self.inner);
                rados_aio_wait_for_complete_and_cb(self.inner);
            }
        }

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

fn with_completion_impl<F>(ioctx: Arc<IoCtx>, f: F) -> RadosResult<Completion>
where
    F: FnOnce(rados_completion_t) -> libc::c_int,
{
    let mut waker = Box::new(Mutex::new(None));

    let completion = unsafe {
        let mut completion: rados_completion_t = std::ptr::null_mut();
        let p: *mut Mutex<Option<Waker>> = &mut *waker;
        let p = p as *mut c_void;

        let r = rados_aio_create_completion2(p, Some(completion_complete), &mut completion);
        if r != 0 {
            panic!("Error {} allocating RADOS completion: out of memory?", r);
        }

        completion
    };

    let ret_code = f(completion);
    if ret_code < 0 {
        // On error dispatching I/O, drop the unused rados_completion_t
        unsafe {
            rados_aio_release(completion);
            drop(completion)
        }
        Err(ret_code.into())
    } else {
        // Pass the rados_completion_t into a Future-implementing wrapper and await it.
        Ok(Completion {
            ioctx,
            inner: completion,
            waker,
        })
    }
}
/// Completions are only created via this wrapper, in order to ensure
/// that the Completion struct is only constructed around 'armed' rados_completion_t
/// instances (i.e. those that have been used to start an I/O).
pub async fn with_completion<F>(ioctx: Arc<IoCtx>, f: F) -> RadosResult<i32>
where
    F: FnOnce(rados_completion_t) -> libc::c_int,
{
    // Hide c_void* temporaries in a non-async function so that the future generated
    // by this function isn't encumbered by their non-Send-ness.
    let completion = with_completion_impl(ioctx, f)?;

    completion.await
}
