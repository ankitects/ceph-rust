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
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

use crate::error::RadosResult;
use crate::rados::{
    rados_aio_create_completion2, rados_aio_get_return_value, rados_aio_is_complete,
    rados_aio_release, rados_aio_wait_for_complete_and_cb, rados_completion_t,
};

pub struct Completion {
    completion: rados_completion_t,

    // Box to provide a stable address for completion_complete callback
    // Mutex for memory fencing when writing from poll() and reading from completion_complete()
    waker: Box<std::sync::Mutex<Option<std::task::Waker>>>,
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

impl Completion {
    pub fn new() -> Self {
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

        Self { completion, waker }
    }

    pub fn get_completion(&self) -> rados_completion_t {
        self.completion
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        let am_complete = unsafe { rados_aio_is_complete(self.completion) } != 0;

        // We must block here to avoid leaving the completion in an armed
        // state (where it would call its callback on a dropped Completion)
        // when we release it.
        if !am_complete {
            unsafe {
                rados_aio_wait_for_complete_and_cb(self.completion);
            }
        }

        unsafe {
            rados_aio_release(self.completion);
        }
    }
}

impl std::future::Future for Completion {
    type Output = crate::error::RadosResult<i32>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let am_complete = unsafe { rados_aio_is_complete(self.completion) } != 0;

        if am_complete {
            let r = unsafe { rados_aio_get_return_value(self.completion) };
            let result = if r < 0 { Err(r.into()) } else { Ok(r) };
            std::task::Poll::Ready(result)
        } else {
            // Register a waker
            *self.as_mut().waker.lock().unwrap() = Some(cx.waker().clone());

            std::task::Poll::Pending
        }
    }
}

pub async fn with_completion<F>(f: F) -> RadosResult<i32>
where
    F: FnOnce(rados_completion_t) -> libc::c_int,
{
    let completion = Completion::new();
    let ret_code = f(completion.get_completion());
    if ret_code < 0 {
        Err(ret_code.into())
    } else {
        completion.await
    }
}
