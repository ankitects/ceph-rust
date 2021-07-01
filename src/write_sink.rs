use futures::{FutureExt, Sink};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::ceph::IoCtx;
use crate::completion::with_completion;
use crate::error::{RadosError, RadosResult};
use crate::rados::rados_aio_write;
use std::ffi::CString;
use std::os::raw::c_char;

const DEFAULT_CONCURRENCY: usize = 2;

pub struct WriteSink<'a> {
    ioctx: &'a IoCtx,
    in_flight: Vec<Pin<Box<dyn Future<Output = RadosResult<i32>> + 'a>>>,
    object_name: String,

    // Offset into object where the next write will land
    next: u64,

    // How many RADOS ops in flight at same time?
    concurrency: usize,
}

unsafe impl Send for WriteSink<'_> {}

impl<'a> WriteSink<'a> {
    pub fn new(ioctx: &'a IoCtx, object_name: &str, concurrency: Option<usize>) -> Self {
        let concurrency = concurrency.unwrap_or(DEFAULT_CONCURRENCY);
        assert!(concurrency > 0);

        Self {
            ioctx,
            in_flight: Vec::new(),
            object_name: object_name.to_string(),
            next: 0,
            concurrency,
        }
    }

    fn trim_in_flight(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        target_len: usize,
    ) -> Poll<Result<(), <Self as Sink<Vec<u8>>>::Error>> {
        // FIXME: there's no reason these need to complete in-order.

        while self.in_flight.len() > target_len {
            match self.in_flight[0].as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    self.in_flight.remove(0);

                    match result {
                        Err(e) => return Poll::Ready(Err(e)),
                        Ok(sz) => {
                            debug!("flush: IO completed with r={}", sz);
                        }
                    }
                }
            };
        }

        // Nothing left in flight, we're done
        Poll::Ready(Ok(()))
    }
}

impl<'a> Sink<Vec<u8>> for WriteSink<'a> {
    type Error = RadosError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // If we have fewer than 1 slots available, this will try to wait on some outstanding futures
        let target = self.as_ref().concurrency - 1;
        self.trim_in_flight(cx, target)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), Self::Error> {
        let ioctx = self.ioctx;
        let obj_name_str = CString::new(self.object_name.clone()).expect("CString error");
        let write_at = self.next;
        self.next += item.len() as u64;

        let mut fut = Box::pin(async move {
            let c = with_completion(ioctx, |c| unsafe {
                rados_aio_write(
                    ioctx.ioctx,
                    obj_name_str.as_ptr(),
                    c,
                    item.as_ptr() as *mut c_char,
                    item.len(),
                    write_at,
                )
            })?;

            c.await
        });

        // Kick the async{} future to get the RADOS op sent
        match fut.as_mut().now_or_never() {
            Some(Ok(_)) => Ok(()),
            Some(Err(e)) => return Err(e),
            None => {
                self.in_flight.push(fut);
                Ok(())
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.trim_in_flight(cx, 0)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // There is no special work to be done on close
        self.poll_flush(cx)
    }
}
