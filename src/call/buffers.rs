// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::{self, BufRead, ErrorKind, Read};
use std::{
    cmp,
    mem::{self, MaybeUninit},
    slice, usize,
};

use crate::grpc_sys::{self, grpc_byte_buffer, grpc_byte_buffer_reader, grpc_slice};

#[cfg(feature = "prost-codec")]
use bytes::{Buf, BufMut};

struct GrpcSlice(grpc_slice);

impl GrpcSlice {
    fn len(&self) -> usize {
        unsafe { grpc_sys::grpcwrap_slice_length(&self.0) }
    }

    fn range_from(&self, offset: usize) -> &[u8] {
        unsafe {
            let mut len = 0;
            let ptr = grpc_sys::grpcwrap_slice_raw_offset(&self.0, offset, &mut len);
            slice::from_raw_parts(ptr as _, len)
        }
    }
}

impl Default for GrpcSlice {
    fn default() -> Self {
        GrpcSlice(unsafe { grpc_sys::grpc_empty_slice() })
    }
}

impl Drop for GrpcSlice {
    fn drop(&mut self) {
        unsafe {
            grpc_sys::grpcwrap_slice_unref(&mut self.0);
        }
    }
}

struct GrpcByteBufferReader(grpc_byte_buffer_reader);

impl GrpcByteBufferReader {
    /// Create a wrapper around a `grpc_byte_buffer_reader` reading from `buf`.
    ///
    /// Safety: this `GrpcByteBufferReader` takes ownership of `buf` and will
    /// destroy it. `buf` must be valid when this method is called.
    unsafe fn new(buf: *mut grpc_byte_buffer) -> GrpcByteBufferReader {
        let mut reader = MaybeUninit::uninit();
        let init_result = grpc_sys::grpc_byte_buffer_reader_init(reader.as_mut_ptr(), buf);
        assert_eq!(init_result, 1);
        GrpcByteBufferReader(reader.assume_init())
    }

    fn len(&self) -> usize {
        unsafe { grpc_sys::grpc_byte_buffer_length(self.0.buffer_out) }
    }

    fn next_slice(&mut self) -> GrpcSlice {
        unsafe {
            let mut slice = GrpcSlice::default();
            let code = grpc_sys::grpc_byte_buffer_reader_next(&mut self.0, &mut slice.0);
            debug_assert_ne!(code, 0);
            slice
        }
    }
}

impl Drop for GrpcByteBufferReader {
    fn drop(&mut self) {
        unsafe {
            let buf = self.0.buffer_in;
            grpc_sys::grpc_byte_buffer_reader_destroy(&mut self.0);
            grpc_sys::grpc_byte_buffer_destroy(buf);
        }
    }
}

/// `MessageReader` is a zero-copy reader for the message payload.
///
/// To achieve zero-copy, use the BufRead API `fill_buf` and `consume`
/// to operate the reader.
pub struct MessageReader {
    reader: GrpcByteBufferReader,
    buffer_slice: GrpcSlice,
    buffer_offset: usize,
    remaining: usize,
}

unsafe impl Send for MessageReader {}

impl MessageReader {
    /// Create a new `MessageReader`.
    ///
    /// Safety: `raw` must be a unique reference. The `MessageReader` takes
    /// ownership of `raw` and will destroy it, the caller should not use the
    /// reference after calling this method.
    pub unsafe fn new(raw: *mut grpc_byte_buffer) -> MessageReader {
        let reader = GrpcByteBufferReader::new(raw);
        let remaining = reader.len();

        MessageReader {
            reader,
            buffer_slice: Default::default(),
            buffer_offset: 0,
            remaining,
        }
    }
}

impl Read for MessageReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let amt = {
            let bytes = self.fill_buf()?;
            if bytes.is_empty() {
                return Ok(0);
            }
            let amt = cmp::min(buf.len(), bytes.len());
            buf[..amt].copy_from_slice(&bytes[..amt]);
            amt
        };

        self.consume(amt);
        Ok(amt)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }
        buf.reserve(self.remaining);
        let start = buf.len();
        let mut len = start;
        unsafe {
            buf.set_len(start + self.remaining);
        }
        let ret = loop {
            match self.read(&mut buf[len..]) {
                Ok(0) => break Ok(len - start),
                Ok(n) => len += n,
                Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => break Err(e),
            }
        };
        unsafe {
            buf.set_len(len);
        }
        ret
    }
}

impl BufRead for MessageReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        // Optimization for empty slice
        if self.remaining == 0 {
            return Ok(&[]);
        }

        // When finished reading current `buffer_slice`, start reading next slice
        let buffer_len = self.buffer_slice.len();
        if buffer_len == 0 || self.buffer_offset == buffer_len {
            self.buffer_slice = self.reader.next_slice();
            self.buffer_offset = 0;
        }

        debug_assert!(self.buffer_offset <= buffer_len);
        Ok(self.buffer_slice.range_from(self.buffer_offset))
    }

    fn consume(&mut self, amt: usize) {
        self.remaining -= amt;
        self.buffer_offset += amt;
    }
}

#[cfg(feature = "prost-codec")]
impl Buf for MessageReader {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn bytes(&self) -> &[u8] {
        // This is similar but not identical to `BuffRead::fill_buf`, since `self`
        // is not mutable, we can only return bytes up to the end of the current
        // slice.

        // Optimization for empty slice
        if self.buffer_slice.is_empty() {
            return &[];
        }

        debug_assert!(self.buffer_offset <= self.buffer_slice.len());
        self.buffer_slice.range_from(self.buffer_offset)
    }

    fn advance(&mut self, mut cnt: usize) {
        // Similar but not identical to `BufRead::consume`. We must also advance
        // the buffer slice if we have exhausted the current slice.

        // The number of bytes remaining in the current slice.
        let mut remaining = self.buffer_slice.len() - self.buffer_offset;
        while remaining <= cnt {
            self.consume(remaining);
            if self.remaining == 0 {
                return;
            }

            cnt -= remaining;
            self.buffer_slice = self.reader.next_slice();
            self.buffer_offset = 0;
            remaining = self.buffer_slice.len();
        }

        self.consume(cnt);
    }
}

pub struct MessageWriter {
    // TODO recycle Vecs
    pub write_buffer: Vec<u8>,
}

impl MessageWriter {
    /// Create an empty MessageWriter.
    pub fn new() -> MessageWriter {
        MessageWriter {
            write_buffer: Vec::new(),
        }
    }

    pub fn clear(&mut self) {
        self.write_buffer.clear();
    }

    pub fn reserve(&mut self, size: usize) -> &mut [u8] {
        let new_len = self.write_buffer.len() + size;
        self.write_buffer.reserve(size);
        unsafe {
            self.write_buffer.set_len(new_len);
            &mut self.write_buffer
        }
    }

    // Unsafe because the caller takes responsibility for destroying the returned
    // byte buffer. Consumes and clears the internal buffer.
    pub unsafe fn byte_buffer(&mut self) -> *mut grpc_byte_buffer {
        let mut vec = Vec::new();
        mem::swap(&mut self.write_buffer, &mut vec);
        grpc_sys::grpc_raw_byte_buffer_create(alloc::vec_slice(vec), 1)
    }
}

/// A wrapper for `MessageWriter` for implementing `Bytes::BufMut`. A wrapper is
/// needed because `BufMut` can be read and written incrementally, which
/// `MessageWriter` does not support.
#[cfg(feature = "prost-codec")]
pub struct MessageWriterBuf<'a> {
    inner: &'a mut MessageWriter,
    offset: usize,
}

#[cfg(feature = "prost-codec")]
impl<'a> From<&'a mut MessageWriter> for MessageWriterBuf<'a> {
    fn from(inner: &'a mut MessageWriter) -> MessageWriterBuf<'a> {
        MessageWriterBuf { inner, offset: 0 }
    }
}

#[cfg(feature = "prost-codec")]
impl<'a> BufMut for MessageWriterBuf<'a> {
    fn remaining_mut(&self) -> usize {
        self.inner.write_buffer.len() - self.offset
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.offset += cnt;
    }

    unsafe fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.inner.write_buffer[self.offset..]
    }
}

// TODO update comments
// The below thread-locals and functions are for arena allocating some of the small
// data structures which grpc requires for slices (and which can't be stack allocated).
// Allocating these on the heap gets expensive when sending many small messages.
//
// If we fill our arenas, then `alloc` will use `Box` to allocate on the heap.
//
// Our arenas are kept in sync (treat this allocator as having a single arena).
// We track the start and end of an occupied zone (which may have some freed data
// inside).
mod alloc {
    use std::{cell::Cell, mem, ptr, usize};

    use crate::grpc_sys::{self, grpc_slice, grpc_slice_refcount, grpc_slice_refcount_vtable};

    // The size of our arenas in number of items per thread.
    // TODO value
    const ARENA_SIZE: u32 = 4000;

    thread_local! {
        // An arena for allocating `VecSlice`s. These are smart-pointer-like objects
        // which hold a pointer to the data itself.
        static SLICE_ARENA: [VecSlice; ARENA_SIZE as usize] = unsafe {
            mem::transmute(mem::zeroed::<[u8; ARENA_SIZE as usize * mem::size_of::<VecSlice>()]>())
        };
        static ARENA_MIN: Cell<usize> = Cell::new(0);
        static ARENA_MAX: Cell<usize> = Cell::new(0);
    }

    // Returned data is partially initialised. Why is this safe?
    unsafe fn alloc() -> *mut VecSlice {
        let min = ARENA_MIN.with(|i| i.get());
        let index = if min > 0 {
            let index = min - 1;
            ARENA_MIN.with(|i| i.set(index));
            index
        } else {
            let max = ARENA_MAX.with(|i| i.get());
            debug_assert!(max <= ARENA_SIZE as usize);
            if max == ARENA_SIZE as usize {
                // TODO remove
                eprintln!("boxing");
                let slice: *mut VecSlice = Box::into_raw(Box::new(mem::zeroed()));
                (*slice).boxed = ARENA_SIZE;
                return slice;
            }

            ARENA_MAX.with(|i| i.set(max + 1));
            max
        };

        let slice: *mut VecSlice = SLICE_ARENA.with(|a| &a[index] as *const _ as *mut _);
        debug_assert!((*slice).vtable == ptr::null());
        (*slice).boxed = index as u32;
        slice
    }

    unsafe fn free(slice: *mut VecSlice, index: u32) {
        debug_assert!((*slice).boxed == index && index < ARENA_SIZE);
        (*slice).vtable = ptr::null();

        let mut next = ARENA_MAX.with(|i| i.get());
        let mut prev = ARENA_MIN.with(|i| i.get());
        if prev as u32 == index && prev + 1 < next {
            let prev = SLICE_ARENA.with(|a| {
                while prev + 1 < next && a[prev].vtable == ptr::null() {
                    prev += 1
                }
                prev
            });
            ARENA_MIN.with(|i| i.set(prev));
        } else if next as u32 - 1 == index {
            let next = SLICE_ARENA.with(|a| {
                while next > 0 && a[next - 1].vtable == ptr::null() {
                    next -= 1
                }
                next
            });
            ARENA_MAX.with(|i| i.set(next));
        }
    }

    pub unsafe fn vec_slice(v: Vec<u8>) -> *mut grpc_slice {
        let mut data = grpc_sys::grpc_slice_grpc_slice_data::default();
        // FIXME use inlined slices (without allocation) for < 8 bytes.
        *data.refcounted.as_mut() = grpc_sys::grpc_slice_grpc_slice_data_grpc_slice_refcounted {
            bytes: v.as_ptr() as *const _ as *mut _,
            length: v.len(),
        };
        let slice = alloc();
        let result = &mut (*slice).slice;
        (*result).data = data;
        (*result).refcount = slice as *mut _;
        (*slice).vtable = &VEC_SLICE_VTABLE;
        (*slice).sub_refcount = slice as *mut _;
        (*slice).vec = Some(v);
        (*slice).count = 0;
        result
    }

    // comment: grpc_slice_refcount
    #[repr(C)]
    struct VecSlice {
        vtable: *const grpc_slice_refcount_vtable,
        sub_refcount: *mut grpc_slice_refcount,
        vec: Option<Vec<u8>>,
        count: u32,
        boxed: u32,
        slice: grpc_slice,
    }

    static VEC_SLICE_VTABLE: grpc_slice_refcount_vtable = grpc_slice_refcount_vtable {
        ref_: Some(vec_slice_ref),
        unref: Some(vec_slice_unref),
        eq: Some(grpc_sys::grpc_slice_default_eq_impl),
        hash: Some(grpc_sys::grpc_slice_default_hash_impl),
    };

    unsafe extern "C" fn vec_slice_ref(arg1: *mut ::std::os::raw::c_void) {
        let refcount = arg1 as *mut VecSlice;
        (*refcount).count += 1;
    }

    unsafe extern "C" fn vec_slice_unref(arg1: *mut ::std::os::raw::c_void) {
        let refcount = arg1 as *mut VecSlice;
        (*refcount).count -= 1;
        if (*refcount).count == 0 {
            let boxed = (*refcount).boxed;
            if boxed == ARENA_SIZE {
                let refcount = Box::from_raw(refcount);
                mem::drop(refcount);
            } else {
                let vec = (*refcount).vec.take();
                mem::drop(vec);
                free(refcount, boxed);
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::mem::MaybeUninit;

        #[test]
        fn test_alloc() {
            unsafe {
                for _ in 0..6 {
                    alloc();
                }
                assert_eq!(ARENA_MIN.with(|i| i.get()), 0);
                assert_eq!(ARENA_MAX.with(|i| i.get()), 6);

                for _ in 0..ARENA_SIZE {
                    alloc();
                }
                // We'll have allocated `ARENA_SIZE + 6` times by now, but the last 6
                // should have been heap-allocated because the arenas are full.
                assert_eq!(ARENA_MIN.with(|i| i.get()), 0);
                assert_eq!(ARENA_MAX.with(|i| i.get()), ARENA_SIZE as usize);
            }
            // Test will leak all slices.
        }

        #[test]
        fn test_free() {
            // This test allocs and frees vc slices in various orders to test
            // that the alloc and free mechanisms work as expected.
            unsafe {
                let mut data = vec![];
                for _ in 0..10 {
                    data.push(vec_slice(vec![1, 2, 3, 4, 5]));
                }
                assert_eq!(ARENA_MIN.with(|i| i.get()), 0);
                assert_eq!(ARENA_MAX.with(|i| i.get()), 10);
                while let Some(datum) = data.pop() {
                    // We have to ref and unref because the slices are returned with
                    // 0 refcount.
                    vec_slice_ref((*datum).refcount as *mut _);
                    vec_slice_unref((*datum).refcount as *mut _);
                }
                assert_eq!(ARENA_MIN.with(|i| i.get()), 0);
                assert_eq!(ARENA_MAX.with(|i| i.get()), 0);

                for _ in 0..10 {
                    data.push(vec_slice(vec![1, 2, 3, 4, 5]));
                }
                assert_eq!(ARENA_MIN.with(|i| i.get()), 0);
                assert_eq!(ARENA_MAX.with(|i| i.get()), 10);
                for datum in data.into_iter().skip(2) {
                    vec_slice_ref((*datum).refcount as *mut _);
                    vec_slice_unref((*datum).refcount as *mut _);
                }
                assert_eq!(ARENA_MIN.with(|i| i.get()), 0);
                assert_eq!(ARENA_MAX.with(|i| i.get()), 2);
            }
            // Test will leak some slices.
        }

        #[test]
        fn test_free_min() {
            unsafe {
                let mut data = vec![];
                for _ in 0..10 {
                    data.push(vec_slice(vec![1, 2, 3, 4, 5]));
                }
                assert_eq!(ARENA_MIN.with(|i| i.get()), 0);
                assert_eq!(ARENA_MAX.with(|i| i.get()), 10);
                for datum in data.into_iter().take(5) {
                    vec_slice_ref((*datum).refcount as *mut _);
                    vec_slice_unref((*datum).refcount as *mut _);
                }
                assert_eq!(ARENA_MIN.with(|i| i.get()), 5);
                assert_eq!(ARENA_MAX.with(|i| i.get()), 10);
                let mut data = vec![];
                for _ in 0..3 {
                    data.push(vec_slice(vec![1, 2, 3, 4, 5]));
                }
                assert_eq!(ARENA_MIN.with(|i| i.get()), 2);
                assert_eq!(ARENA_MAX.with(|i| i.get()), 10);
                while let Some(datum) = data.pop() {
                    vec_slice_ref((*datum).refcount as *mut _);
                    vec_slice_unref((*datum).refcount as *mut _);
                }
                assert_eq!(ARENA_MIN.with(|i| i.get()), 5);
                assert_eq!(ARENA_MAX.with(|i| i.get()), 10);
            }
            // Test will leak some slices.
        }

        #[test]
        fn test_free_bb() {
            // Use the grpc 'byte_buffer` interfaces to test we call ref/unref correctly
            // and use `byte_buffer_reader` to check the contents of the slice.
            unsafe {
                let mut data = vec![];
                for _ in 0..10 {
                    data.push(vec_slice(vec![1, 2, 3, 4, 5]));
                }
                assert_eq!(ARENA_MIN.with(|i| i.get()), 0);
                assert_eq!(ARENA_MAX.with(|i| i.get()), 10);
                for datum in data.into_iter().take(5) {
                    let bb = grpc_sys::grpc_raw_byte_buffer_create(datum, 1);
                    let mut reader = MaybeUninit::uninit();
                    grpc_sys::grpc_byte_buffer_reader_init(reader.as_mut_ptr(), bb);
                    let mut reader = reader.assume_init();
                    let mut slice = MaybeUninit::uninit();
                    assert_eq!(
                        grpc_sys::grpc_byte_buffer_reader_next(&mut reader, slice.as_mut_ptr()),
                        1
                    );
                    let slice = slice.assume_init();
                    let bytes = slice.data.refcounted.as_ref().bytes;
                    for i in 0..5 {
                        assert_eq!(*bytes.offset(i), (i + 1) as u8);
                    }
                    grpc_sys::grpc_slice_unref(slice);
                    grpc_sys::grpc_byte_buffer_reader_destroy(&mut reader);
                    grpc_sys::grpc_byte_buffer_destroy(bb);
                }
                assert_eq!(ARENA_MIN.with(|i| i.get()), 5);
                assert_eq!(ARENA_MAX.with(|i| i.get()), 10);
                let mut data = vec![];
                for _ in 0..3 {
                    data.push(vec_slice(vec![1, 2, 3, 4, 5]));
                }
                assert_eq!(ARENA_MIN.with(|i| i.get()), 2);
                assert_eq!(ARENA_MAX.with(|i| i.get()), 10);
                while let Some(datum) = data.pop() {
                    let bb = grpc_sys::grpc_raw_byte_buffer_create(datum, 1);
                    grpc_sys::grpc_byte_buffer_destroy(bb);
                }
                assert_eq!(ARENA_MIN.with(|i| i.get()), 5);
                assert_eq!(ARENA_MAX.with(|i| i.get()), 10);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl MessageWriter {
        fn len(&self) -> usize {
            self.write_buffer.len()
        }
    }

    fn make_message_reader(source: &[u8], n_slice: usize) -> MessageReader {
        unsafe {
            let mut data: Vec<_> = ::std::iter::repeat(source)
                .take(n_slice)
                .map(|s| grpc_sys::grpc_slice_from_copied_buffer(s.as_ptr() as _, s.len()))
                .collect();
            let buf = grpc_sys::grpc_raw_byte_buffer_create(data.as_mut_ptr(), data.len());
            MessageReader::new(buf)
        }
    }

    #[test]
    // Old code crashes under a very weird circumstance, due to a typo in `MessageReader::consume`
    fn test_typo_len_offset() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
        // half of the size of `data`
        const HALF_SIZE: usize = 4;
        let mut reader = make_message_reader(&data, 1);
        assert_eq!(reader.remaining, data.len());
        // first 3 elements of `data`
        let mut buf = [0; HALF_SIZE];
        reader.read(&mut buf).unwrap();
        assert_eq!(data[..HALF_SIZE], buf);
        reader.read(&mut buf).unwrap();
        assert_eq!(data[HALF_SIZE..], buf);
    }

    #[test]
    fn test_message_reader() {
        for len in 0..1024 + 1 {
            for n_slice in 1..4 {
                let source = vec![len as u8; len];
                let expect = vec![len as u8; len * n_slice];
                // Test read.
                let mut reader = make_message_reader(&source, n_slice);
                let mut dest = [0; 7];
                let amt = reader.read(&mut dest).unwrap();

                assert_eq!(
                    dest[..amt],
                    expect[..amt],
                    "len: {}, nslice: {}",
                    len,
                    n_slice
                );

                // Read after move.
                let mut box_reader = Box::new(reader);
                let amt = box_reader.read(&mut dest).unwrap();
                assert_eq!(
                    dest[..amt],
                    expect[..amt],
                    "len: {}, nslice: {}",
                    len,
                    n_slice
                );

                // Test read_to_end.
                let mut reader = make_message_reader(&source, n_slice);
                let mut dest = vec![];
                reader.read_to_end(&mut dest).unwrap();
                assert_eq!(dest, expect, "len: {}, nslice: {}", len, n_slice);

                assert_eq!(0, reader.remaining);
                assert_eq!(0, reader.read(&mut [1]).unwrap())
            }
        }
    }

    #[cfg(feature = "prost-codec")]
    #[test]
    fn test_buf_impl() {
        for len in 0..1024 + 1 {
            for n_slice in 1..4 {
                let source = vec![len as u8; len];

                let mut reader = make_message_reader(&source, n_slice);

                let mut remaining = len * n_slice;
                let mut count = 100;
                while reader.remaining() > 0 {
                    assert_eq!(remaining, reader.remaining());
                    let bytes = Buf::bytes(&reader);
                    bytes.iter().for_each(|b| assert_eq!(*b, len as u8));
                    let mut read = bytes.len();
                    // We don't have to advance by the whole amount we read.
                    if read > 5 && len % 2 == 0 {
                        read -= 5;
                    }
                    reader.advance(read);
                    remaining -= read;
                    count -= 1;
                    assert!(count > 0);
                }

                assert_eq!(0, remaining);
                assert_eq!(0, reader.remaining());
            }
        }
    }

    #[test]
    fn msg_writer_reserve_flush_clear() {
        let mut writer = MessageWriter::new();
        assert_eq!(writer.len(), 0);
        let bytes = writer.reserve(3);
        bytes[2] = 42;
        assert_eq!(writer.len(), 3);
        writer.clear();
        assert_eq!(writer.len(), 0);
    }

    #[test]
    fn msg_writer_multi_write() {
        let mut writer = MessageWriter::new();
        assert_eq!(writer.len(), 0);
        let bytes = writer.reserve(3);
        bytes[0] = 42;
        let bytes = writer.reserve(3);
        bytes[2] = 255;
        let bytes = writer.reserve(2);
        bytes[1] = 0;
        assert_eq!(writer.len(), 8);
    }

    #[cfg(feature = "prost-codec")]
    #[test]
    fn msg_writer_buf_mut() {
        let writer = &mut MessageWriter::new();
        assert_eq!(writer.len(), 0);
        writer.reserve(10);
        unsafe {
            let mut buf: MessageWriterBuf = writer.into();
            assert_eq!(buf.remaining_mut(), 10);
            let bytes = buf.bytes_mut();
            bytes[0] = 4;
            bytes[3] = 42;
            buf.advance_mut(3);
            assert_eq!(buf.remaining_mut(), 7);
            assert_eq!(buf.bytes_mut()[0], 42);
        }
    }
}
