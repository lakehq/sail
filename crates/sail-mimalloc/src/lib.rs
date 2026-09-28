//! Sail's Rust global allocator, backed by mimalloc.

#![no_std]

use core::alloc::{GlobalAlloc, Layout};

use rustfs_mimalloc_sys::{mi_free, mi_malloc_aligned, mi_realloc_aligned, mi_zalloc_aligned};

/// A mimalloc allocator for Rust allocations in the CLI and Python extension.
pub struct MiMalloc;

// SAFETY: mimalloc supports concurrent allocation and cross-thread deallocation.
// Its aligned APIs honor Layout's alignment, return null on allocation failure,
// and preserve the original allocation when reallocation fails.
unsafe impl GlobalAlloc for MiMalloc {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: GlobalAlloc's caller supplies a valid, nonzero allocation layout.
        unsafe { mi_malloc_aligned(layout.size(), layout.align()).cast() }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The aligned zeroing API accepts the same layout as alloc.
        unsafe { mi_zalloc_aligned(layout.size(), layout.align()).cast() }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        // SAFETY: The caller returns a live allocation from this allocator.
        unsafe { mi_free(ptr.cast()) };
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: The caller supplies a live allocation and a valid nonzero new size.
        unsafe { mi_realloc_aligned(ptr.cast(), new_size, layout.align()).cast() }
    }
}
