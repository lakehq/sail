use std::alloc::{GlobalAlloc, Layout};
use std::error::Error;
use std::slice;

use sail_mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[test]
fn allocations_honor_size_and_alignment() -> Result<(), Box<dyn Error>> {
    for size in [1, 7, 8, 17, 1024, 1025, 65537, 1 << 20] {
        for alignment in [1, 8, 64, 4096, 1 << 16] {
            let layout = Layout::from_size_align(size, alignment)?;
            // SAFETY: Each pointer is checked, initialized within its layout, and freed once.
            unsafe {
                let ptr = GLOBAL.alloc(layout);
                assert!(!ptr.is_null());
                assert_eq!(ptr.addr() % alignment, 0);
                ptr.write_bytes(0xa5, size);
                assert!(slice::from_raw_parts(ptr, size).iter().all(|&x| x == 0xa5));
                GLOBAL.dealloc(ptr, layout);
            }
        }
    }
    Ok(())
}

#[test]
fn zeroed_allocations_honor_size_and_alignment() -> Result<(), Box<dyn Error>> {
    for size in [1, 17, 1024, 1025, 65537, 1 << 20] {
        for alignment in [1, 8, 64, 4096, 1 << 16] {
            let layout = Layout::from_size_align(size, alignment)?;
            // SAFETY: alloc_zeroed initializes the entire layout; each allocation is freed once.
            unsafe {
                let ptr = GLOBAL.alloc_zeroed(layout);
                assert!(!ptr.is_null());
                assert_eq!(ptr.addr() % alignment, 0);
                assert!(slice::from_raw_parts(ptr, size).iter().all(|&x| x == 0));
                ptr.write_bytes(0xa5, size);
                GLOBAL.dealloc(ptr, layout);
            }
        }
    }
    Ok(())
}

#[test]
fn reallocation_preserves_contents_and_alignment() -> Result<(), Box<dyn Error>> {
    for alignment in [1, 8, 64, 4096, 1 << 16] {
        for (size, new_size) in [(7, 1025), (1025, 7), (1 << 20, 2 << 20), (4096, 4096)] {
            let layout = Layout::from_size_align(size, alignment)?;
            let new_layout = Layout::from_size_align(new_size, alignment)?;
            // SAFETY: Only initialized bytes retained by realloc are read, and deallocation
            // uses the new layout after ownership transfers to the returned pointer.
            unsafe {
                let ptr = GLOBAL.alloc(layout);
                assert!(!ptr.is_null());
                for i in 0..size {
                    ptr.add(i).write((i % 251) as u8);
                }
                let resized = GLOBAL.realloc(ptr, layout, new_size);
                if resized.is_null() {
                    GLOBAL.dealloc(ptr, layout);
                }
                assert!(!resized.is_null());
                assert_eq!(resized.addr() % alignment, 0);
                for i in 0..size.min(new_size) {
                    assert_eq!(resized.add(i).read(), (i % 251) as u8);
                }
                GLOBAL.dealloc(resized, new_layout);
            }
        }
    }
    Ok(())
}

#[test]
fn allocations_outlive_the_allocating_thread() -> Result<(), Box<dyn Error>> {
    #[repr(align(4096))]
    struct Page([u8; 4096]);

    let threads: Vec<_> = (0..4)
        .map(|_| {
            std::thread::spawn(|| {
                let mut pages = Vec::new();
                for byte in 0..64 {
                    pages.push(Box::new(Page([byte; 4096])));
                }
                pages
            })
        })
        .collect();
    for thread in threads {
        let pages = thread.join().map_err(|_| "allocator thread panicked")?;
        for (i, page) in pages.iter().enumerate() {
            assert_eq!(page.0.as_ptr().addr() % 4096, 0);
            assert!(page.0.iter().all(|&x| x == i as u8));
        }
        drop(pages);
    }
    Ok(())
}
