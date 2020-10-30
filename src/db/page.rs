pub type PageIndex = u64;

#[repr(u8)]
#[derive(Clone)]
pub enum PageType {
    Blank = 0,
    Root = 1,
    FreeList = 2,
    ValueLog = 3,
    Branch = 4
}

#[repr(align(4096))]
#[repr(C)]
#[derive(Clone)]
pub struct PageContent {
    pub data: [u8; 4095 as usize],
    pub page_type: PageType
}

impl PageContent {
    pub(super) fn as_slice<'s>(&'s self) -> &'s [u8] {
        unsafe {
            ::std::slice::from_raw_parts(
                (self as *const PageContent) as *const u8,
                PAGE_SIZE,
            )
        }
    }
    pub(super) fn uninit() -> UninitPage {
        UninitPage(std::mem::MaybeUninit::uninit())
    }
}

pub const PAGE_SIZE: usize = std::mem::size_of::<PageContent>();

#[repr(C)]
struct UninitPage(std::mem::MaybeUninit<PageContent>);

struct MutPageBytes<'a>(libc::iovec, &'a mut std::marker::PhantomData<()>);

#[cfg(target_os="linux")]
impl<'a> rio::AsIoVec for MutPageBytes<'a> {
    fn into_new_iovec(&self) -> libc::iovec {
        self.0
    }
}

#[cfg(target_os="linux")]
impl<'a> rio::AsIoVecMut for MutPageBytes<'a> {}

impl UninitPage {
    pub(super) fn as_page_bytes<'a>(&'a mut self, offset: usize) -> MutPageBytes<'a> {
        assert!(PAGE_SIZE > offset);

        let ptr = self.0.as_mut_ptr() as *mut libc::c_void;
        let ptr = unsafe { ptr.add(offset) };

        let iovec = libc::iovec {
            iov_base: ptr,
            iov_len: PAGE_SIZE - offset,
        };

        MutPageBytes(iovec, &mut std::marker::PhantomData)
    }
    pub(super) unsafe fn assume_init(self) -> PageContent {
        self.0.assume_init()
    }
}


pub struct Page {
    pub content: PageContent,
    index: PageIndex
}

impl Page {
    pub(super) fn new(content: PageContent, index: PageIndex) -> Page {
        Page { content, index }
    }
    pub fn idx(&self) -> PageIndex {
        self.index
    }
}