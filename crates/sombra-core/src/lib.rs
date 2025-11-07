#![forbid(unsafe_code)]

use sombra_pager::{PageStore, PagerStats};

pub struct Db<P: PageStore> {
    pager: P,
}

impl<P: PageStore> Db<P> {
    pub fn new(pager: P) -> Self {
        Self { pager }
    }

    pub fn pager(&self) -> &P {
        &self.pager
    }

    pub fn stats(&self) -> PagerStats {
        PagerStats::default()
    }
}
