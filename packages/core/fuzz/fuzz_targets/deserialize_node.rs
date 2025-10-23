#![no_main]

use libfuzzer_sys::fuzz_target;
use sombra::storage::record::{RecordHeader, RecordKind};

fuzz_target!(|data: &[u8]| {
    if data.len() < 8 {
        return;
    }

    let _ = RecordHeader::from_bytes(data);
    
    if data.len() > 8 {
        let header = RecordHeader::new(RecordKind::Node, (data.len() - 8) as u32);
        let mut header_bytes = vec![0u8; 8];
        let _ = header.write_to(&mut header_bytes);
    }
});
