fn init_leaf_root(
    store: &Arc<dyn PageStore>,
    write: &mut WriteGuard<'_>,
    page_id: PageId,
    page_size: usize,
    salt: u64,
) -> Result<()> {
    let mut page = write.page_mut(page_id)?;
    let buf = page.data_mut();
    if buf.len() < page_size {
        return Err(SombraError::Invalid("page buffer shorter than page size"));
    }
    buf[..page_size].fill(0);
    let header = PageHeader::new(
        page_id,
        crate::types::page::PageKind::BTreeLeaf,
        store.page_size(),
        salt,
    )?
    .with_crc32(0);
    header.encode(&mut buf[..PAGE_HDR_LEN])?;
    page::write_initial_header(&mut buf[PAGE_HDR_LEN..page_size], page::BTreePageKind::Leaf)
}

fn meta_salt(store: &Arc<dyn PageStore>) -> Result<u64> {
    let read = store.begin_read()?;
    let meta = store.get_page(&read, PageId(0))?;
    let header = PageHeader::decode(&meta.data()[..PAGE_HDR_LEN])?;
    Ok(header.salt)
}
