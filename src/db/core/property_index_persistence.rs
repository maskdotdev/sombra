use crate::error::{GraphError, Result};
use crate::model::NodeId;
use crate::pager::{PageId, Pager};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use tracing::{debug, info};

use super::graphdb::IndexableValue;

const PROPERTY_INDEX_MAGIC: &[u8; 4] = b"PIDX";
const PROPERTY_INDEX_VERSION: u16 = 1;

type PropertyIndexMap = HashMap<(String, String), BTreeMap<IndexableValue, BTreeSet<NodeId>>>;
type IndexEntry = BTreeMap<IndexableValue, BTreeSet<NodeId>>;

pub struct PropertyIndexSerializer<'a> {
    pager: &'a mut Pager,
}

impl<'a> PropertyIndexSerializer<'a> {
    pub fn new(pager: &'a mut Pager) -> Self {
        Self { pager }
    }

    pub fn serialize_indexes(&mut self, indexes: &PropertyIndexMap) -> Result<(PageId, u32, Vec<PageId>)> {
        if indexes.is_empty() {
            return Ok((0, 0, Vec::new()));
        }

        let mut serialized_data = Vec::new();

        serialized_data.extend_from_slice(PROPERTY_INDEX_MAGIC);
        serialized_data.extend_from_slice(&PROPERTY_INDEX_VERSION.to_le_bytes());

        let count = indexes.len() as u32;
        serialized_data.extend_from_slice(&count.to_le_bytes());

        for ((label, property_key), index) in indexes {
            self.serialize_single_index(&mut serialized_data, label, property_key, index)?;
        }

        let (root_page, written_pages) = self.write_serialized_data(&serialized_data)?;

        info!(
            count = indexes.len(),
            root_page,
            size_bytes = serialized_data.len(),
            "Serialized property indexes"
        );

        Ok((root_page, count, written_pages))
    }

    pub fn collect_old_pages(&mut self, root_page: PageId) -> Result<Vec<PageId>> {
        let mut pages = Vec::new();
        let mut current_page = root_page;

        while current_page != 0 {
            pages.push(current_page);

            let next_page = self.pager.with_page(current_page, |page_data| {
                let next =
                    u32::from_le_bytes([page_data[4], page_data[5], page_data[6], page_data[7]]);
                Ok(next)
            })?;

            current_page = next_page;
        }

        Ok(pages)
    }

    fn serialize_single_index(
        &self,
        data: &mut Vec<u8>,
        label: &str,
        property_key: &str,
        index: &IndexEntry,
    ) -> Result<()> {
        let label_bytes = label.as_bytes();
        data.extend_from_slice(&(label_bytes.len() as u32).to_le_bytes());
        data.extend_from_slice(label_bytes);

        let property_key_bytes = property_key.as_bytes();
        data.extend_from_slice(&(property_key_bytes.len() as u32).to_le_bytes());
        data.extend_from_slice(property_key_bytes);

        let entry_count = index.len() as u32;
        data.extend_from_slice(&entry_count.to_le_bytes());

        for (indexable_value, node_ids) in index {
            self.serialize_indexable_value(data, indexable_value)?;

            let node_count = node_ids.len() as u32;
            data.extend_from_slice(&node_count.to_le_bytes());

            for node_id in node_ids {
                data.extend_from_slice(&node_id.to_le_bytes());
            }
        }

        Ok(())
    }

    fn serialize_indexable_value(&self, data: &mut Vec<u8>, value: &IndexableValue) -> Result<()> {
        match value {
            IndexableValue::Bool(b) => {
                data.push(1);
                data.push(if *b { 1 } else { 0 });
            }
            IndexableValue::Int(i) => {
                data.push(2);
                data.extend_from_slice(&i.to_le_bytes());
            }
            IndexableValue::String(s) => {
                data.push(3);
                let s_bytes = s.as_bytes();
                data.extend_from_slice(&(s_bytes.len() as u32).to_le_bytes());
                data.extend_from_slice(s_bytes);
            }
        }
        Ok(())
    }

    fn write_serialized_data(&mut self, data: &[u8]) -> Result<(PageId, Vec<PageId>)> {
        let page_size = self.pager.page_size();
        // Page layout: [4 bytes chunk_len][4 bytes next_page][data]
        // The pager automatically reserves last 4 bytes for checksum
        // Usable space = (page_size - 4 checksum) - 8 (header) = page_size - 12
        let chunk_size = page_size - 12;
        let pages_needed = data.chunks(chunk_size).count();

        if pages_needed == 0 {
            return Err(GraphError::Corruption(
                "no pages needed for property indexes".into(),
            ));
        }

        let mut pages = Vec::new();
        for _i in 0..pages_needed {
            let page_id = self.pager.allocate_page()?;
            pages.push(page_id);
        }

        for (i, chunk) in data.chunks(chunk_size).enumerate() {
            let page_id = pages[i];
            let next_page = if i + 1 < pages.len() { pages[i + 1] } else { 0 };

            let chunk_len = chunk.len() as u32;
            let page = self.pager.fetch_page(page_id)?;
            page.data[0..4].copy_from_slice(&chunk_len.to_le_bytes());
            page.data[4..8].copy_from_slice(&next_page.to_le_bytes());
            page.data[8..8 + chunk.len()].copy_from_slice(chunk);
            page.dirty = true;
        }

        Ok((pages[0], pages))
    }

    pub fn deserialize_indexes(&mut self, root_page: PageId) -> Result<PropertyIndexMap> {
        if root_page == 0 {
            return Ok(HashMap::new());
        }

        let serialized_data = self.read_serialized_data(root_page)?;

        if serialized_data.len() < 10 {
            return Err(GraphError::Corruption(
                "property index data too short".into(),
            ));
        }

        if &serialized_data[0..4] != PROPERTY_INDEX_MAGIC {
            return Err(GraphError::Corruption(
                "invalid property index magic".into(),
            ));
        }

        let version = u16::from_le_bytes([serialized_data[4], serialized_data[5]]);
        if version != PROPERTY_INDEX_VERSION {
            return Err(GraphError::Corruption(format!(
                "unsupported property index version {version}"
            )));
        }

        let count = u32::from_le_bytes([
            serialized_data[6],
            serialized_data[7],
            serialized_data[8],
            serialized_data[9],
        ]);

        let mut indexes = HashMap::new();
        let mut offset = 10;

        for _i in 0..count {
            let (key, index, new_offset) =
                self.deserialize_single_index(&serialized_data, offset)?;
            indexes.insert(key, index);
            offset = new_offset;
        }

        info!(count = indexes.len(), "Deserialized property indexes");

        Ok(indexes)
    }

    fn deserialize_single_index(
        &self,
        data: &[u8],
        mut offset: usize,
    ) -> Result<((String, String), IndexEntry, usize)> {
        if offset + 4 > data.len() {
            return Err(GraphError::Corruption(
                "truncated property index label".into(),
            ));
        }

        let label_len = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + label_len > data.len() {
            return Err(GraphError::Corruption(
                "truncated property index label data".into(),
            ));
        }

        let label = String::from_utf8(data[offset..offset + label_len].to_vec())
            .map_err(|_| GraphError::Corruption("invalid UTF-8 in label".into()))?;
        offset += label_len;

        if offset + 4 > data.len() {
            return Err(GraphError::Corruption("truncated property key".into()));
        }

        let property_key_len = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + property_key_len > data.len() {
            return Err(GraphError::Corruption("truncated property key data".into()));
        }

        let property_key = String::from_utf8(data[offset..offset + property_key_len].to_vec())
            .map_err(|_| GraphError::Corruption("invalid UTF-8 in property key".into()))?;
        offset += property_key_len;

        if offset + 4 > data.len() {
            return Err(GraphError::Corruption("truncated entry count".into()));
        }

        let entry_count = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        offset += 4;

        let mut index = BTreeMap::new();

        for _entry_idx in 0..entry_count {
            let (indexable_value, new_offset) = self.deserialize_indexable_value(data, offset)?;
            offset = new_offset;

            if offset + 4 > data.len() {
                return Err(GraphError::Corruption("truncated node count".into()));
            }

            let node_count = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            offset += 4;

            if label == "Record" && property_key == "active" {
                eprintln!(
                    "Deserializing value {:?} with node_count {} at offset {}",
                    indexable_value, node_count, offset
                );
            }

            let mut node_ids = BTreeSet::new();
            let mut prev_id: Option<u64> = None;
            for _ in 0..node_count {
                if offset + 8 > data.len() {
                    return Err(GraphError::Corruption("truncated node id".into()));
                }

                let node_id = u64::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                if label == "Record" && property_key == "active" {
                    if let Some(prev) = prev_id {
                        if node_id <= prev {
                            eprintln!(
                                "Non-increasing node id detected: prev={} current={} at offset {}",
                                prev, node_id, offset
                            );
                        }
                    }
                    prev_id = Some(node_id);
                }
                node_ids.insert(node_id);
            }

            index.insert(indexable_value, node_ids);
        }

        Ok(((label, property_key), index, offset))
    }

    fn deserialize_indexable_value(
        &self,
        data: &[u8],
        mut offset: usize,
    ) -> Result<(IndexableValue, usize)> {
        if offset >= data.len() {
            return Err(GraphError::Corruption(
                "truncated indexable value type".into(),
            ));
        }

        let value_type = data[offset];
        offset += 1;

        match value_type {
            1 => {
                if offset >= data.len() {
                    return Err(GraphError::Corruption("truncated bool value".into()));
                }
                let b = data[offset] != 0;
                offset += 1;
                Ok((IndexableValue::Bool(b), offset))
            }
            2 => {
                if offset + 8 > data.len() {
                    return Err(GraphError::Corruption("truncated int value".into()));
                }
                let i = i64::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Ok((IndexableValue::Int(i), offset))
            }
            3 => {
                if offset + 4 > data.len() {
                    return Err(GraphError::Corruption("truncated string length".into()));
                }
                let str_len = u32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]) as usize;
                offset += 4;

                if offset + str_len > data.len() {
                    return Err(GraphError::Corruption("truncated string data".into()));
                }

                let s = String::from_utf8(data[offset..offset + str_len].to_vec())
                    .map_err(|_| GraphError::Corruption("invalid UTF-8 in string value".into()))?;
                offset += str_len;
                Ok((IndexableValue::String(s), offset))
            }
            _ => Err(GraphError::Corruption(format!(
                "invalid indexable value type {value_type}"
            ))),
        }
    }

    fn read_serialized_data(&mut self, root_page: PageId) -> Result<Vec<u8>> {
        let page_size = self.pager.page_size();
        // Page layout: [4 bytes chunk_len][4 bytes next_page][data]
        // The pager automatically reserves last 4 bytes for checksum
        // Max chunk size = (page_size - 4 checksum) - 8 (header) = page_size - 12
        let max_chunk_size = page_size - 12;
        let mut result = Vec::new();
        let mut current_page = root_page;

        let mut page_count = 0;

        loop {
            let mut chunk_len = 0u32;
            let mut next_page = 0u32;
            let mut chunk_data = Vec::new();

            self.pager.with_page(current_page, |page_data| {
                chunk_len =
                    u32::from_le_bytes([page_data[0], page_data[1], page_data[2], page_data[3]]);

                if chunk_len as usize > max_chunk_size {
                    return Err(GraphError::Corruption(
                        "invalid chunk length in property index page".into(),
                    ));
                }

                chunk_data.extend_from_slice(&page_data[8..8 + chunk_len as usize]);

                next_page =
                    u32::from_le_bytes([page_data[4], page_data[5], page_data[6], page_data[7]]);

                Ok(())
            })?;

            page_count += 1;

            result.extend_from_slice(&chunk_data);

            if next_page == 0 {
                break;
            }

            current_page = next_page;
        }

        debug!(
            pages_read = page_count,
            total_bytes = result.len(),
            "Read property index data"
        );

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pager::Pager;
    use tempfile::NamedTempFile;

    fn create_pager_with_reserved_header() -> (NamedTempFile, Pager) {
        let tmp = NamedTempFile::new().expect("temp file");
        let mut pager = Pager::open(tmp.path()).expect("open pager");
        let header_page = pager.allocate_page().expect("allocate header page");
        assert_eq!(
            header_page, 0,
            "First page should be 0 (reserved for header)"
        );
        (tmp, pager)
    }

    #[test]
    fn test_empty_index_serialization() {
        let tmp = NamedTempFile::new().expect("temp file");
        let mut pager = Pager::open(tmp.path()).expect("open pager");
        let mut serializer = PropertyIndexSerializer::new(&mut pager);

        let indexes = PropertyIndexMap::new();
        let (root_page, count, _pages) = serializer.serialize_indexes(&indexes).expect("serialize");

        assert_eq!(root_page, 0);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_single_index_round_trip() {
        let (_tmp, mut pager) = create_pager_with_reserved_header();

        let root_page = {
            let mut serializer = PropertyIndexSerializer::new(&mut pager);
            let mut indexes = PropertyIndexMap::new();
            let mut index = BTreeMap::new();

            let mut node_set = BTreeSet::new();
            node_set.insert(1);
            node_set.insert(2);
            node_set.insert(3);
            index.insert(IndexableValue::Int(42), node_set);

            let mut node_set2 = BTreeSet::new();
            node_set2.insert(4);
            node_set2.insert(5);
            index.insert(IndexableValue::String("test".to_string()), node_set2);

            indexes.insert(("User".to_string(), "age".to_string()), index);

            let (root_page, count, _pages) = serializer.serialize_indexes(&indexes).expect("serialize");
            assert_eq!(count, 1);
            root_page
        };

        {
            let mut serializer = PropertyIndexSerializer::new(&mut pager);
            let deserialized = serializer
                .deserialize_indexes(root_page)
                .expect("deserialize");

            assert_eq!(deserialized.len(), 1);

            let index = deserialized
                .get(&("User".to_string(), "age".to_string()))
                .expect("index exists");

            assert_eq!(index.len(), 2);

            let nodes = index.get(&IndexableValue::Int(42)).expect("int value");
            assert_eq!(nodes.len(), 3);
            assert!(nodes.contains(&1));
            assert!(nodes.contains(&2));
            assert!(nodes.contains(&3));

            let nodes2 = index
                .get(&IndexableValue::String("test".to_string()))
                .expect("string value");
            assert_eq!(nodes2.len(), 2);
            assert!(nodes2.contains(&4));
            assert!(nodes2.contains(&5));
        }
    }

    #[test]
    fn test_multiple_indexes_round_trip() {
        let (_tmp, mut pager) = create_pager_with_reserved_header();

        let root_page = {
            let mut serializer = PropertyIndexSerializer::new(&mut pager);
            let mut indexes = PropertyIndexMap::new();

            let mut index1 = BTreeMap::new();
            let mut node_set1 = BTreeSet::new();
            node_set1.insert(1);
            node_set1.insert(2);
            index1.insert(IndexableValue::Int(30), node_set1);
            indexes.insert(("User".to_string(), "age".to_string()), index1);

            let mut index2 = BTreeMap::new();
            let mut node_set2 = BTreeSet::new();
            node_set2.insert(1);
            index2.insert(IndexableValue::String("John".to_string()), node_set2);
            indexes.insert(("User".to_string(), "name".to_string()), index2);

            let mut index3 = BTreeMap::new();
            let mut node_set3 = BTreeSet::new();
            node_set3.insert(3);
            index3.insert(IndexableValue::Bool(true), node_set3);
            indexes.insert(("Post".to_string(), "published".to_string()), index3);

            let (root_page, count, _pages) = serializer.serialize_indexes(&indexes).expect("serialize");
            assert_eq!(count, 3);
            root_page
        };

        {
            let mut serializer = PropertyIndexSerializer::new(&mut pager);
            let deserialized = serializer
                .deserialize_indexes(root_page)
                .expect("deserialize");

            assert_eq!(deserialized.len(), 3);
            assert!(deserialized.contains_key(&("User".to_string(), "age".to_string())));
            assert!(deserialized.contains_key(&("User".to_string(), "name".to_string())));
            assert!(deserialized.contains_key(&("Post".to_string(), "published".to_string())));
        }
    }

    #[test]
    fn test_large_index_round_trip() {
        let (_tmp, mut pager) = create_pager_with_reserved_header();

        let root_page = {
            let mut serializer = PropertyIndexSerializer::new(&mut pager);
            let mut indexes = PropertyIndexMap::new();
            let mut index = BTreeMap::new();

            for i in 0..1000 {
                let mut node_set = BTreeSet::new();
                node_set.insert(i);
                index.insert(IndexableValue::Int(i as i64), node_set);
            }

            indexes.insert(("User".to_string(), "id".to_string()), index);

            let (root_page, count, _pages) = serializer.serialize_indexes(&indexes).expect("serialize");
            assert_eq!(count, 1);
            root_page
        };

        {
            let mut serializer = PropertyIndexSerializer::new(&mut pager);
            let deserialized = serializer
                .deserialize_indexes(root_page)
                .expect("deserialize");

            let index = deserialized
                .get(&("User".to_string(), "id".to_string()))
                .expect("index exists");
            assert_eq!(index.len(), 1000);

            for i in 0..1000 {
                let nodes = index
                    .get(&IndexableValue::Int(i as i64))
                    .expect(&format!("value {} exists", i));
                assert_eq!(nodes.len(), 1);
                assert!(nodes.contains(&i));
            }
        }
    }

    #[test]
    fn test_all_value_types() {
        let (_tmp, mut pager) = create_pager_with_reserved_header();

        let root_page = {
            let mut serializer = PropertyIndexSerializer::new(&mut pager);
            let mut indexes = PropertyIndexMap::new();
            let mut index = BTreeMap::new();

            let mut bool_set = BTreeSet::new();
            bool_set.insert(1);
            index.insert(IndexableValue::Bool(true), bool_set.clone());
            index.insert(IndexableValue::Bool(false), bool_set.clone());

            let mut int_set = BTreeSet::new();
            int_set.insert(2);
            index.insert(IndexableValue::Int(0), int_set.clone());
            index.insert(IndexableValue::Int(-999), int_set.clone());
            index.insert(IndexableValue::Int(999), int_set.clone());

            let mut str_set = BTreeSet::new();
            str_set.insert(3);
            index.insert(IndexableValue::String("".to_string()), str_set.clone());
            index.insert(IndexableValue::String("short".to_string()), str_set.clone());
            index.insert(
                IndexableValue::String(
                    "a very long string with lots of characters to test serialization".to_string(),
                ),
                str_set.clone(),
            );

            indexes.insert(("Test".to_string(), "prop".to_string()), index);

            let (root_page, _, _pages) = serializer.serialize_indexes(&indexes).expect("serialize");
            root_page
        };

        {
            let mut serializer = PropertyIndexSerializer::new(&mut pager);
            let deserialized = serializer
                .deserialize_indexes(root_page)
                .expect("deserialize");

            let index = deserialized
                .get(&("Test".to_string(), "prop".to_string()))
                .expect("index exists");

            assert!(index.contains_key(&IndexableValue::Bool(true)));
            assert!(index.contains_key(&IndexableValue::Bool(false)));
            assert!(index.contains_key(&IndexableValue::Int(0)));
            assert!(index.contains_key(&IndexableValue::Int(-999)));
            assert!(index.contains_key(&IndexableValue::Int(999)));
            assert!(index.contains_key(&IndexableValue::String("".to_string())));
            assert!(index.contains_key(&IndexableValue::String("short".to_string())));
            assert!(index.contains_key(&IndexableValue::String(
                "a very long string with lots of characters to test serialization".to_string()
            )));
        }
    }

    #[test]
    fn test_deserialize_empty_returns_empty() {
        let tmp = NamedTempFile::new().expect("temp file");
        let mut pager = Pager::open(tmp.path()).expect("open pager");
        let mut serializer = PropertyIndexSerializer::new(&mut pager);

        let deserialized = serializer.deserialize_indexes(0).expect("deserialize");
        assert_eq!(deserialized.len(), 0);
    }
}
