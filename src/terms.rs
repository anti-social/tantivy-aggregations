use std::collections::HashMap;

use tantivy::{SegmentLocalId, SegmentReader, DocId, Score, Result};
use tantivy::schema::Field;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::fastfield::FastFieldReader;

pub struct FacetCounts {
    facet_counts: HashMap<u64, u64>,
}

pub struct TermBucket<T> {
    count: u64,
    sub: T,
}

/// Terms aggregation for u64 fast field
pub struct TermsAgg<SubAgg> {
    field: Field,
    sub_agg: Option<SubAgg>,
}

impl<SubAgg> TermsAgg<SubAgg> {
    /// Creates a new terms aggregation for aggregating a given field.
    pub fn for_field(field: Field) -> TermsAgg<SubAgg> {
        TermsAgg {
            field,
            sub_agg: None,
        }
    }
}

impl<SubAgg: /*Collector +*/ Sync> Collector for TermsAgg<SubAgg> {
    type Fruit = FacetCounts;

    type Child = TermsSegmentCollector;

    fn for_segment(&self, _: SegmentLocalId, reader: &SegmentReader) -> Result<Self::Child> {
        let ff_reader = reader.fast_fields().u64(self.field)
            .expect("Expect u64 field");

        Ok(Self::Child {
            counters: HashMap::new(),
            ff_reader,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segments_facet_counts: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        let mut total_counters = HashMap::new();
        for counters in segments_facet_counts {
            for (key, count) in counters.facet_counts {
                *(total_counters.entry(key).or_insert(0)) += count;
            }
        }
        Ok(Self::Fruit {
            facet_counts: total_counters,
        })
    }
}

pub struct TermsSegmentCollector {
    counters: HashMap<u64, u64>,
    ff_reader: FastFieldReader<u64>,
}

impl SegmentCollector for TermsSegmentCollector {
    type Fruit = FacetCounts;

    fn collect(&mut self, doc: DocId, _: Score) {
        let key = self.ff_reader.get(doc);
        let count = self.counters.entry(key).or_insert(0);
        *count += 1;
    }

    fn harvest(self) -> Self::Fruit {
        Self::Fruit {
            facet_counts: self.counters
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::{Index, Result};
    use tantivy::directory::RAMDirectory;
    use tantivy::query::AllQuery;

    use crate::fixtures::{ProductSchema, index_test_products};
    use super::TermsAgg;

    #[test]
    fn terms_agg() -> Result<()> {
        let dir = RAMDirectory::create();
        let schema = ProductSchema::create();
        let index = Index::create(dir, schema.schema.clone())?;
        let mut index_writer = index.writer(3_000_000)?;
        index_test_products(&mut index_writer, &schema)?;

        let index_reader = index.reader()?;
        let searcher = index_reader.searcher();
        let cat_agg = searcher.search(&AllQuery, &TermsAgg::<()>::for_field(schema.category_id))?;
        assert_eq!(
            cat_agg.facet_counts.get(&1u64),
            Some(&2u64)
        );
        assert_eq!(
            cat_agg.facet_counts.get(&2u64),
            Some(&3u64)
        );

        Ok(())
    }
}
