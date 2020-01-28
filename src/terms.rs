use std::collections::HashMap;
use std::marker::PhantomData;

use tantivy::{SegmentLocalId, SegmentReader, DocId, Score, Result};
use tantivy::schema::Field;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::fastfield::FastFieldReader;


#[derive(Default)]
pub struct TermCounts<T: Default> {
    facet_counts: HashMap<u64, TermBucket<T>>,
}

#[derive(Default)]
pub struct TermBucket<T: Default> {
    count: u64,
    sub: Option<T>,
}

trait Bucket {
    fn update(&mut self, other: &Self);
}

impl<T: Default> Bucket for TermBucket<T> {
    fn update(&mut self, other: &Self) {
        self.count += other.count;
    }
}

/// Terms aggregation for u64 fast field
pub struct TermsAgg<T: Default, SubAgg> {
    field: Field,
    sub_agg: Option<SubAgg>,
    _marker: PhantomData<T>,
}

impl<T: Default, SubAgg> TermsAgg<T, SubAgg> {
    /// Creates a new terms aggregation for aggregating a given field.
    pub fn for_field(field: Field) -> TermsAgg<(), ()> {
        TermsAgg {
            field,
            sub_agg: None,
            _marker: PhantomData
        }
    }
}

impl<T: Default + Send + Sync + 'static, SubAgg: /*Collector +*/ Sync> Collector for TermsAgg<T, SubAgg> {
    type Fruit = TermCounts<T>;

    type Child = TermsSegmentCollector<T>;

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

    fn merge_fruits(&self, fruits: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        let mut total_counters = HashMap::new();
        for counters in fruits {
            for (key, bucket) in counters.facet_counts {
                total_counters.entry(key).or_insert(TermBucket::default()).update(&bucket);
            }
        }
        Ok(Self::Fruit {
            facet_counts: total_counters,
        })
    }
}

pub struct TermsSegmentCollector<T: Default> {
    counters: HashMap<u64, TermBucket<T>>,
    ff_reader: FastFieldReader<u64>,
}

impl<T: Default + Send + Sync + 'static> SegmentCollector for TermsSegmentCollector<T> {
    type Fruit = TermCounts<T>;

    fn collect(&mut self, doc: DocId, _: Score) {
        let key = self.ff_reader.get(doc);
        let bucket = self.counters.entry(key).or_insert(TermBucket::default());
        bucket.count += 1;
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
    use super::{TermsAgg, TermBucket, TermCounts};

    #[test]
    fn terms_agg() -> Result<()> {
        let dir = RAMDirectory::create();
        let schema = ProductSchema::create();
        let index = Index::create(dir, schema.schema.clone())?;
        let mut index_writer = index.writer(3_000_000)?;
        index_test_products(&mut index_writer, &schema)?;

        let index_reader = index.reader()?;
        let searcher = index_reader.searcher();
        let cat_agg = TermsAgg::<TermBucket<()>, ()>::for_field(schema.category_id);
        let cat_counts: TermCounts<()> = searcher.search(&AllQuery,  &cat_agg)?;
        assert_eq!(
            cat_counts.facet_counts.get(&1u64).map(|b| b.count),
            Some(2u64)
        );
        assert_eq!(
            cat_counts.facet_counts.get(&2u64).map(|b| b.count),
            Some(3u64)
        );

        Ok(())
    }
}
