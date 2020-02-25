use tantivy::{DocId, Result, Score, Searcher};

use crate::agg::{Agg, PreparedAgg, SegmentAgg, AggSegmentContext};

pub struct CountAgg;

pub fn count_agg() -> CountAgg {
    CountAgg {}
}

impl Agg for CountAgg {
    type Fruit = u64;
    type Child = PreparedCountAgg;

    fn prepare(&self, _: &Searcher) -> Result<Self::Child> {
        Ok(Self::Child {})
    }

    fn requires_scoring(&self) -> bool {
        false
    }

}

pub struct PreparedCountAgg;

impl PreparedAgg for PreparedCountAgg {
    type Fruit = u64;
    type Child = CountSegmentAgg;

    fn for_segment(&self, _: &AggSegmentContext) -> Result<Self::Child> {
        Ok(Self::Child {})
    }

    fn merge(&self, acc: &mut Self::Fruit, other: Self::Fruit) {
        *acc += other
    }
}

pub struct CountSegmentAgg;

impl SegmentAgg for CountSegmentAgg {
    type Fruit = u64;

    fn collect(&mut self, _: DocId, _: Score, agg_value: &mut Self::Fruit) {
        *agg_value += 1;
    }
}

#[cfg(test)]
mod tests {
    use tantivy::Result;
    use tantivy::query::AllQuery;

    use test_fixtures::ProductIndex;

    use super::count_agg;
    use crate::AggSearcher;

    #[test]
    fn test_count() -> Result<()> {
        let mut product_index = ProductIndex::create_in_ram(3)?;
        product_index.index_test_products()?;
        let searcher = product_index.reader.searcher();

        let agg = count_agg();
        let count = searcher.agg_search(&AllQuery, &agg)?;

        assert_eq!(count, 5);

        Ok(())
    }
}
