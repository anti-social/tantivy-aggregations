use tantivy::{Result, DocId, Score, Searcher};
use tantivy::fastfield::FastFieldReader;
use tantivy::schema::Field;

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

    fn merge(&self, acc: &mut Self::Fruit, other: &Self::Fruit) {
        *acc += *other
    }
}

pub struct CountSegmentAgg;

impl SegmentAgg for CountSegmentAgg {
    type Fruit = u64;

    fn collect(&mut self, _: DocId, _: Score, agg_value: &mut Self::Fruit) {
        *agg_value += 1;
    }
}

pub struct MinAgg {
    field: Field,
}

pub fn min_agg(field: Field) -> MinAgg {
    MinAgg {
        field,
    }
}

impl Agg for MinAgg {
    type Fruit = Option<f64>;
    type Child = PreparedMinAgg;

    fn prepare(&self, _: &Searcher) -> Result<Self::Child> {
        Ok(PreparedMinAgg {
            field: self.field,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }
}

pub struct PreparedMinAgg {
    field: Field,
}

impl PreparedAgg for PreparedMinAgg {
    type Fruit = Option<f64>;
    type Child = MinSegmentAgg;

    fn for_segment(&self, ctx: &AggSegmentContext) -> Result<Self::Child> {
        let ff_reader = ctx.reader.fast_fields().f64(self.field)
            .expect("Expect f64 field");
        Ok(Self::Child {
            ff_reader,
        })
    }

    fn merge(&self, acc: &mut Self::Fruit, fruit: &Self::Fruit) {
        match fruit {
            None => return,
            Some(v) => {
                // TODO: optimize case when acc.is_none() - do not need condition
                let cur_min_value = acc.get_or_insert(*v);
                if *v < *cur_min_value {
                    *cur_min_value = *v;
                }
            }
        }
    }
}

pub struct MinSegmentAgg {
    ff_reader: FastFieldReader<f64>,
}

impl SegmentAgg for MinSegmentAgg {
    type Fruit = Option<f64>;

    fn collect(&mut self, doc: DocId, _: Score, agg_value: &mut Self::Fruit) {
        let v = self.ff_reader.get(doc);
        let min_value = agg_value.get_or_insert(v);
        if v < *min_value {
            *min_value = v;
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::{Index, Result};
    use tantivy::directory::RAMDirectory;
    use tantivy::query::AllQuery;

    use test_fixtures::{ProductSchema, index_test_products};

    use super::{count_agg, min_agg};
    use crate::searcher::AggSearcher;

    #[test]
    fn test_count() -> Result<()> {
        let dir = RAMDirectory::create();
        let schema = ProductSchema::create();
        let index = Index::create(dir, schema.schema.clone())?;
        let mut index_writer = index.writer(3_000_000)?;
        index_test_products(&mut index_writer, &schema)?;

        let index_reader = index.reader()?;
        let searcher = AggSearcher::from_reader(index_reader);

        let agg = count_agg();
        let count = searcher.search(&AllQuery, &agg)?;

        assert_eq!(count, 5);

        Ok(())
    }

    #[test]
    fn test_min() -> Result<()> {
        let dir = RAMDirectory::create();
        let schema = ProductSchema::create();
        let index = Index::create(dir, schema.schema.clone())?;
        let mut index_writer = index.writer(3_000_000)?;
        index_test_products(&mut index_writer, &schema)?;

        let index_reader = index.reader()?;
        let searcher = AggSearcher::from_reader(index_reader);

        let agg = min_agg(schema.price);
        let min_price = searcher.search(&AllQuery, &agg)?;

        assert_eq!(min_price, Some(0.5_f64));

        Ok(())
    }
}