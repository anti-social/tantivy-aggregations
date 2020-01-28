use tantivy::{SegmentLocalId, SegmentReader, DocId, Score, Result};
use tantivy::schema::Field;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::fastfield::FastFieldReader;

/// Min aggregation for f64 fast field
pub struct MinAgg {
    field: Field,
}

impl MinAgg {
    /// Creates a new terms aggregation for aggregating a given field.
    pub fn for_field(field: Field) -> MinAgg {
        MinAgg {
            field,
        }
    }
}

impl Collector for MinAgg {
    type Fruit = Option<f64>;

    type Child = MinSegmentCollector;

    fn for_segment(&self, _: SegmentLocalId, reader: &SegmentReader) -> Result<Self::Child> {
        let ff_reader = reader.fast_fields().f64(self.field)
            .expect("Expect u64 field");

        Ok(Self::Child {
            min_value: None,
            ff_reader,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, fruits: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        let mut min_value = Self::Fruit::default();
        for v in fruits {
            match v {
                None => continue,
                Some(v) => {
                    let cur_min_value = min_value.get_or_insert(v);
                    if v < *cur_min_value {
                        *cur_min_value = v;
                    }
                }
            }
        }
        Ok(min_value)
    }
}

pub struct MinSegmentCollector {
    min_value: Option<f64>,
    ff_reader: FastFieldReader<f64>,
}

impl SegmentCollector for MinSegmentCollector {
    type Fruit = Option<f64>;

    fn collect(&mut self, doc: DocId, _: Score) {
        let v = self.ff_reader.get(doc);
        let min_value = self.min_value.get_or_insert(v);
        if v < *min_value {
            *min_value = v;
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.min_value
    }
}

#[cfg(test)]
mod tests {
    use tantivy::{Index, Result};
    use tantivy::directory::RAMDirectory;
    use tantivy::query::AllQuery;

    use crate::fixtures::{ProductSchema, index_test_products};
    use super::MinAgg;

    #[test]
    fn min_agg() -> Result<()> {
        let dir = RAMDirectory::create();
        let schema = ProductSchema::create();
        let index = Index::create(dir, schema.schema.clone())?;
        let mut index_writer = index.writer(3_000_000)?;
        index_test_products(&mut index_writer, &schema)?;

        let index_reader = index.reader()?;
        let searcher = index_reader.searcher();
        let min_price = searcher.search(&AllQuery, &MinAgg::for_field(schema.price))?;
        assert_eq!(
            min_price,
            Some(0.5_f64)
        );

        Ok(())
    }
}
