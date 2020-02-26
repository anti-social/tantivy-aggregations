use tantivy::{DocId, Result, Score, Searcher};
use tantivy::fastfield::{
    FastFieldNotAvailableError,
    FastFieldReader,
    MultiValueIntFastFieldReader,
};
use tantivy::schema::Field;

use crate::agg::{Agg, PreparedAgg, SegmentAgg, AggSegmentContext};

macro_rules! impl_sum_agg_for_type {
    ( $type:ty, $reader_fn:ident : $agg_fn:ident, $agg_struct:ident, $prepared_agg_struct:ident, $segment_agg_struct:ident ) => {

pub struct $agg_struct {
    field: Field,
}

pub fn $agg_fn(field: Field) -> $agg_struct {
    $agg_struct { field }
}

impl Agg for $agg_struct {
    type Fruit = Option<$type>;
    type Child = $prepared_agg_struct;

    fn prepare(&self, _: &Searcher) -> Result<Self::Child> {
        Ok(Self::Child {
            field: self.field,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }
}

pub struct $prepared_agg_struct {
    field: Field,
}

impl PreparedAgg for $prepared_agg_struct {
    type Fruit = Option<$type>;
    type Child = $segment_agg_struct;

    fn create_fruit(&self) -> Self::Fruit {
        Default::default()
    }

    fn for_segment(&self, ctx: &AggSegmentContext) -> Result<Self::Child> {
        let ff_reader = ctx.reader.fast_fields().$reader_fn(self.field)
            .ok_or_else(|| {
                FastFieldNotAvailableError::new(
                    ctx.reader.schema().get_field_entry(self.field)
                )
            })?;
        Ok(Self::Child::new(ff_reader))
    }

    fn merge(&self, acc: &mut Self::Fruit, fruit: Self::Fruit) {
        match fruit {
            None => return,
            Some(v) => {
                if let Some(ref mut value) = acc {
                    *value += v;
                } else {
                    acc.replace(v);
                }
            }
        }
    }
}

    };
    ( SINGLE $(|$type:ty, $reader_fn:ident : $agg_fn:ident, $agg_struct:ident, $prepared_agg_struct:ident, $segment_agg_struct:ident|),+ ) => { $(

impl_sum_agg_for_type!($type, $reader_fn : $agg_fn, $agg_struct, $prepared_agg_struct, $segment_agg_struct);

pub struct $segment_agg_struct {
    ff_reader: FastFieldReader<$type>,
}

impl $segment_agg_struct {
    fn new(ff_reader: FastFieldReader<$type>) -> Self {
        Self { ff_reader }
    }
}

impl SegmentAgg for $segment_agg_struct {
    type Fruit = Option<$type>;

    fn create_fruit(&self) -> Self::Fruit {
        Default::default()
    }

    fn collect(&mut self, doc: DocId, _: Score, fruit: &mut Self::Fruit) {
        let v = self.ff_reader.get(doc);
        if let Some(ref mut value) = fruit {
            *value += v;
        } else {
            fruit.replace(v);
        }
    }
}

    )* };
    ( MULTI $(|$type:ty, $reader_fn:ident : $agg_fn:ident, $agg_struct:ident, $prepared_agg_struct:ident, $segment_agg_struct:ident|),+ ) => { $(

impl_sum_agg_for_type!($type, $reader_fn : $agg_fn, $agg_struct, $prepared_agg_struct, $segment_agg_struct);

pub struct $segment_agg_struct {
    ff_reader: MultiValueIntFastFieldReader<$type>,
    vals: Vec<$type>,
}

impl $segment_agg_struct {
    fn new(ff_reader: MultiValueIntFastFieldReader<$type>) -> Self {
        Self {
            ff_reader,
            vals: vec!(),
        }
    }
}

impl SegmentAgg for $segment_agg_struct {
    type Fruit = Option<$type>;

    fn create_fruit(&self) -> Self::Fruit {
        Default::default()
    }

    fn collect(&mut self, doc: DocId, _: Score, fruit: &mut Self::Fruit) {
        self.ff_reader.get_vals(doc, &mut self.vals);
        for &v in self.vals.iter() {
            if let Some(ref mut value) = fruit {
                *value += v;
            } else {
                fruit.replace(v);
            }
        }
    }
}

    )* };
}

impl_sum_agg_for_type!(
    SINGLE
    |u64, u64 : sum_agg_u64, SumAggU64, PreparedSumAggU64, SumSegmentAggU64|,
    |i64, i64 : sum_agg_i64, SumAggI64, PreparedSumAggI64, SumSegmentAggI64|,
    |f64, f64 : sum_agg_f64, SumAggF64, PreparedSumAggF64, SumSegmentAggF64|
);

impl_sum_agg_for_type!(
    MULTI
    |u64, u64s : sum_agg_u64s, SumAggU64s, PreparedSumAggU64s, SumSegmentAggU64s|,
    |i64, i64s : sum_agg_i64s, SumAggI64s, PreparedSumAggI64s, SumSegmentAggI64s|,
    |f64, f64s : sum_agg_f64s, SumAggF64s, PreparedSumAggF64s, SumSegmentAggF64s|
);

#[cfg(test)]
mod tests {
    use tantivy::Result;
    use tantivy::query::AllQuery;

    use test_fixtures::ProductIndex;

    use crate::AggSearcher;
    use super::{sum_agg_f64, sum_agg_u64, sum_agg_u64s};

    #[test]
    fn test_sum() -> Result<()> {
        let mut product_index = ProductIndex::create_in_ram(3)?;
        product_index.index_test_products()?;
        let searcher = product_index.reader.searcher();

        assert_eq!(
            searcher.agg_search(&AllQuery, &sum_agg_u64(product_index.schema.positive_opinion_percent))?,
            Some(437_u64)
        );

        assert_eq!(
            searcher.agg_search(&AllQuery, &sum_agg_f64(product_index.schema.price))?,
            Some(170.5_f64)
        );

        assert_eq!(
            searcher.agg_search(&AllQuery, &sum_agg_u64s(product_index.schema.tag_ids))?,
            Some(2740_u64)
        );

        Ok(())
    }
}
