use quantiles::ckms::CKMS;

use std::fmt::Debug;
use std::ops::{Add, Div, Sub};

use tantivy::{DocId, Result, Score, Searcher};
use tantivy::fastfield::{FastFieldNotAvailableError, FastFieldReader, MultiValueIntFastFieldReader};
use tantivy::schema::Field;

use crate::agg::{Agg, SegmentAgg, PreparedAgg, AggSegmentContext};

macro_rules! impl_percentiles_agg_for_type {
    ( $type:ty, $reader_fn:ident : $agg_fn:ident, $agg_struct:ident, $prepared_agg_struct:ident, $segment_agg_struct:ident ) => {

pub struct $agg_struct {
    field: Field,
}

pub fn $agg_fn(field: Field) -> $agg_struct {
    $agg_struct { field }
}

impl Agg for $agg_struct {
    type Fruit = Percentiles<$type>;
    type Child = $prepared_agg_struct;

    fn prepare(&self, _: &Searcher) -> Result<Self::Child> {
        Ok(Self::Child { field: self.field })
    }

    fn requires_scoring(&self) -> bool {
        false
    }
}

pub struct $prepared_agg_struct {
    field: Field,
}

impl PreparedAgg for $prepared_agg_struct {
    type Fruit = Percentiles<$type>;
    type Child = $segment_agg_struct;

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
        for sample in fruit.quantiles.into_vec() {
            acc.quantiles.insert(sample);
        }
    }
}

    };
    ( SINGLE $(|$type:ty, $reader_fn:ident : $agg_fn:ident, $agg_struct:ident, $prepared_agg_struct:ident, $segment_agg_struct:ident|),+ ) => { $(

impl_percentiles_agg_for_type!($type, $reader_fn : $agg_fn, $agg_struct, $prepared_agg_struct, $segment_agg_struct);

pub struct $segment_agg_struct {
    ff_reader: FastFieldReader<$type>,
}

impl $segment_agg_struct {
    fn new(ff_reader: FastFieldReader<$type>) -> Self {
        Self { ff_reader }
    }
}

impl SegmentAgg for $segment_agg_struct {
    type Fruit = Percentiles<$type>;

    fn collect(&mut self, doc: DocId, _: Score, fruit: &mut Self::Fruit) {
        let v = self.ff_reader.get(doc);
        fruit.quantiles.insert(v);
    }
}

    )* };
    ( MULTI $(|$type:ty, $reader_fn:ident : $agg_fn:ident, $agg_struct:ident, $prepared_agg_struct:ident, $segment_agg_struct:ident|),+ ) => { $(

impl_percentiles_agg_for_type!($type, $reader_fn : $agg_fn, $agg_struct, $prepared_agg_struct, $segment_agg_struct);

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
    type Fruit = Percentiles<$type>;

    fn collect(&mut self, doc: DocId, _: Score, fruit: &mut Self::Fruit) {
        self.ff_reader.get_vals(doc, &mut self.vals);
        for &v in self.vals.iter() {
            fruit.quantiles.insert(v);
        }
    }
}

    )* };
}

impl_percentiles_agg_for_type!(
    SINGLE
    |f64, f64 : percentiles_agg_f64, PercentilesAggF64, PercentilesPreparedAggF64, PercentilesSegmentAggF64|
);

impl_percentiles_agg_for_type!(
    MULTI
    |f64, f64s : percentiles_agg_f64s, PercentilesAggF64s, PercentilesPreparedAggF64s, PercentilesSegmentAggF64s|
);

pub trait PercentileValue<T>:
    Into<f64> +
    Add<Output = T> +
    Sub<Output = T> +
    Div<Output = T> +
    PartialOrd +
    Copy +
    Send +
    Debug {}

impl PercentileValue<f64> for f64 {}

pub struct Percentiles<T>
where
    T: PercentileValue<T>,
{
    quantiles: CKMS<T>,
}

impl<T> Percentiles<T>
where
    T: PercentileValue<T>,
{
    pub fn percentile(&self, q: f64) -> Option<T>{
        self.quantiles.query(q).map(|p| p.1)
    }
}

impl<T> Default for Percentiles<T>
where
    T: PercentileValue<T>,
{
    fn default() -> Self {
        Self {
            quantiles: CKMS::new(0.01_f64),
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::Result;
    use tantivy::query::AllQuery;

    use test_fixtures::ProductIndex;

    use crate::AggSearcher;
    use super::percentiles_agg_f64;

    #[test]
    fn test_percentiles_agg() -> Result<()> {
        let mut product_index = ProductIndex::create_in_ram(3)?;
        product_index.index_test_products()?;
        let searcher = product_index.reader.searcher();

        let price_percentiles = searcher.agg_search(
            &AllQuery,
            &percentiles_agg_f64(product_index.schema.price),
        )?;
        assert_eq!(
            price_percentiles.percentile(0.5),
            Some(10.0)
        );
        assert_eq!(
            price_percentiles.percentile(0.33),
            Some(9.99)
        );
        assert_eq!(
            price_percentiles.percentile(0.7),
            Some(50.0)
        );
        assert_eq!(
            price_percentiles.percentile(0.01),
            Some(0.5)
        );
        assert_eq!(
            price_percentiles.percentile(0.99),
            Some(100.01)
        );

        Ok(())
    }
}
