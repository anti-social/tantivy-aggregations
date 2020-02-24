use quantiles::ckms::CKMS;

use std::fmt::Debug;
use std::ops::{Add, Div, Sub};

use tantivy::{DocId, Result, Score, Searcher};
use tantivy::fastfield::{FastFieldNotAvailableError, FastFieldReader};
use tantivy::schema::Field;

use crate::agg::{Agg, SegmentAgg, PreparedAgg, AggSegmentContext};

pub struct PercentilesAggF64 {
    field: Field,
}

pub fn percentiles_agg_f64(field: Field) -> PercentilesAggF64 {
    PercentilesAggF64 { field }
}

impl Agg for PercentilesAggF64 {
    type Fruit = Percentiles<f64>;
    type Child = PercentilesPreparedAggF64;

    fn prepare(&self, _: &Searcher) -> Result<Self::Child> {
        Ok(Self::Child { field: self.field })
    }

    fn requires_scoring(&self) -> bool {
        false
    }
}

pub struct PercentilesPreparedAggF64 {
    field: Field
}

impl PreparedAgg for PercentilesPreparedAggF64 {
    type Fruit = Percentiles<f64>;
    type Child = PercentilesSegmentAggF64;

    fn for_segment(&self, ctx: &AggSegmentContext) -> Result<Self::Child> {
        let ff_reader = ctx.reader.fast_fields().f64(self.field)
            .ok_or_else(|| {
                FastFieldNotAvailableError::new(
                    ctx.reader.schema().get_field_entry(self.field)
                )
            })?;
        Ok(Self::Child { ff_reader })
    }

    fn merge(&self, acc: &mut Self::Fruit, fruit: Self::Fruit) {
        for sample in fruit.quantiles.into_vec() {
            acc.quantiles.insert(sample);
        }
    }
}

pub struct PercentilesSegmentAggF64 {
    ff_reader: FastFieldReader<f64>,
}

impl SegmentAgg for PercentilesSegmentAggF64 {
    type Fruit = Percentiles<f64>;

    fn collect(&mut self, doc: DocId, _: Score, fruit: &mut Self::Fruit) {
        let v = self.ff_reader.get(doc);
        fruit.quantiles.insert(v);
    }
}

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
