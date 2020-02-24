use quantiles::ckms::CKMS;

use tantivy::{DocId, Result, Score, Searcher};
use tantivy::fastfield::{FastFieldNotAvailableError, FastFieldReader};
use tantivy::schema::Field;

use crate::agg::{Agg, SegmentAgg, PreparedAgg, AggSegmentContext};

struct PercentilesAgg {
    field: Field,
}

impl Agg for PercentilesAgg {
    type Fruit = Percentiles;
    type Child = PercentilesPreparedAgg;

    fn prepare(&self, _: &Searcher) -> Result<Self::Child> {
        Ok(Self::Child { field: self.field })
    }

    fn requires_scoring(&self) -> bool {
        false
    }
}

struct PercentilesPreparedAgg {
    field: Field
}

impl PreparedAgg for PercentilesPreparedAgg {
    type Fruit = Percentiles;
    type Child = PercentilesSegmentAgg;

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

struct PercentilesSegmentAgg {
    ff_reader: FastFieldReader<f64>,
}

impl SegmentAgg for PercentilesSegmentAgg {
    type Fruit = Percentiles;

    fn collect(&mut self, doc: DocId, _: Score, fruit: &mut Self::Fruit) {
        let v = self.ff_reader.get(doc);
        fruit.quantiles.insert(v);
    }
}

struct Percentiles {
    quantiles: CKMS<f64>,
}

impl Default for Percentiles {
    fn default() -> Self {
        Self {
            quantiles: CKMS::new(0.01_f64),
        }
    }
}

#[cfg(test)]
mod tests {
    use quantiles::ckms::CKMS;

    #[test]
    fn test_percentiles_agg() {
        let mut quantiles = CKMS::<f64>::new(0.01);

        quantiles.insert(1.0_f64);
        quantiles.insert(5.0_f64);
        quantiles.insert(10.0_f64);

        assert_eq!(quantiles.query(0.5), Some((2, 5.0_f64)));
    }
}
