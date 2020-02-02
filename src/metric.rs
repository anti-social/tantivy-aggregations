use tantivy::{DateTime, DocId, Result, Score, Searcher};
use tantivy::fastfield::{
    FastFieldNotAvailableError,
    FastFieldReader,
    MultiValueIntFastFieldReader,
};
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

macro_rules! impl_min_agg_for_type {
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

    fn for_segment(&self, ctx: &AggSegmentContext) -> Result<Self::Child> {
        let ff_reader = ctx.reader.fast_fields().$reader_fn(self.field)
            .ok_or_else(|| {
                FastFieldNotAvailableError::new(
                    ctx.reader.schema().get_field_entry(self.field)
                )
            })?;
        Ok(Self::Child::new(ff_reader))
    }

    fn merge(&self, acc: &mut Self::Fruit, fruit: &Self::Fruit) {
        match *fruit {
            None => return,
            Some(v) => {
                if let Some(ref mut min_val) = acc {
                    if v < *min_val {
                        *min_val = v;
                    }
                } else {
                    acc.replace(v);
                }
            }
        }
    }
}

    };
    ( SINGLE $(|$type:ty, $reader_fn:ident : $agg_fn:ident, $agg_struct:ident, $prepared_agg_struct:ident, $segment_agg_struct:ident|),+ ) => { $(

impl_min_agg_for_type!($type, $reader_fn : $agg_fn, $agg_struct, $prepared_agg_struct, $segment_agg_struct);

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

    fn collect(&mut self, doc: DocId, _: Score, agg_value: &mut Self::Fruit) {
        let v = self.ff_reader.get(doc);
        if let Some(ref mut min_val) = agg_value {
            if v < *min_val {
                *min_val = v;
            }
        } else {
            agg_value.replace(v);
        }
    }
}

    )* };
    ( MULTI $(|$type:ty, $reader_fn:ident : $agg_fn:ident, $agg_struct:ident, $prepared_agg_struct:ident, $segment_agg_struct:ident|),+ ) => { $(

impl_min_agg_for_type!($type, $reader_fn : $agg_fn, $agg_struct, $prepared_agg_struct, $segment_agg_struct);

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

    fn collect(&mut self, doc: DocId, _: Score, agg_value: &mut Self::Fruit) {
        self.ff_reader.get_vals(doc, &mut self.vals);
        for &v in self.vals.iter() {
            if let Some(ref mut min_val) = agg_value {
                if v < *min_val {
                    *min_val = v;
                }
            } else {
                agg_value.replace(v);
            }
        }
    }
}

    )* };
}

impl_min_agg_for_type!(
    SINGLE
    |u64, u64 : min_agg_u64, MinAggU64, PreparedMinAggU64, MinSegmentAggU64|,
    |i64, i64 : min_agg_i64, MinAggI64, PreparedMinAggI64, MinSegmentAggI64|,
    |f64, f64 : min_agg_f64, MinAggF64, PreparedMinAggF64, MinSegmentAggF64|,
    |DateTime, date: min_agg_date, MinAggDate, PreparedMinAggDate, MinSegmentAggDate|
);

impl_min_agg_for_type!(
    MULTI
    |u64, u64s : min_agg_u64s, MinAggU64s, PreparedMinAggU64s, MinSegmentAggU64s|,
    |i64, i64s : min_agg_i64s, MinAggI64s, PreparedMinAggI64s, MinSegmentAggI64s|,
    |f64, f64s : min_agg_f64s, MinAggF64s, PreparedMinAggF64s, MinSegmentAggF64s|,
    |DateTime, dates: min_agg_dates, MinAggDates, PreparedMinAggDates, MinSegmentAggDates|
);

macro_rules! impl_max_agg_for_type {
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

    fn for_segment(&self, ctx: &AggSegmentContext) -> Result<Self::Child> {
        let ff_reader = ctx.reader.fast_fields().$reader_fn(self.field)
            .ok_or_else(|| {
                FastFieldNotAvailableError::new(
                    ctx.reader.schema().get_field_entry(self.field)
                )
            })?;
        Ok(Self::Child::new(ff_reader))
    }

    fn merge(&self, acc: &mut Self::Fruit, fruit: &Self::Fruit) {
        match *fruit {
            None => return,
            Some(v) => {
                if let Some(ref mut max_val) = acc {
                    if v > *max_val {
                        *max_val = v;
                    }
                } else {
                    acc.replace(v);
                }
            }
        }
    }
}

    };
    ( SINGLE $(|$type:ty, $reader_fn:ident : $agg_fn:ident, $agg_struct:ident, $prepared_agg_struct:ident, $segment_agg_struct:ident|),+ ) => { $(

impl_max_agg_for_type!($type, $reader_fn : $agg_fn, $agg_struct, $prepared_agg_struct, $segment_agg_struct);

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

    fn collect(&mut self, doc: DocId, _: Score, agg_value: &mut Self::Fruit) {
        let v = self.ff_reader.get(doc);
        if let Some(ref mut max_val) = agg_value {
            if v > *max_val {
                *max_val = v;
            }
        } else {
            agg_value.replace(v);
        }
    }
}

    )* };
    ( MULTI $(|$type:ty, $reader_fn:ident : $agg_fn:ident, $agg_struct:ident, $prepared_agg_struct:ident, $segment_agg_struct:ident|),+ ) => { $(

impl_max_agg_for_type!($type, $reader_fn : $agg_fn, $agg_struct, $prepared_agg_struct, $segment_agg_struct);

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

    fn collect(&mut self, doc: DocId, _: Score, agg_value: &mut Self::Fruit) {
        self.ff_reader.get_vals(doc, &mut self.vals);
        for &v in self.vals.iter() {
            if let Some(ref mut max_val) = agg_value {
                if v > *max_val {
                    *max_val = v;
                }
            } else {
                agg_value.replace(v);
            }
        }
    }
}

    )* };
}

impl_max_agg_for_type!(
    SINGLE
    |u64, u64 : max_agg_u64, MaxAggU64, PreparedMaxAggU64, MaxSegmentAggU64|,
    |i64, i64 : max_agg_i64, MaxAggI64, PreparedMaxAggI64, MaxSegmentAggI64|,
    |f64, f64 : max_agg_f64, MaxAggF64, PreparedMaxAggF64, MaxSegmentAggF64|,
    |DateTime, date: max_agg_date, MaxAggDate, PreparedMaxAggDate, MaxSegmentAggDate|
);
impl_max_agg_for_type!(
    MULTI
    |u64, u64s : max_agg_u64s, MaxAggU64s, PreparedMaxAggU64s, MaxSegmentAggU64s|,
    |i64, i64s : max_agg_i64s, MaxAggI64s, PreparedMaxAggI64s, MaxSegmentAggI64s|,
    |f64, f64s : max_agg_f64s, MaxAggF64s, PreparedMaxAggF64s, MaxSegmentAggF64s|,
    |DateTime, dates: max_agg_dates, MaxAggDates, PreparedMaxAggDates, MaxSegmentAggDates|
);

#[cfg(test)]
mod tests {
    use tantivy::chrono::{DateTime, Utc};

    use tantivy::Result;
    use tantivy::query::AllQuery;

    use test_fixtures::ProductIndex;

    use super::count_agg;
    use super::{min_agg_u64, min_agg_u64s, min_agg_f64, min_agg_date};
    use super::{max_agg_u64, max_agg_u64s, max_agg_f64, max_agg_date};
    use crate::searcher::AggSearcher;

    #[test]
    fn test_count() -> Result<()> {
        let product_index = ProductIndex::create_in_ram(3)?;
        let searcher = AggSearcher::from_reader(product_index.reader);

        let agg = count_agg();
        let count = searcher.search(&AllQuery, &agg)?;

        assert_eq!(count, 5);

        Ok(())
    }

    #[test]
    fn test_min() -> Result<()> {
        let product_index = ProductIndex::create_in_ram(3)?;
        let searcher = AggSearcher::from_reader(product_index.reader);

        assert_eq!(
            searcher.search(&AllQuery, &min_agg_u64(product_index.schema.positive_opinion_percent))?,
            Some(71_u64)
        );

        assert_eq!(
            searcher.search(&AllQuery, &min_agg_date(product_index.schema.date_created))?,
            Some(
                DateTime::parse_from_rfc3339("1970-01-01T00:00:00+00:00").unwrap().with_timezone(&Utc)
            )
        );

        assert_eq!(
            searcher.search(&AllQuery, &min_agg_f64(product_index.schema.price))?,
            Some(0.5_f64)
        );

        assert_eq!(
            searcher.search(&AllQuery, &min_agg_u64s(product_index.schema.tag_ids))?,
            Some(111_u64)
        );

        Ok(())
    }

    #[test]
    fn test_max() -> Result<()> {
        let product_index = ProductIndex::create_in_ram(3)?;
        let searcher = AggSearcher::from_reader(product_index.reader);

        assert_eq!(
            searcher.search(&AllQuery, &max_agg_f64(product_index.schema.price))?,
            Some(100.01_f64)
        );

        assert_eq!(
            searcher.search(&AllQuery, &max_agg_u64(product_index.schema.positive_opinion_percent))?,
            Some(100_u64)
        );

        assert_eq!(
            searcher.search(&AllQuery, &max_agg_date(product_index.schema.date_created))?,
            Some(
                DateTime::parse_from_rfc3339("2020-01-01T00:59:59+00:00").unwrap().with_timezone(&Utc)
            )
        );

        assert_eq!(
            searcher.search(&AllQuery, &max_agg_u64s(product_index.schema.tag_ids))?,
            Some(511_u64)
        );

        Ok(())
    }
}