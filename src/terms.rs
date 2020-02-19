use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::hash::Hash;

use tantivy::{DocId, Result, Score, Searcher};
use tantivy::fastfield::{
    FastFieldNotAvailableError,
    FastFieldReader,
    MultiValueIntFastFieldReader,
};
use tantivy::schema::Field;

use crate::agg::{Agg, AggSegmentContext, PreparedAgg, SegmentAgg};

macro_rules! impl_terms_agg_for_type {
    ( $type:ty, $reader_fn:ident : $agg_fn:ident, $agg_struct:ident, $prepared_agg_struct:ident, $segment_agg_struct:ident ) => {

pub struct $agg_struct<SubAgg>
where
    SubAgg: Agg,
{
    field: Field,
    sub_agg: SubAgg,
}

pub fn $agg_fn<SubAgg>(field: Field, sub_agg: SubAgg) -> $agg_struct<SubAgg>
where
    SubAgg: Agg,
{
    $agg_struct {
        field,
        sub_agg,
    }
}

impl<SubAgg> Agg for $agg_struct<SubAgg>
where
    SubAgg: Agg,
    <SubAgg as Agg>::Child: PreparedAgg,
{
    type Fruit = TermsAggResult<$type, SubAgg::Fruit>;
    type Child = $prepared_agg_struct<SubAgg::Child>;

    fn prepare(&self, searcher: &Searcher) -> Result<Self::Child> {
        Ok(Self::Child {
            field: self.field,
            sub_agg: self.sub_agg.prepare(searcher)?,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }
}

pub struct $prepared_agg_struct<SubAgg>
where
    SubAgg: PreparedAgg,
{
    field: Field,
    sub_agg: SubAgg,
}

impl<SubAgg> PreparedAgg for $prepared_agg_struct<SubAgg>
where
    SubAgg: PreparedAgg,
{
    type Fruit = TermsAggResult<$type, SubAgg::Fruit>;
    type Child = $segment_agg_struct<SubAgg::Child>;

    fn for_segment(&self, ctx: &AggSegmentContext) -> Result<Self::Child> {
        let ff_reader = ctx.reader.fast_fields().$reader_fn(self.field)
            .ok_or_else(|| {
                FastFieldNotAvailableError::new(
                    ctx.reader.schema().get_field_entry(self.field)
                )
            })?;
        Ok(Self::Child::new(ff_reader, self.sub_agg.for_segment(ctx)?))
    }

    fn merge(&self, harvest: &mut Self::Fruit, fruit: &Self::Fruit) {
        for (key, bucket) in fruit.res.iter() {
            let existing_bucket = harvest.res.entry(*key)
                .or_insert_with(|| self.sub_agg.create_fruit());

            self.sub_agg.merge(existing_bucket, bucket);
        }
    }
}

    };
    ( SINGLE $(|$type:ty, $reader_fn:ident : $agg_fn:ident, $agg_struct:ident, $prepared_agg_struct:ident, $segment_agg_struct:ident|),+ ) => { $(

impl_terms_agg_for_type!($type, $reader_fn : $agg_fn, $agg_struct, $prepared_agg_struct, $segment_agg_struct);

pub struct $segment_agg_struct<SubAgg>
where
    SubAgg: SegmentAgg,
{
    ff_reader: FastFieldReader<$type>,
    sub_agg: SubAgg,
}

impl<SubAgg> $segment_agg_struct<SubAgg>
where
    SubAgg: SegmentAgg,
{
    fn new(ff_reader: FastFieldReader<$type>, sub_agg: SubAgg) -> Self {
        Self { ff_reader, sub_agg }
    }
}

impl<SubAgg> SegmentAgg for $segment_agg_struct<SubAgg>
where
    SubAgg: SegmentAgg,
{
    type Fruit = TermsAggResult<$type, SubAgg::Fruit>;

    fn collect(&mut self, doc: DocId, score: Score, agg_value: &mut Self::Fruit) {
        let key = self.ff_reader.get(doc);
        let bucket = agg_value.res.entry(key)
            .or_insert_with(|| self.sub_agg.create_fruit());
        self.sub_agg.collect(doc, score, bucket);
    }
}

    )* };
    ( MULTI $(|$type:ty, $reader_fn:ident : $agg_fn:ident, $agg_struct:ident, $prepared_agg_struct:ident, $segment_agg_struct:ident|),+ ) => { $(

impl_terms_agg_for_type!($type, $reader_fn : $agg_fn, $agg_struct, $prepared_agg_struct, $segment_agg_struct);

pub struct $segment_agg_struct<SubAgg>
where
    SubAgg: SegmentAgg,
{
    ff_reader: MultiValueIntFastFieldReader<$type>,
    sub_agg: SubAgg,
    vals: Vec<$type>,
}

impl<SubAgg> $segment_agg_struct<SubAgg>
where
    SubAgg: SegmentAgg,
{
    fn new(ff_reader: MultiValueIntFastFieldReader<$type>, sub_agg: SubAgg) -> Self {
        Self {
            ff_reader,
            sub_agg,
            vals: vec!(),
        }
    }
}

impl<SubAgg> SegmentAgg for $segment_agg_struct<SubAgg>
where
    SubAgg: SegmentAgg,
{
    type Fruit = TermsAggResult<$type, SubAgg::Fruit>;

    fn collect(&mut self, doc: DocId, score: Score, agg_value: &mut Self::Fruit) {
        self.ff_reader.get_vals(doc, &mut self.vals);
        for &key in self.vals.iter() {
            let bucket = agg_value.res.entry(key)
                .or_insert_with(|| self.sub_agg.create_fruit());
            self.sub_agg.collect(doc, score, bucket);
        }
    }
}

    )* };
}

impl_terms_agg_for_type!(
    SINGLE
    |u64, u64 : terms_agg_u64, TermsAggU64, PreparedTermsAggU64, TermsSegmentAggU64|,
    |i64, i64 : terms_agg_i64, TermsAggI64, PreparedTermsAggI64, TermsSegmentAggI64|
);

impl_terms_agg_for_type!(
    MULTI
    |u64, u64s : terms_agg_u64s, TermsAggU64s, PreparedTermsAggU64s, TermsSegmentAggU64s|,
    |i64, i64s : terms_agg_i64s, TermsAggI64s, PreparedTermsAggI64s, TermsSegmentAggI64s|
);

macro_rules! impl_filtered_terms_agg_for_type {
    ( $type:ty, $reader_fn:ident : $agg_fn:ident, $agg_struct:ident, $prepared_agg_struct:ident, $segment_agg_struct:ident ) => {

pub struct $agg_struct<F, SubAgg>
where
    F: Fn($type) -> bool,
    SubAgg: Agg,
{
    field: Field,
    sub_agg: SubAgg,
    filter: F,
}

pub fn $agg_fn<F, SubAgg>(field: Field, sub_agg: SubAgg, filter: F) -> $agg_struct<F, SubAgg>
where
    F: Fn($type) -> bool,
    SubAgg: Agg,
{
    $agg_struct {
        field,
        filter,
        sub_agg,
    }
}

impl<F, SubAgg> Agg for $agg_struct<F, SubAgg>
where
    F: Fn($type) -> bool + Sync + Copy,
    SubAgg: Agg,
    <SubAgg as Agg>::Child: PreparedAgg,
{
    type Fruit = TermsAggResult<$type, SubAgg::Fruit>;
    type Child = $prepared_agg_struct<F, SubAgg::Child>;

    fn prepare(&self, searcher: &Searcher) -> Result<Self::Child> {
        Ok(Self::Child {
            field: self.field,
            filter: self.filter,
            sub_agg: self.sub_agg.prepare(searcher)?,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }
}

pub struct $prepared_agg_struct<F, SubAgg>
where
    F: Fn($type) -> bool,
    SubAgg: PreparedAgg,
{
    field: Field,
    filter: F,
    sub_agg: SubAgg,
}

impl<F, SubAgg> PreparedAgg for $prepared_agg_struct<F, SubAgg>
where
    F: Fn($type) -> bool + Sync + Copy,
    SubAgg: PreparedAgg,
{
    type Fruit = TermsAggResult<$type, SubAgg::Fruit>;
    type Child = $segment_agg_struct<F, SubAgg::Child>;

    fn for_segment(&self, ctx: &AggSegmentContext) -> Result<Self::Child> {
        let ff_reader = ctx.reader.fast_fields().$reader_fn(self.field)
            .ok_or_else(|| {
                FastFieldNotAvailableError::new(
                    ctx.reader.schema().get_field_entry(self.field)
                )
            })?;
        Ok(Self::Child::new(ff_reader, self.sub_agg.for_segment(ctx)?, self.filter))
    }

    fn merge(&self, harvest: &mut Self::Fruit, fruit: &Self::Fruit) {
        for (key, bucket) in fruit.res.iter() {
            let existing_bucket = harvest.res.entry(*key)
                .or_insert_with(|| self.sub_agg.create_fruit());

            self.sub_agg.merge(existing_bucket, bucket);
        }
    }
}

    };
    ( SINGLE $(|$type:ty, $reader_fn:ident : $agg_fn:ident, $agg_struct:ident, $prepared_agg_struct:ident, $segment_agg_struct:ident|),+ ) => { $(

impl_filtered_terms_agg_for_type!($type, $reader_fn : $agg_fn, $agg_struct, $prepared_agg_struct, $segment_agg_struct);

pub struct $segment_agg_struct<F, SubAgg>
where
    F: Fn($type) -> bool,
    SubAgg: SegmentAgg,
{
    ff_reader: FastFieldReader<$type>,
    filter: F,
    sub_agg: SubAgg,
}

impl<F, SubAgg> $segment_agg_struct<F, SubAgg>
where
    F: Fn($type) -> bool,
    SubAgg: SegmentAgg,
{
    fn new(ff_reader: FastFieldReader<$type>, sub_agg: SubAgg, filter: F) -> Self {
        Self { ff_reader, filter, sub_agg }
    }
}

impl<F, SubAgg> SegmentAgg for $segment_agg_struct<F, SubAgg>
where
    F: Fn($type) -> bool,
    SubAgg: SegmentAgg,
{
    type Fruit = TermsAggResult<$type, SubAgg::Fruit>;

    fn collect(&mut self, doc: DocId, score: Score, agg_value: &mut Self::Fruit) {
        let key = self.ff_reader.get(doc);
        if !(self.filter)(key) {
            return;
        }
        let bucket = agg_value.res.entry(key)
            .or_insert_with(|| self.sub_agg.create_fruit());
        self.sub_agg.collect(doc, score, bucket);
    }
}

    )* };
    ( MULTI $(|$type:ty, $reader_fn:ident : $agg_fn:ident, $agg_struct:ident, $prepared_agg_struct:ident, $segment_agg_struct:ident|),+ ) => { $(

impl_filtered_terms_agg_for_type!($type, $reader_fn : $agg_fn, $agg_struct, $prepared_agg_struct, $segment_agg_struct);

pub struct $segment_agg_struct<F, SubAgg>
where
    F: Fn($type) -> bool,
    SubAgg: SegmentAgg,
{
    ff_reader: MultiValueIntFastFieldReader<$type>,
    filter: F,
    sub_agg: SubAgg,
    vals: Vec<$type>,
}

impl<F, SubAgg> $segment_agg_struct<F, SubAgg>
where
    F: Fn($type) -> bool,
    SubAgg: SegmentAgg,
{
    fn new(ff_reader: MultiValueIntFastFieldReader<$type>, sub_agg: SubAgg, filter: F) -> Self {
        Self {
            ff_reader,
            filter,
            sub_agg,
            vals: vec!(),
        }
    }
}

impl<F, SubAgg> SegmentAgg for $segment_agg_struct<F, SubAgg>
where
    F: Fn($type) -> bool,
    SubAgg: SegmentAgg,
{
    type Fruit = TermsAggResult<$type, SubAgg::Fruit>;

    fn collect(&mut self, doc: DocId, score: Score, agg_value: &mut Self::Fruit) {
        self.ff_reader.get_vals(doc, &mut self.vals);
        for &key in self.vals.iter() {
            if !(self.filter)(key) {
                continue;
            }
            let bucket = agg_value.res.entry(key)
                .or_insert_with(|| self.sub_agg.create_fruit());
            self.sub_agg.collect(doc, score, bucket);
        }
    }
}

    )* };
}

impl_filtered_terms_agg_for_type!(
    SINGLE
    |u64, u64 : filtered_terms_agg_u64, FilteredTermsAggU64, PreparedFilteredTermsAggU64, FilteredTermsSegmentAggU64|,
    |i64, i64 : filtered_terms_agg_i64, FilteredTermsAggI64, PreparedFilteredTermsAggI64, FilteredTermsSegmentAggI64|
);

impl_filtered_terms_agg_for_type!(
    MULTI
    |u64, u64s : filtered_terms_agg_u64s, FilteredTermsAggU64s, PreparedFilteredTermsAggU64s, FilteredTermsSegmentAggU64s|,
    |i64, i64s : filtered_terms_agg_i64s, FilteredTermsAggI64s, PreparedFilteredTermsAggI64s, FilteredTermsSegmentAggI64s|
);

#[derive(Default, Debug)]
pub struct TermsAggResult<K, T>
where
    K: Eq + Hash,
{
    res: HashMap<K, T>,
}

impl<T, K> TermsAggResult<K, T>
where
    K: Eq + Hash + Ord,
{
    pub fn get(&self, key: &K) -> Option<&T> {
        self.res.get(key)
    }

    pub fn top_k<'a, F, U>(&'a self, k: usize, mut sort_by: F) -> Vec<(&'a K, &'a T)>
    where
        F: FnMut(&'a T) -> U,
        U: Copy + Ord,
    {
        if self.res.is_empty() || k == 0 {
            return vec!();
        }

        let mut heap = BinaryHeap::with_capacity(k);
        let mut it = self.res.iter();

        for (key, facet) in (&mut it).take(k) {
            heap.push((Reverse(sort_by(facet)), key));
        }

        let mut lowest = (heap.peek().unwrap().0).0;

        for (key, facet) in it {
            let sort_value = sort_by(facet);
            if sort_value > lowest {
                if let Some(mut head) = heap.peek_mut() {
                    *head = (Reverse(sort_value), key);
                }
                lowest = (heap.peek().unwrap().0).0;
            }
        }

        heap.into_sorted_vec()
            .into_iter()
            .map(|(_, key)| (key, self.get(key).unwrap()))
            .collect::<Vec<_>>()
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Reverse;

    use tantivy::Result;
    use tantivy::query::AllQuery;

    use test_fixtures::ProductIndex;

    use crate::metric::{count_agg, min_agg_f64};
    use crate::searcher::AggSearcher;
    use super::{filtered_terms_agg_u64, terms_agg_u64};

    #[test]
    fn test_empty_terms_agg() -> Result<()> {
        let product_index = ProductIndex::create_in_ram(3)?;
        let searcher = product_index.reader.searcher();

        let cat_agg = terms_agg_u64(
            product_index.schema.category_id, count_agg()
        );
        let cat_counts = searcher.agg_search(&AllQuery,  &cat_agg)?;
        assert_eq!(
            cat_counts.top_k(10, |b| b),
            vec!()
        );

        Ok(())
    }

    #[test]
    fn test_terms_agg() -> Result<()> {
        let mut product_index = ProductIndex::create_in_ram(3)?;
        product_index.index_test_products()?;

        let searcher = product_index.reader.searcher();

        let cat_agg = terms_agg_u64(
            product_index.schema.category_id,
            (count_agg(), min_agg_f64(product_index.schema.price))
        );
        let cat_counts = searcher.agg_search(&AllQuery,  &cat_agg)?;
        let cat1_bucket = cat_counts.get(&1u64);
        assert_eq!(
            cat1_bucket,
            Some(&(2u64, Some(9.99_f64)))
        );
        let cat2_bucket = cat_counts.get(&2u64);
        assert_eq!(
            cat2_bucket,
            Some(&(3u64, Some(0.5_f64)))
        );

        // Sort terms facet by doc count desc
        assert_eq!(
            cat_counts.top_k(2, |b| b.0),
            vec!(
                (&2u64, &(3u64, Some(0.5_f64))),
                (&1u64, &(2u64, Some(9.99_f64))),
            ),
        );

        // Sort terms facet with minimum min price
        assert_eq!(
            cat_counts.top_k(1, |b| {
                // Floats are hard to sort
                Reverse(b.1.map(|v| v.to_le_bytes()))
            }),
            vec!(
                (&2u64, &(3u64, Some(0.5_f64))),
            ),
        );
        // Sort terms facet with maximum min price
        assert_eq!(
            cat_counts.top_k(1, |b| {
                // Floats are hard to sort
                b.1.map(|v| v.to_le_bytes())
            }),
            vec!(
                (&1u64, &(2u64, Some(9.99_f64))),
            ),
        );

        Ok(())
    }

    #[test]
    fn test_filtered_terms_agg() -> Result<()> {
        let mut product_index = ProductIndex::create_in_ram(3)?;
        product_index.index_test_products()?;

        let searcher = product_index.reader.searcher();

        let even = 0_u64;
        let cat_agg = filtered_terms_agg_u64(
            product_index.schema.category_id,
            (count_agg(), min_agg_f64(product_index.schema.price)),
            |cat_id| cat_id % 2 == even
        );
        let cat_counts = searcher.agg_search(&AllQuery,  &cat_agg)?;
        let cat1_bucket = cat_counts.get(&1u64);
        assert_eq!(
            cat1_bucket,
            None
        );
        let cat2_bucket = cat_counts.get(&2u64);
        assert_eq!(
            cat2_bucket,
            Some(&(3u64, Some(0.5_f64)))
        );

        Ok(())
    }
}
