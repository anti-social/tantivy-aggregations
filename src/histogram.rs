use std::collections::BTreeMap;

use tantivy::{DocId, Result, Score, Searcher};
use tantivy::fastfield::FastFieldReader;
use tantivy::schema::Field;

use crate::agg::{Agg, AggSegmentContext, PreparedAgg, SegmentAgg};

type HistogramFruit<T> = HistogramAggRes<T>;

pub fn histogram_agg_f64<SubAgg>(field: Field, interval: f64, sub_agg: SubAgg) -> HistogramAgg<SubAgg>
where
    SubAgg: Agg,
{
    HistogramAgg {
        field,
        interval,
        sub_agg,
    }
}

pub struct HistogramAgg<SubAgg>
where
    SubAgg: Agg,
{
    field: Field,
    interval: f64,
    sub_agg: SubAgg,
}

impl<SubAgg> Agg for HistogramAgg<SubAgg>
where
    SubAgg: Agg,
    <SubAgg as Agg>::Child: PreparedAgg,
{
    type Fruit = HistogramFruit<SubAgg::Fruit>;
    type Child = PreparedHistogramAgg<SubAgg::Child>;

    fn prepare(&self, searcher: &Searcher) -> Result<Self::Child> {
        Ok(Self::Child {
            field: self.field,
            interval: self.interval,
            sub_agg: self.sub_agg.prepare(searcher)?,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }
}

pub struct PreparedHistogramAgg<SubAgg>
where
    SubAgg: PreparedAgg,
{
    field: Field,
    interval: f64,
    sub_agg: SubAgg,
}

impl<SubAgg> PreparedAgg for PreparedHistogramAgg<SubAgg>
where
    SubAgg: PreparedAgg,
{
    type Fruit = HistogramFruit<SubAgg::Fruit>;
    type Child = HistogramSegmentAgg<SubAgg::Child>;

    fn create_fruit(&self) -> Self::Fruit {
        HistogramFruit {
            interval: self.interval,
            buckets: BTreeMap::new(),
        }
    }

    fn for_segment(&self, ctx: &AggSegmentContext) -> Result<Self::Child> {
        let ff_reader = ctx.reader.fast_fields().f64(self.field).unwrap();
        Ok(Self::Child::new(
            ff_reader,
            self.interval,
            self.sub_agg.for_segment(ctx)?,
        ))
    }

    fn merge(&self, harvest: &mut Self::Fruit, fruit: Self::Fruit) {
        for (key, bucket) in fruit.buckets {
            let existing_bucket = harvest.buckets.entry(key)
                .or_insert_with(|| self.sub_agg.create_fruit());

            self.sub_agg.merge(existing_bucket, bucket);
        }
    }

}

pub struct HistogramSegmentAgg<SubAgg>
where
    SubAgg: SegmentAgg,
{
    ff_reader: FastFieldReader<f64>,
    interval: f64,
    sub_agg: SubAgg,
//    fruit_factory: Fn() ->
}

impl<SubAgg> HistogramSegmentAgg<SubAgg>
where
    SubAgg: SegmentAgg,
{
    fn new(ff_reader: FastFieldReader<f64>, interval: f64, sub_agg: SubAgg) -> Self {
        Self {
            ff_reader, interval, sub_agg
        }
    }
}

impl<SubAgg> SegmentAgg for HistogramSegmentAgg<SubAgg>
where
    SubAgg: SegmentAgg,
{
    type Fruit = HistogramFruit<SubAgg::Fruit>;

    fn create_fruit(&self) -> Self::Fruit {
        HistogramFruit {
            interval: self.interval,
            buckets: BTreeMap::new(),
        }
    }

    fn collect(&mut self, doc: DocId, score: Score, agg_value: &mut Self::Fruit) {
        let k = self.ff_reader.get(doc);
        if k.is_nan() {
            return;
        }

        let bucket_ord = (k / self.interval).floor() as u64;
        let bucket = agg_value.buckets.entry(bucket_ord)
            .or_insert_with(|| self.sub_agg.create_fruit());

        self.sub_agg.collect(doc, score, bucket);
    }
}

#[derive(Default, Debug)]
pub struct HistogramAggRes<T>
where
    T: Default,
{
    interval: f64,
    buckets: BTreeMap<u64, T>,
}

impl<T> HistogramAggRes<T>
where
    T: Default,
{
    pub fn buckets(&self) -> Vec<(f64, Option<&T>)> {
        let mut res = vec!();
        let mut last_bucket_ord = if let Some(&bucket_ord) = self.buckets.keys().next() {
            bucket_ord
        } else {
            return res;
        };
        for (bucket_ord, agg) in self.buckets.iter() {
            let gap = bucket_ord - last_bucket_ord;
            if gap > 1 {
                for i in 0_u64..(gap - 1) {
                    res.push(((last_bucket_ord + i + 1) as f64 * self.interval, None));
                }
            }
            res.push((*bucket_ord as f64 * self.interval, Some(&agg)));
            last_bucket_ord = *bucket_ord;
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use tantivy::Result;
    use tantivy::query::AllQuery;

    use test_fixtures::ProductIndex;

    use crate::searcher::AggSearcher;
    use crate::metric::count_agg;
    use crate::terms::terms_agg_u64s;
    use super::histogram_agg_f64;

    #[test]
    fn test_histogram_agg() -> Result<()> {
        let mut product_index = ProductIndex::create_in_ram(3_u16)?;
        product_index.index_test_products()?;

        let searcher = product_index.reader.searcher();

        let price_hist_agg = histogram_agg_f64(
            product_index.schema.price, 10.0_f64, count_agg()
        );
        let price_hist = searcher.agg_search(&AllQuery, &price_hist_agg)?;
        assert_eq!(
            price_hist.buckets(),
            vec!(
                (0.0_f64, Some(&2_u64)),
                (10.0_f64, Some(&1_u64)),
                (20.0_f64, None),
                (30.0_f64, None),
                (40.0_f64, None),
                (50.0_f64, Some(&1_u64)),
                (60.0_f64, None),
                (70.0_f64, None),
                (80.0_f64, None),
                (90.0_f64, None),
                (100.0_f64, Some(&1_u64)),
            )
        );

        Ok(())
    }

    #[test]
    fn test_nested_histogram_agg() -> Result<()> {
        let mut product_index = ProductIndex::create_in_ram(3_u16)?;
        product_index.index_test_products()?;
        let searcher = product_index.reader.searcher();

        let price_hist_for_tags_agg = terms_agg_u64s(
            product_index.schema.tag_ids,
            (
                count_agg(),
                histogram_agg_f64(
                    product_index.schema.price, 10.0_f64, count_agg()
                )
            )
        );
        let price_hist_for_tags = searcher.agg_search(&AllQuery, &price_hist_for_tags_agg)?;
        let top_tags = price_hist_for_tags.top_k(3, |b| b.0);
        assert_eq!(top_tags.len(), 3);

        let tag = top_tags[0];
        assert_eq!(
            tag.0, &211_u64
        );
        let tag_price_hist = tag.1;
        assert_eq!(
            tag_price_hist.0, 3_u64
        );
        assert_eq!(
            tag_price_hist.1.buckets(),
            vec!(
                (0.0_f64, Some(&2_u64)),
                (10.0_f64, Some(&1_u64)),
            )
        );

        let tag = top_tags[1];
        assert_eq!(
            tag.0, &111_u64
        );
        let tag_price_hist = tag.1;
        assert_eq!(
            tag_price_hist.0, 2_u64
        );
        assert_eq!(
            tag_price_hist.1.buckets(),
            vec!(
                (0.0_f64, Some(&1_u64)),
                (10.0_f64, Some(&1_u64)),
            )
        );

        let tag = top_tags[2];
        assert_eq!(
            tag.0, &311_u64
        );
        let tag_price_hist = tag.1;
        assert_eq!(
            tag_price_hist.0, 2_u64
        );
        assert_eq!(
            tag_price_hist.1.buckets(),
            vec!(
                (0.0_f64, Some(&1_u64)),
                (10.0_f64, None),
                (20.0_f64, None),
                (30.0_f64, None),
                (40.0_f64, None),
                (50.0_f64, None),
                (60.0_f64, None),
                (70.0_f64, None),
                (80.0_f64, None),
                (90.0_f64, None),
                (100.0_f64, Some(&1_u64)),
            )
        );

        Ok(())
    }
}
