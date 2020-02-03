use tantivy::{DocId, Result, Score, Searcher};
use tantivy::query::Scorer;
use tantivy::schema::Field;

use crate::agg::{Agg, AggSegmentContext, PreparedAgg, SegmentAgg};
use tantivy::fastfield::FastFieldReader;

type HistogramFruit<T> = HistogramAggRes<f64, T>;

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
        let mut min_value = std::f64::INFINITY;
        let mut max_value = std::f64::NEG_INFINITY;
        for segment_reader in searcher.segment_readers() {
            let ff_reader = segment_reader.fast_fields().f64(self.field).unwrap();
            if ff_reader.min_value() < min_value {
                min_value = ff_reader.min_value();
            }
            if ff_reader.max_value() > max_value {
                max_value = ff_reader.max_value();
            }
        }
        Ok(Self::Child {
            field: self.field,
            min_value,
            max_value,
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
    min_value: f64,
    max_value: f64,
    interval: f64,
    sub_agg: SubAgg,
}

impl<'q, SubAgg> PreparedAgg for PreparedHistogramAgg<SubAgg>
where
    SubAgg: PreparedAgg,
{
    type Fruit = HistogramFruit<SubAgg::Fruit>;
    type Child = HistogramSegmentAgg<SubAgg::Child>;

    fn create_fruit(&self) -> Self::Fruit {
//        let mut buckets = vec!();
//        if self.min_value < self.max_value {
//            let mut start_value = self.min_value - self.min_value / self.interval;
//            while start_value < self.max_value {
//                buckets.push((start_value, SubAgg::Fruit::default()));
//                start_value += self.interval;
//            }
//        }
        HistogramFruit {
            buckets: vec!(),
        }
    }

    fn for_segment(&self, ctx: &AggSegmentContext) -> Result<Self::Child> {
        let ff_reader = ctx.reader.fast_fields().f64(self.field).unwrap();
        Ok(Self::Child::new(
            ff_reader,
            self.sub_agg.for_segment(ctx)?,
        ))
    }

    fn merge(&self, harvest: &mut Self::Fruit, fruit: &Self::Fruit) {
//        self.sub_agg.merge(harvest, fruit);
    }

}

pub struct HistogramSegmentAgg<SubAgg>
where
    SubAgg: SegmentAgg,
{
    ff_reader: FastFieldReader<f64>,
    sub_agg: SubAgg,
}

impl<SubAgg> HistogramSegmentAgg<SubAgg>
where
    SubAgg: SegmentAgg,
{
    fn new(ff_reader: FastFieldReader<f64>, sub_agg: SubAgg) -> Self {
        Self {
            ff_reader, sub_agg
        }
    }
}

impl<SubAgg> SegmentAgg for HistogramSegmentAgg<SubAgg>
where
    SubAgg: SegmentAgg,
{
    type Fruit = HistogramFruit<SubAgg::Fruit>;

    fn collect(&mut self, doc: DocId, score: Score, agg_value: &mut Self::Fruit) {
        let key = self.ff_reader.get(doc);

        if agg_value.buckets.is_empty() {
            let val = <SubAgg as SegmentAgg>::Fruit::default();
            agg_value.buckets.push((key, val));
        }

        let mut start_ix = 0;
        let mut end_ix = agg_value.buckets.len() - 1;
        let mut mid_ix: usize;
        loop {
            mid_ix = (end_ix - start_ix + 1) / 2;
            let start_key = agg_value.buckets.get
            let key > start_key
        }
        agg_value.buckets = Vec::with_capacity(agg_value.buckets.len() + 1);
    }
}

#[derive(Default, Debug)]
pub struct HistogramAggRes<K, T>
where
    K: PartialOrd + PartialEq,
    T: Default,
{
    buckets: Vec<(K, T)>
}

//impl<K, T> HistogramAggRes<K, T>
//where
//    K: PartialOrd + PartialEq,
//    T: Default,
//{
//    fn find_bucket(&mut self, key: K) -> () {
//        if self.buckets.is_empty() {
//            let val = T::default();
//            self.buckets.push((key, val));
//            // return &val;
//        }
//        let mid = self.buckets.len() / 2;
//        unimplemented!()
//    }
//}
