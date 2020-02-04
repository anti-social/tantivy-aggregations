use std::collections::BTreeMap;

use tantivy::{DocId, Result, Score, Searcher};
use tantivy::fastfield::FastFieldReader;
use tantivy::query::Scorer;
use tantivy::schema::Field;

use crate::agg::{Agg, AggSegmentContext, PreparedAgg, SegmentAgg};
use std::cmp::Ordering;

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
            self.interval,
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
    interval: f64,
    sub_agg: SubAgg,
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

    fn collect(&mut self, doc: DocId, score: Score, agg_value: &mut Self::Fruit) {
        let k = self.ff_reader.get(doc);
        if k.is_nan() {
            return;
        }

        let search_res = agg_value.buckets.binary_search_by(|&(start, _)| {
            if start > k {
                Ordering::Greater
            } else if k >= start && k < start + self.interval {
                Ordering::Equal
            } else {
                Ordering::Less
            }
        });
        let ix = match search_res {
            Ok(found_ix) => {
                found_ix
            }
            Err(insert_ix) => {
                dbg!(k);
                dbg!(self.interval);
                agg_value.buckets.insert(
                    insert_ix,
                    (dbg!((k / self.interval).floor() * self.interval), <SubAgg as SegmentAgg>::Fruit::default())
                );
                insert_ix
            }
        };

        self.sub_agg.collect(doc, score, &mut agg_value.buckets[ix].1);
    }
}

#[derive(Default, Debug)]
pub struct HistogramAggRes<K, T>
where
    K: PartialOrd,
    T: Default,
{
    pub buckets: Vec<(K, T)>
}

//#[derive(Default, Debug)]
//pub struct TotalNum<T: PartialEq + PartialOrd>(T);
//
//impl From<f64> for TotalNum<f64> {
//    fn from(n: f64) -> Self {
//        if n.is_nan() {
//            panic!("Not a number");
//        }
//        Self(n)
//    }
//}
//
//impl PartialEq for TotalNum<f64> {
//    fn eq(&self, other: &Self) -> bool {
//        self.0 == other.0
//    }
//}
//
//impl Eq for TotalNum<f64> {}
//
//impl Ord for TotalNum<f64> {
//    fn cmp(&self, other: &Self) -> Ordering {
//        if self.0 < other.0 {
//            Ordering::Less
//        } else if self.0 > other.0 {
//            Ordering::Greater
//        } else {
//            Ordering::Equal
//        }
//    }
//}
//
//impl PartialOrd for TotalNum<f64> {
//    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//        Some(self.cmp(other))
//    }
//}

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

#[cfg(test)]
mod tests {
    use std::cmp::Reverse;

    use tantivy::{Index, Result};
    use tantivy::directory::RAMDirectory;
    use tantivy::query::AllQuery;

    use test_fixtures::{ProductSchema, index_test_products};

    use crate::searcher::AggSearcher;
    use crate::metric::count_agg;
    use super::histogram_agg_f64;

    #[test]
    fn test_histogram_agg() -> Result<()> {
        let dir = RAMDirectory::create();
        let schema = ProductSchema::create();
        let index = Index::create(dir, schema.schema.clone())?;
        let mut index_writer = index.writer(3_000_000)?;
        index_test_products(&mut index_writer, &schema)?;

        let index_reader = index.reader()?;
        let searcher = AggSearcher::from_reader(index_reader);

        let price_hist_agg = histogram_agg_f64(
            schema.price, 10.0_f64, count_agg()
        );
        let price_hist = searcher.search(&AllQuery, &price_hist_agg)?;
        println!("{:?}", price_hist.buckets);
//        let cat1_bucket = cat_counts.get(&1u64);
//        assert_eq!(
//            cat1_bucket,
//            Some(&(2u64, Some(9.99_f64)))
//        );

        Ok(())
    }
}
