use tantivy::{DocId, Result as TantivyResult, Score, Searcher};
use tantivy::fastfield::FastFieldNotAvailableError;
use tantivy::schema::Field;

use crate::agg::{Agg, PreparedAgg, SegmentAgg, AggSegmentContext};

pub fn post_filter_agg<FFReaderFetcher, FFReader, Filter, SubAgg>(
    ff_reader_fetcher: FFReaderFetcher, filter: Filter, sub_agg: SubAgg
) -> PostFilterAgg<FFReaderFetcher, FFReader, Filter, SubAgg>
where
    FFReaderFetcher: Fn(&AggSegmentContext) -> Result<FFReader, Field>,
    Filter: Fn(&FFReader, DocId, Score) -> bool,
    SubAgg: Agg,
{
    PostFilterAgg {
        ff_reader_fetcher, filter, sub_agg
    }
}

pub struct PostFilterAgg<FFReaderFetcher, FFReader, Filter, SubAgg>
where
    FFReaderFetcher: Fn(&AggSegmentContext) -> Result<FFReader, Field>,
    Filter: Fn(&FFReader, DocId, Score) -> bool,
    SubAgg: Agg,
{
    ff_reader_fetcher: FFReaderFetcher,
    filter: Filter,
    sub_agg: SubAgg,
}

impl<FFReaderFetcher, FFReader, Filter, SubAgg> Agg for PostFilterAgg<FFReaderFetcher, FFReader, Filter, SubAgg>
where
    FFReaderFetcher: Fn(&AggSegmentContext) -> Result<FFReader, Field> + Sync + Copy,
    Filter: Fn(&FFReader, DocId, Score) -> bool + Sync + Copy,
    SubAgg: Agg,
{
    type Fruit = SubAgg::Fruit;
    type Child = PostFilterPreparedAgg<FFReaderFetcher, FFReader, Filter, SubAgg::Child>;

    fn prepare(&self, searcher: &Searcher) -> TantivyResult<Self::Child> {
        Ok(PostFilterPreparedAgg {
            ff_reader_fetcher: self.ff_reader_fetcher,
            filter: self.filter,
            sub_agg: self.sub_agg.prepare(searcher)?,
        })
    }

    fn requires_scoring(&self) -> bool {
        self.sub_agg.requires_scoring()
    }
}

pub struct PostFilterPreparedAgg<FFReaderFetcher, FFReader, Filter, SubAgg>
where
    FFReaderFetcher: Fn(&AggSegmentContext) -> Result<FFReader, Field>,
    Filter: Fn(&FFReader, DocId, Score) -> bool,
    SubAgg: PreparedAgg,
{
    ff_reader_fetcher: FFReaderFetcher,
    filter: Filter,
    sub_agg: SubAgg,
}

impl<FFReaderFetcher, FFReader, Filter, SubAgg> PreparedAgg for PostFilterPreparedAgg<FFReaderFetcher, FFReader, Filter, SubAgg>
where
    FFReaderFetcher: Fn(&AggSegmentContext) -> Result<FFReader, Field> + Sync,
    Filter: Fn(&FFReader, DocId, Score) -> bool + Sync + Copy,
    SubAgg: PreparedAgg,
{
    type Fruit = SubAgg::Fruit;
    type Child = PostFilterSegmentAgg<FFReader, Filter, SubAgg::Child>;

    fn create_fruit(&self) -> Self::Fruit {
        self.sub_agg.create_fruit()
    }

    fn for_segment(&self, ctx: &AggSegmentContext) -> TantivyResult<Self::Child> {
        let ff_reader = (self.ff_reader_fetcher)(ctx)
            .map_err(|f| FastFieldNotAvailableError::new(ctx.reader.schema().get_field_entry(f)))?;
        Ok(Self::Child::new(self.filter, ff_reader, self.sub_agg.for_segment(ctx)?))
    }

    fn merge(&self, acc: &mut Self::Fruit, fruit: Self::Fruit) {
        self.sub_agg.merge(acc, fruit);
    }
}

pub struct PostFilterSegmentAgg<FFReader, Filter, SubAgg>
where
    Filter: Fn(&FFReader, DocId, Score) -> bool,
    SubAgg: SegmentAgg,
{
    filter: Filter,
    ff_reader: FFReader,
    sub_agg: SubAgg,
}

impl<FFReader, Filter, SubAgg> PostFilterSegmentAgg<FFReader, Filter, SubAgg>
where
    Filter: Fn(&FFReader, DocId, Score) -> bool,
    SubAgg: SegmentAgg,
{
    fn new(filter: Filter, ff_reader: FFReader, sub_agg: SubAgg) -> Self {
        Self { filter, ff_reader, sub_agg }
    }
}

impl<FFReader, Filter, SubAgg> SegmentAgg for PostFilterSegmentAgg<FFReader, Filter, SubAgg>
where
    Filter: Fn(&FFReader, DocId, Score) -> bool,
    SubAgg: SegmentAgg,
{
    type Fruit = SubAgg::Fruit;

    fn create_fruit(&self) -> Self::Fruit {
        self.sub_agg.create_fruit()
    }

    fn collect(&mut self, doc: DocId, score: Score, fruit: &mut Self::Fruit) {
        if (self.filter)(&self.ff_reader, doc, score) {
            self.sub_agg.collect(doc, score, fruit);
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::Result as TantivyResult;
    use tantivy::query::AllQuery;

    use test_fixtures::ProductIndex;

    use crate::{AggSearcher, count_agg};
    use super::post_filter_agg;

    #[test]
    fn test_post_filter_agg() -> TantivyResult<()> {
        let mut product_index = ProductIndex::create_in_ram(3)?;
        product_index.index_test_products()?;

        let searcher = product_index.reader.searcher();

        let mut min_price = 5.0;
        let count_min_price_agg = post_filter_agg(
            |ctx| {
                ctx.reader.fast_fields().f64(product_index.schema.price)
                    .ok_or(product_index.schema.price)
            },
            move |ff, doc, _| ff.get(doc) > min_price,
            count_agg()
        );
        assert_eq!(searcher.agg_search(&AllQuery, &count_min_price_agg)?, 4);

        min_price = 10.0;
        let category_id = 1_u64;
        let count_min_price_agg = post_filter_agg(
            |ctx| {
                Ok((
                    ctx.reader.fast_fields().f64(product_index.schema.price)
                        .ok_or(product_index.schema.price)?,
                    ctx.reader.fast_fields().u64(product_index.schema.category_id)
                        .ok_or(product_index.schema.category_id)?,
                ))
            },
            move |ff, doc, _| {
                ff.0.get(doc) >= min_price && ff.1.get(doc) == category_id
            },
            count_agg()
        );
        assert_eq!(searcher.agg_search(&AllQuery, &count_min_price_agg)?, 1);

        Ok(())
    }
}
