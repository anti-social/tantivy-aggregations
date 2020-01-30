use tantivy::{Result, DocId, Score, Searcher, SkipResult};
use tantivy::query::{Query, Weight, Scorer};

use crate::agg::{Agg, AggSegmentContext, PreparedAgg, SegmentAgg};

pub struct FilterAgg<'q, SubAgg>
where
    SubAgg: Agg,
{
    query: &'q dyn Query,
    sub_agg: SubAgg,
}

pub fn filter_agg<'q, SubAgg>(query: &'q dyn Query, sub_agg: SubAgg) -> FilterAgg<SubAgg>
where
    SubAgg: Agg,
{
    FilterAgg {
        query,
        sub_agg,
    }
}

impl<'q, SubAgg> Agg for FilterAgg<'q, SubAgg>
    where
        SubAgg: Agg,
        <SubAgg as Agg>::Child: PreparedAgg,
{
    type Fruit = SubAgg::Fruit;
    type Child = PreparedFilterAgg<SubAgg::Child>;

    fn prepare(&self, searcher: &Searcher) -> Result<Self::Child> {
        Ok(PreparedFilterAgg {
            weight: self.query.weight(searcher, false)?,
            sub_agg: self.sub_agg.prepare(searcher)?,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }
}

pub struct PreparedFilterAgg<SubAgg>
where
    SubAgg: PreparedAgg,
{
    weight: Box<dyn Weight>,
    sub_agg: SubAgg,
}

impl<'q, SubAgg> PreparedAgg for PreparedFilterAgg<SubAgg>
where
    SubAgg: PreparedAgg,
{
    type Fruit = SubAgg::Fruit;
    type Child = FilterSegmentAgg<SubAgg::Child>;

    fn for_segment(&self, ctx: &AggSegmentContext) -> Result<Self::Child> {
        let mut scorer = self.weight.scorer(ctx.reader)?;
        let exhausted = !scorer.advance();
        Ok(Self::Child {
            scorer,
            exhausted,
            sub_agg: self.sub_agg.for_segment(ctx)?,
        })
    }

    fn merge(&self, harvest: &mut Self::Fruit, fruit: &Self::Fruit) {
        self.sub_agg.merge(harvest, fruit);
    }

}

pub struct FilterSegmentAgg<SubAgg>
where
    SubAgg: SegmentAgg,
{
    scorer: Box<dyn Scorer>,
    exhausted: bool,
    sub_agg: SubAgg,
}

impl<SubAgg> SegmentAgg for FilterSegmentAgg<SubAgg>
where
    SubAgg: SegmentAgg,
{
    type Fruit = SubAgg::Fruit;

    fn collect(&mut self, doc: DocId, score: Score, agg_value: &mut Self::Fruit) {
        if dbg!(self.exhausted) {
            return;
        }
        if doc == self.scorer.doc() {
            self.sub_agg.collect(doc, score, agg_value);
            return;
        }
        match self.scorer.skip_next(doc) {
            SkipResult::Reached => {
                self.sub_agg.collect(doc, score, agg_value);
            }
            SkipResult::OverStep => {}
            SkipResult::End => {
                self.exhausted = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::{Index, Result, Term};
    use tantivy::directory::RAMDirectory;
    use tantivy::query::{AllQuery, TermQuery, RangeQuery};
    use tantivy::schema::IndexRecordOption;

    use crate::fixtures::{ProductSchema, index_test_products};
    use crate::searcher::AggSearcher;
    use crate::metric::count_agg;
    use super::filter_agg;

    #[test]
    fn test_filter_agg() -> Result<()> {
        let dir = RAMDirectory::create();
        let schema = ProductSchema::create();
        let index = Index::create(dir, schema.schema.clone())?;
        let mut index_writer = index.writer(3_000_000)?;
        index_test_products(&mut index_writer, &schema)?;

        let index_reader = index.reader()?;
        let searcher = AggSearcher::from_reader(index_reader);

        let filter_query = TermQuery::new(
            Term::from_field_u64(schema.category_id, 1_u64),
            IndexRecordOption::Basic
        );
        let agg = filter_agg(&filter_query, count_agg());
        let filtered_agg = searcher.search(&AllQuery, &agg)?;
        assert_eq!(
            filtered_agg, 2_u64
        );

        let filter_query = TermQuery::new(
            Term::from_field_u64(schema.category_id, 2_u64),
            IndexRecordOption::Basic
        );
        let agg = filter_agg(&filter_query, count_agg());
        let filtered_agg = searcher.search(
            &RangeQuery::new_f64(schema.price, 100_f64..200_f64),
            &agg
        )?;
        assert_eq!(
            filtered_agg, 1_u64
        );

        Ok(())
    }
}