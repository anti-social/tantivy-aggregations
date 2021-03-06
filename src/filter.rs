use std::cmp::Ordering;

use tantivy::{Result, DocId, Score, Searcher, SkipResult};
use tantivy::query::{Query, Scorer, Weight};

use crate::agg::{Agg, AggSegmentContext, PreparedAgg, SegmentAgg};

pub fn filter_agg<'q, SubAgg>(query: &'q dyn Query, sub_agg: SubAgg) -> FilterAgg<SubAgg>
where
    SubAgg: Agg,
{
    FilterAgg {
        query,
        sub_agg,
    }
}

pub struct FilterAgg<'q, SubAgg>
where
    SubAgg: Agg,
{
    query: &'q dyn Query,
    sub_agg: SubAgg,
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

impl<SubAgg> PreparedAgg for PreparedFilterAgg<SubAgg>
where
    SubAgg: PreparedAgg,
{
    type Fruit = SubAgg::Fruit;
    type Child = FilterSegmentAgg<SubAgg::Child>;

    fn create_fruit(&self) -> Self::Fruit {
        self.sub_agg.create_fruit()
    }

    fn for_segment(&self, ctx: &AggSegmentContext) -> Result<Self::Child> {
        let mut scorer = self.weight.scorer(ctx.reader)?;
        let exhausted = !scorer.advance();
        Ok(Self::Child {
            scorer,
            exhausted,
            sub_agg: self.sub_agg.for_segment(ctx)?,
        })
    }

    fn merge(&self, harvest: &mut Self::Fruit, fruit: Self::Fruit) {
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

    fn create_fruit(&self) -> Self::Fruit {
        self.sub_agg.create_fruit()
    }

    fn collect(&mut self, doc: DocId, score: Score, fruit: &mut Self::Fruit) {
        if self.exhausted {
            return;
        }

        match self.scorer.doc().cmp(&doc) {
            Ordering::Equal => {
                self.sub_agg.collect(doc, score, fruit);
            }
            Ordering::Greater => {}
            Ordering::Less => {
                match self.scorer.skip_next(doc) {
                    SkipResult::Reached => {
                        self.sub_agg.collect(doc, score, fruit);
                    }
                    SkipResult::OverStep => {}
                    SkipResult::End => {
                        self.exhausted = true;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::{Result, Term};
    use tantivy::query::{AllQuery, TermQuery, RangeQuery};
    use tantivy::schema::IndexRecordOption;

    use test_fixtures::ProductIndex;

    use crate::{AggSearcher, count_agg};
    use super::filter_agg;

    #[test]
    fn test_filter_agg() -> Result<()> {
        let mut product_index = ProductIndex::create_in_ram(3)?;
        product_index.index_test_products()?;
        let searcher = product_index.reader.searcher();

        let filter_query = TermQuery::new(
            Term::from_field_u64(product_index.schema.category_id, 1_u64),
            IndexRecordOption::Basic
        );
        let agg = filter_agg(&filter_query, count_agg());
        let filtered_agg = searcher.agg_search(&AllQuery, &agg)?;
        assert_eq!(
            filtered_agg, 2_u64
        );

        let filter_query = TermQuery::new(
            Term::from_field_u64(product_index.schema.category_id, 2_u64),
            IndexRecordOption::Basic
        );
        let agg = filter_agg(&filter_query, count_agg());
        let filtered_agg = searcher.agg_search(
            &RangeQuery::new_f64(product_index.schema.price, 100_f64..200_f64),
            &agg
        )?;
        assert_eq!(
            filtered_agg, 1_u64
        );

        Ok(())
    }
}