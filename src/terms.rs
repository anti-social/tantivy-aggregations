use std::collections::HashMap;

use tantivy::{DocId, Result, Score, Searcher};
use tantivy::fastfield::FastFieldReader;
use tantivy::schema::Field;

use crate::agg::{Agg, AggSegmentContext, PreparedAgg, SegmentAgg};

pub struct TermsAgg<SubAgg>
where
    SubAgg: Agg,
{
    field: Field,
    sub_agg: SubAgg,
}

pub fn terms_agg<SubAgg>(field: Field, sub_agg: SubAgg) -> TermsAgg<SubAgg>
where
    SubAgg: Agg,
{
    TermsAgg {
        field,
        sub_agg,
    }
}

impl<SubAgg> Agg for TermsAgg<SubAgg>
where
    SubAgg: Agg,
    <SubAgg as Agg>::Child: PreparedAgg,
{
    type Fruit = HashMap<u64, SubAgg::Fruit>;
    type Child = PreparedTermsAgg<SubAgg::Child>;

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

pub struct PreparedTermsAgg<SubAgg>
where
    SubAgg: PreparedAgg,
{
    field: Field,
    sub_agg: SubAgg,
}

impl<SubAgg> PreparedAgg for PreparedTermsAgg<SubAgg>
where
    SubAgg: PreparedAgg,
{
    type Fruit = HashMap<u64, SubAgg::Fruit>;
    type Child = TermsSegmentAgg<SubAgg::Child>;

    fn for_segment(&self, ctx: &AggSegmentContext) -> Result<Self::Child> {
        let ff_reader = ctx.reader.fast_fields().u64(self.field)
            .expect("Expect u64 field");
        Ok(Self::Child {
            ff_reader,
            sub_agg: self.sub_agg.for_segment(ctx)?,
        })
    }

    fn merge(&self, harvest: &mut Self::Fruit, fruit: &Self::Fruit) {
        for (key, bucket) in fruit {
            let existing_bucket = harvest.entry(*key)
                .or_insert(SubAgg::Fruit::default());

            self.sub_agg.merge(existing_bucket, bucket);
        }
    }
}

pub struct TermsSegmentAgg<SubAgg>
where
    SubAgg: SegmentAgg,
{
    ff_reader: FastFieldReader<u64>,
    sub_agg: SubAgg,
}

impl<SubAgg> SegmentAgg for TermsSegmentAgg<SubAgg>
where
    SubAgg: SegmentAgg,
{
    type Fruit = HashMap<u64, SubAgg::Fruit>;

    fn collect(&mut self, doc: DocId, score: Score, agg_value: &mut Self::Fruit) {
        let key = self.ff_reader.get(doc);
        let bucket = agg_value.entry(key)
            .or_insert(<SubAgg as SegmentAgg>::Fruit::default());
        self.sub_agg.collect(doc, score, bucket);
    }
}

#[cfg(test)]
mod tests {
    use tantivy::{Index, Result};
    use tantivy::directory::RAMDirectory;
    use tantivy::query::AllQuery;

    use test_fixtures::{ProductSchema, index_test_products};

    use crate::metric::{count_agg, min_agg};
    use crate::searcher::AggSearcher;
    use super::terms_agg;

    #[test]
    fn test_terms_agg() -> Result<()> {
        let dir = RAMDirectory::create();
        let schema = ProductSchema::create();
        let index = Index::create(dir, schema.schema.clone())?;
        let mut index_writer = index.writer(3_000_000)?;
        index_test_products(&mut index_writer, &schema)?;

        let index_reader = index.reader()?;
        let searcher = AggSearcher::from_reader(index_reader);

        let cat_agg = terms_agg(
            schema.category_id,
            (count_agg(), min_agg(schema.price))
        );
        let cat_counts = searcher.search(&AllQuery,  &cat_agg)?;
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


        Ok(())
    }
}
