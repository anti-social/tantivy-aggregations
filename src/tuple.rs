use tantivy::{DocId, Result, Score, Searcher};

use crate::agg::{Agg, AggSegmentContext, SegmentAgg, PreparedAgg};

macro_rules! impl_agg_for_tuple {
    ( $( $a:ident => $n:tt ),+ ) => {

impl<$($a,)*> Agg for ($($a,)*)
where $(
    $a: Agg,
)*
{
    type Fruit = ($($a::Fruit,)*);
    type Child = ($($a::Child,)*);

    fn prepare(&self, searcher: &Searcher) -> Result<Self::Child> {
        Ok(($(
            self.$n.prepare(searcher)?,
        )*))
    }

    fn requires_scoring(&self) -> bool {
        $(self.$n.requires_scoring()) || *
    }
}

impl<$($a,)*> PreparedAgg for ($($a,)*)
where $(
    $a: PreparedAgg,
)*
{
    type Fruit = ($($a::Fruit,)*);
    type Child = ($($a::Child,)*);

    fn create_fruit(&self) -> Self::Fruit {
        ($(self.$n.create_fruit(),)*)
    }

    fn for_segment(&self, ctx: &AggSegmentContext) -> Result<Self::Child> {
        Ok(($(
            self.$n.for_segment(ctx)?,
        )*))
    }

    fn merge(&self, acc: &mut Self::Fruit, fruit: &Self::Fruit) {
        $(
            self.$n.merge(&mut acc.$n, &fruit.$n);
        )*
    }
}

impl<$($a,)*> SegmentAgg for ($($a,)*)
where $(
    $a: SegmentAgg,
)*
{
    type Fruit = ($($a::Fruit,)*);

    fn create_fruit(&self) -> Self::Fruit {
        ($(self.$n.create_fruit(),)*)
    }

    fn collect(&mut self, doc: DocId, score: Score, output: &mut Self::Fruit) {
        $(
            self.$n.collect(doc, score, &mut output.$n);
        )*
    }
}

    };
}

impl_agg_for_tuple!(A1 => 0, A2 => 1);
impl_agg_for_tuple!(A1 => 0, A2 => 1, A3 => 2);
impl_agg_for_tuple!(A1 => 0, A2 => 1, A3 => 2, A4 => 3);
impl_agg_for_tuple!(A1 => 0, A2 => 1, A3 => 2, A4 => 3, A5 => 4);
impl_agg_for_tuple!(A1 => 0, A2 => 1, A3 => 2, A4 => 3, A5 => 4, A6 => 5);
impl_agg_for_tuple!(A1 => 0, A2 => 1, A3 => 2, A4 => 3, A5 => 4, A6 => 5, A7 => 6);
impl_agg_for_tuple!(A1 => 0, A2 => 1, A3 => 2, A4 => 3, A5 => 4, A6 => 5, A7 => 6, A8 => 7);
impl_agg_for_tuple!(A1 => 0, A2 => 1, A3 => 2, A4 => 3, A5 => 4, A6 => 5, A7 => 6, A8 => 7, A9 => 8);
impl_agg_for_tuple!(A1 => 0, A2 => 1, A3 => 2, A4 => 3, A5 => 4, A6 => 5, A7 => 6, A8 => 7, A9 => 8, A10 => 9);

#[cfg(test)]
mod tests {
    use tantivy::Result;
    use tantivy::query::AllQuery;

    use test_fixtures::ProductIndex;

    use crate::AggSearcher;
    use crate::metric::{count_agg, min_agg_f64, max_agg_f64};

    #[test]
    fn test_tuple() -> Result<()> {
        let mut product_index = ProductIndex::create_in_ram(3)?;
        product_index.index_test_products()?;
        let searcher = product_index.reader.searcher();

        assert_eq!(
            searcher.agg_search(
                &AllQuery,
                &(
                    count_agg(),
                    min_agg_f64(product_index.schema.price),
                    max_agg_f64(product_index.schema.price),
                )
            )?,
            (5_u64, Some(0.5_f64), Some(100.01_f64))
        );

        Ok(())
    }
}