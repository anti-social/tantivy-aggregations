use tantivy::{DocId, Result, Score, Searcher};

use crate::agg::{Agg, AggSegmentContext, SegmentAgg, PreparedAgg};

impl<A1, A2> Agg for (A1, A2)
where
    A1: Agg,
    A2: Agg,
{
    type Fruit = (A1::Fruit, A2::Fruit);
    type Child = (A1::Child, A2::Child);

    fn prepare(&self, searcher: &Searcher) -> Result<Self::Child> {
        Ok((
            self.0.prepare(searcher)?,
            self.1.prepare(searcher)?,
        ))
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring() || self.1.requires_scoring()
    }
}

impl<A1, A2> PreparedAgg for (A1, A2)
where
    A1: PreparedAgg,
    A2: PreparedAgg,
{
    type Fruit = (A1::Fruit, A2::Fruit);
    type Child = (A1::Child, A2::Child);

    fn for_segment(&self, ctx: &AggSegmentContext) -> Result<Self::Child> {
        Ok((
            self.0.for_segment(ctx)?,
            self.1.for_segment(ctx)?,
        ))
    }

    fn merge(&self, acc: &mut Self::Fruit, fruit: &Self::Fruit) {
        self.0.merge(&mut acc.0, &fruit.0);
        self.1.merge(&mut acc.1, &fruit.1);
    }
}

impl<A1, A2> SegmentAgg for (A1, A2)
where
    A1: SegmentAgg,
    A2: SegmentAgg,
{
    type Fruit = (A1::Fruit, A2::Fruit);

    fn collect(&mut self, doc: DocId, score: Score, output: &mut Self::Fruit) {
        self.0.collect(doc, score, &mut output.0);
        self.1.collect(doc, score, &mut output.1);
    }
}