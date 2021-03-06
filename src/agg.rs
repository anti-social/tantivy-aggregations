use tantivy::{Result, SegmentLocalId, SegmentReader, DocId, Score, Searcher};
use tantivy::query::Scorer;

pub struct AggSegmentContext<'r, 's> {
    pub segment_ord: SegmentLocalId,
    pub reader: &'r SegmentReader,
    pub scorer: &'s dyn Scorer,
}

pub trait Agg {
    type Fruit: Send;
    type Child: PreparedAgg<Fruit= Self::Fruit>;

    fn prepare(&self, searcher: &Searcher) -> Result<Self::Child>;

    fn requires_scoring(&self) -> bool;
}

pub trait PreparedAgg: Sync {
    type Fruit: Send;
    type Child: SegmentAgg<Fruit = Self::Fruit>;

    fn create_fruit(&self) -> Self::Fruit;

    fn for_segment(&self, ctx: &AggSegmentContext) -> Result<Self::Child>;

    fn merge(&self, acc: &mut Self::Fruit, fruit: Self::Fruit);
}

pub trait SegmentAgg {
    type Fruit;

    fn create_fruit(&self) -> Self::Fruit;

    fn collect(&mut self, doc: DocId, score: Score, output: &mut Self::Fruit);
}