use tantivy::query::Query;
use tantivy::query::Weight;
use tantivy::Executor;
use tantivy::Result;
use tantivy::Searcher;
use tantivy::SegmentReader;

use std::ops::Deref;

use crate::agg::{Agg, AggSegmentContext, PreparedAgg, SegmentAgg};

pub trait AggSearcher {
    fn agg_search<A: Agg>(
        &self, query: &dyn Query, agg: &A
    ) -> Result<A::Fruit> {
        self.agg_search_with_executor(query, agg, &Executor::SingleThread)
    }

    fn agg_search_with_executor<A: Agg>(
        &self,
        query: &dyn Query,
        agg: &A,
        executor: &Executor,
    ) -> Result<A::Fruit>;
}

fn collect_segment<A: PreparedAgg>(
    agg: &A,
    weight: &dyn Weight,
    segment_ord: u32,
    segment_reader: &SegmentReader,
    harvest: &mut A::Fruit,
) -> Result<()> {
    let mut scorer = weight.scorer(segment_reader)?;
    let agg_ctx = AggSegmentContext {
        segment_ord,
        reader: segment_reader,
        scorer: scorer.as_ref(),
    };
    let mut segment_agg = agg.for_segment(&agg_ctx)?;
    if let Some(delete_bitset) = segment_reader.delete_bitset() {
        scorer.for_each(&mut |doc, score| {
            if delete_bitset.is_alive(doc) {
                segment_agg.collect(doc, score, harvest);
            }
        });
    } else {
        scorer.for_each(&mut |doc, score| segment_agg.collect(doc, score, harvest));
    }
    Ok(())
}

impl AggSearcher for Searcher {
    /// Search with aggregations
    fn agg_search_with_executor<A: Agg>(
        &self,
        query: &dyn Query,
        agg: &A,
        executor: &Executor,
    ) -> Result<A::Fruit> {
        let scoring_enabled = agg.requires_scoring();
        let weight = query.weight(self.deref(), scoring_enabled)?;
        let prepared_agg = agg.prepare(self.deref())?;
        let segment_readers = self.segment_readers();
        let harvest = match executor {
            Executor::SingleThread => {
                let mut harvest = prepared_agg.create_fruit();
                for (segment_ord, segment_reader) in segment_readers.iter().enumerate() {
                    collect_segment(
                        &prepared_agg,
                        weight.as_ref(),
                        segment_ord as u32,
                        segment_reader,
                        &mut harvest,
                    )?;
                }
                harvest
            }
            executor @ Executor::ThreadPool(_) => {
                let fruits = executor.map(
                    |(segment_ord, segment_reader)| {
                        let mut fruit = prepared_agg.create_fruit();
                        collect_segment(
                            &prepared_agg,
                            weight.as_ref(),
                            segment_ord as u32,
                            segment_reader,
                            &mut fruit,
                        ).map(|_| fruit)
                    },
                    segment_readers.iter().enumerate(),
                )?;
                let mut harvest = prepared_agg.create_fruit();
                for fruit in fruits {
                    prepared_agg.merge(&mut harvest, fruit);
                }
                harvest
            }
        };
        Ok(harvest)
    }
}
