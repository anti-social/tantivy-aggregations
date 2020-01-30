//use tantivy::collector::Collector;
//use tantivy::collector::SegmentCollector;
use tantivy::query::Query;
use tantivy::query::Scorer;
use tantivy::query::Weight;
use tantivy::schema::Document;
use tantivy::schema::Schema;
//use tantivy::schema::Field;
use tantivy::schema::Term;
//use tantivy::space_usage::SearcherSpaceUsage;
//use tantivy::termdict::TermMerger;
use tantivy::{DocAddress, IndexReader, LeasedItem};
use tantivy::Executor;
use tantivy::Index;
//use tantivy::InvertedIndexReader;
use tantivy::Result;
use tantivy::SegmentReader;
use std::fmt;
use std::ops::Deref;
//use std::sync::Arc;

use crate::agg::{Agg, AggSegmentContext, PreparedAgg, SegmentAgg};

fn collect_segment<A: PreparedAgg>(
    agg: &A,
    weight: &dyn Weight,
    segment_ord: u32,
    segment_reader: &SegmentReader,
) -> Result<A::Fruit> {
    let mut scorer = weight.scorer(segment_reader)?;
    let agg_ctx = AggSegmentContext {
        segment_ord,
        reader: segment_reader,
        scorer: scorer.as_ref(),
    };
    let mut segment_agg = agg.for_segment(&agg_ctx)?;
//    if let Some(delete_bitset) = segment_reader.delete_bitset() {
//        scorer.for_each(&mut |doc, score| {
//            if delete_bitset.is_alive(doc) {
//                segment_collector.collect(doc, score);
//            }
//        });
//    } else {
//        scorer.for_each(&mut |doc, score| segment_collector.collect(doc, score));
//    }
    let mut harvest = <<A as PreparedAgg>::Child as SegmentAgg>::Fruit::default();
    scorer.for_each(&mut |doc, score| segment_agg.collect(doc, score, &mut harvest));
    Ok(harvest)
}

/// Holds a list of `SegmentReader`s ready for search.
///
/// It guarantees that the `Segment` will not be removed before
/// the destruction of the `Searcher`.
///
pub struct AggSearcher {
    inner: LeasedItem<tantivy::Searcher>,
}

impl AggSearcher {
    /// Creates a new `Searcher`
    pub fn from_reader(
        reader: IndexReader,
    ) -> Self {
        Self {
            inner: reader.searcher(),
        }
    }

    pub fn searcher(&self) -> &tantivy::Searcher {
        &self.inner
    }

    /// Returns the `Index` associated to the `Searcher`
    pub fn index(&self) -> &Index {
        &self.inner.index()
    }

    /// Fetches a document from tantivy's store given a `DocAddress`.
    ///
    /// The searcher uses the segment ordinal to route the
    /// the request to the right `Segment`.
    pub fn doc(&self, doc_address: DocAddress) -> Result<Document> {
        self.inner.doc(doc_address)
    }

    /// Access the schema associated to the index of this searcher.
    pub fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    /// Returns the overall number of documents in the index.
    pub fn num_docs(&self) -> u64 {
        self.inner.num_docs()
    }

    /// Return the overall number of documents containing
    /// the given term.
    pub fn doc_freq(&self, term: &Term) -> u64 {
        self.inner.doc_freq(term)
    }

    /// Return the list of segment readers
    pub fn segment_readers(&self) -> &[SegmentReader] {
        self.inner.segment_readers()
    }

    /// Returns the segment_reader associated with the given segment_ordinal
    pub fn segment_reader(&self, segment_ord: u32) -> &SegmentReader {
        self.inner.segment_reader(segment_ord)
    }

    /// Runs a query on the segment readers wrapped by the searcher.
    ///
    /// Search works as follows :
    ///
    ///  First the weight object associated to the query is created.
    ///
    ///  Then, the query loops over the segments and for each segment :
    ///  - setup the collector and informs it that the segment being processed has changed.
    ///  - creates a SegmentCollector for collecting documents associated to the segment
    ///  - creates a `Scorer` object associated for this segment
    ///  - iterate through the matched documents and push them to the segment collector.
    ///
    ///  Finally, the Collector merges each of the child collectors into itself for result usability
    ///  by the caller.
    pub fn search<A: Agg>(&self, query: &dyn Query, agg: &A) -> Result<A::Fruit> {
        let executor = self.index().search_executor();
        self.search_with_executor(query, agg, executor)
    }

    /// Same as [`search(...)`](#method.search) but multithreaded.
    ///
    /// The current implementation is rather naive :
    /// multithreading is by splitting search into as many task
    /// as there are segments.
    ///
    /// It is powerless at making search faster if your index consists in
    /// one large segment.
    ///
    /// Also, keep in my multithreading a single query on several
    /// threads will not improve your throughput. It can actually
    /// hurt it. It will however, decrease the average response time.
    pub fn search_with_executor<A: Agg>(
        &self,
        query: &dyn Query,
        agg: &A,
        executor: &Executor,
    ) -> Result<A::Fruit> {
        let scoring_enabled = agg.requires_scoring();
        let weight = query.weight(self.inner.deref(), scoring_enabled)?;
        let prepared_agg = agg.prepare(self.inner.deref())?;
        let segment_readers = self.segment_readers();
        let fruits = executor.map(
            |(segment_ord, segment_reader)| {
                collect_segment(
                    &prepared_agg,
                    weight.as_ref(),
                    segment_ord as u32,
                    segment_reader,
                )
            },
            segment_readers.iter().enumerate(),
        )?;
        let mut harvest = A::Fruit::default();
        for fruit in fruits.iter() {
            prepared_agg.merge(&mut harvest, fruit);
        }
        Ok(harvest)
    }

//    /// Return the field searcher associated to a `Field`.
//    pub fn field(&self, field: Field) -> FieldSearcher {
//        let inv_index_readers = self
//            .segment_readers
//            .iter()
//            .map(|segment_reader| segment_reader.inverted_index(field))
//            .collect::<Vec<_>>();
//        FieldSearcher::new(inv_index_readers)
//    }

//    /// Summarize total space usage of this searcher.
//    pub fn space_usage(&self) -> SearcherSpaceUsage {
//        let mut space_usage = SearcherSpaceUsage::new();
//        for segment_reader in self.segment_readers.iter() {
//            space_usage.add_segment(segment_reader.space_usage());
//        }
//        space_usage
//    }
}

//pub struct FieldSearcher {
//    inv_index_readers: Vec<Arc<InvertedIndexReader>>,
//}
//
//impl FieldSearcher {
//    fn new(inv_index_readers: Vec<Arc<InvertedIndexReader>>) -> FieldSearcher {
//        FieldSearcher { inv_index_readers }
//    }
//
//    /// Returns a Stream over all of the sorted unique terms of
//    /// for the given field.
//    pub fn terms(&self) -> TermMerger<'_> {
//        let term_streamers: Vec<_> = self
//            .inv_index_readers
//            .iter()
//            .map(|inverted_index| inverted_index.terms().stream())
//            .collect();
//        TermMerger::new(term_streamers)
//    }
//}

impl fmt::Debug for AggSearcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let segment_ids = self
            .segment_readers()
            .iter()
            .map(SegmentReader::segment_id)
            .collect::<Vec<_>>();
        write!(f, "Searcher({:?})", segment_ids)
    }
}