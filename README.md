Aggregations for the [Tantivy](https://github.com/tantivy-search/tantivy) search engine

At the moment it is at a PoC state.

Usage example:

```rust
let filter_query = TermQuery::new(Term::from_field_u64(status_field, 0_u64));
let agg = filter_agg(
    &filter_query, 
    (
        count_agg(),
        terms_agg_u64(
            category_id_field,
            (
                count_agg(),
                min_agg_f64(price_field)
            )
        )
    )
);
let searcher = AggSearcher::from_reader(index_reader);
// The result type is specified as an example, it can be omitted
let agg_result: (u64, TermsAggResult<u64, (u64, Option<f64>)>) = searcher.search(&AllQuery, &agg);
```
