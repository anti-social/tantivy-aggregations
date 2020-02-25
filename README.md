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
let searcher = index_reader.searcher();
// The result type is specified as an example, it can be omitted
let agg_result: (u64, TermsAggResult<u64, (u64, Option<f64>)>) = searcher.agg_search(&AllQuery, &agg);
// Top 10 categories by document count
let top10_count = agg_result.1.top_k(10, |b| b.0);
// Top 10 categories by minimum price
let top10_min_price = agg_result.1.top_k(10, |b| b.1); 
```

TODO:
- [x] count
- [x] min, max (u64, i64, f64, date, u64s, i64s, f64s, dates)
- [x] sum (u64, i64, f64, u64s, i64s, f64s)
- [ ] stat
- [ ] cardinality
- [x] percentiles (f64, f64s)
- [x] terms, filtered_terms (u64, i64, u64s, i64s)
- [x] filter
- [ ] filters
- [x] histogram (f64)
- [ ] date_histogram
- [ ] top_hits
- [ ] dynamic aggregations (boxed) - need help
