#![feature(test)]
#[cfg(test)]

use rand::prelude::*;
use rand::SeedableRng;
use rand_pcg::Pcg32;

use tantivy::{Document, IndexWriter, Result, Term};
use tantivy::query::AllQuery;

extern crate tantivy_aggregations;
use tantivy_aggregations::histogram::histogram_agg_f64;
use tantivy_aggregations::metric::count_agg;
use tantivy_aggregations::searcher::AggSearcher;
use tantivy_aggregations::terms::{terms_agg_u64, terms_agg_u64s};

extern crate test;
use test::{Bencher, black_box};

use test_fixtures::{ProductSchema, ProductIndex};

#[bench]
fn bench_terms_agg(b: &mut Bencher) -> Result<()> {
    let mut product_index = ProductIndex::create_in_ram(10)?;
    index_test_products(&mut product_index.writer, &product_index.schema)?;
    product_index.reader.reload()?;

    let searcher = product_index.reader.searcher();
    dbg!(searcher.num_docs());
    dbg!(searcher.segment_readers().len());

//    let aggs = terms_agg(
//        schema.category_id,
//        count_agg()
//    );
//    let aggs = (
//        terms_agg_u64(
//            schema.category_id,
//            count_agg()
//        ),
//        terms_agg_u64s(
//            schema.attr_facets,
//            count_agg()
//        )
//    );
//    let cat_counts = searcher.search(&AllQuery, &aggs)?;
//    println!("Top 10 categories: {:?}", cat_counts.0.top_k(10, |b| b));
//    println!("Top 10 attributes: {:?}", cat_counts.1.top_k(10, |b| b));
//
//    b.iter(|| {
//        let cat_counts = searcher.search(&AllQuery,  &aggs)
//            .expect("Search failed");
//        black_box(cat_counts);
//    });

    let price_hist_agg = histogram_agg_f64(
        product_index.schema.price, 10_f64, count_agg()
    );
    let price_histogram = searcher.agg_search(&AllQuery, &price_hist_agg)?;
    println!("Price histogram: {:?}", price_histogram.buckets());

    b.iter(|| {
        let price_histogram = searcher.agg_search(&AllQuery,  &price_hist_agg)
            .expect("Search failed");
        black_box(price_histogram);
    });

    Ok(())
}

pub fn index_test_products(writer: &mut IndexWriter, schema: &ProductSchema) -> Result<u64> {
    let max_id = 1_250_000_u64;
    let num_deleted = 250_000;
    let mut rng = Pcg32::seed_from_u64(1u64);
    for id in 1_u64..=max_id {
        let mut doc = Document::new();
        doc.add_u64(schema.id, id);
        doc.add_u64(schema.category_id, rng.gen_range(1_u64, 1000));
        doc.add_f64(schema.price, rng.gen_range(1_f64, 101_f64));
        for _ in 0_u8..rng.gen_range(0_u8, 20u8) {
            let attr_id = rng.gen_range(1_u32, 20_u32);
            let value_id = rng.gen_range(1_u32, 100_u32);
            doc.add_u64(schema.attr_facets, ((attr_id as u64) << 32) | (value_id as u64));
        }
        writer.add_document(doc);
    }
    for _i in 1..=num_deleted {
        writer.delete_term(Term::from_field_u64(schema.id, rng.gen_range(1_u64, max_id)));
    }
    writer.commit()
}
