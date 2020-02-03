#![feature(test)]
#[cfg(test)]

use rand::prelude::*;
use rand::SeedableRng;
use rand_pcg::Pcg32;

use tantivy::{Document, Index, IndexWriter, Result, Term};
use tantivy::directory::RAMDirectory;
use tantivy::query::AllQuery;

extern crate tantivy_aggregations;
use tantivy_aggregations::metric::count_agg;
use tantivy_aggregations::searcher::AggSearcher;
use tantivy_aggregations::terms::{terms_agg_u64, terms_agg_u64s};

extern crate test;
use test::{Bencher, black_box};

use test_fixtures::ProductSchema;

#[bench]
fn bench_terms_agg(b: &mut Bencher) -> Result<()> {
    let dir = RAMDirectory::create();
    let schema = ProductSchema::create();
    let index = Index::create(dir, schema.schema.clone())?;
    let mut index_writer = index.writer(10_000_000)?;
    index_test_products(&mut index_writer, &schema)?;

    let index_reader = index.reader()?;
    let searcher = AggSearcher::from_reader(index_reader);
    dbg!(searcher.num_docs());
    dbg!(searcher.segment_readers().len());

//    let aggs = terms_agg(
//        schema.category_id,
//        count_agg()
//    );
    let aggs = (
        terms_agg_u64(
            schema.category_id,
            count_agg()
        ),
        terms_agg_u64s(
            schema.attr_facets,
            count_agg()
        )
    );
    let cat_counts = searcher.search(&AllQuery, &aggs)?;
    println!("Top 10 categories: {:?}", cat_counts.0.top_k(10, |b| b));
    println!("Top 10 attributes: {:?}", cat_counts.1.top_k(10, |b| b));

    b.iter(|| {
        let cat_counts = searcher.search(&AllQuery,  &aggs)
            .expect("Search failed");
        black_box(cat_counts);
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
        doc.add_f64(schema.price, rng.gen_range(1_f64, 2_f64));
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
