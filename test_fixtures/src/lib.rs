use tantivy::{doc, Index, IndexReader, IndexWriter, Result};
use tantivy::chrono::{DateTime, Utc};
use tantivy::directory::RAMDirectory;
use tantivy::schema::{Field, Schema, FAST, INDEXED, STORED, IntOptions, Cardinality};

pub struct ProductIndex {
    pub schema: ProductSchema,
    pub index: Index,
    pub writer: IndexWriter,
    pub reader: IndexReader,
}

impl ProductIndex {
    pub fn create_in_ram(heap_size_in_megabytes: u16) -> Result<Self> {
        let dir = RAMDirectory::create();
        let schema = ProductSchema::create();
        let index = Index::create(dir, schema.schema.clone())?;
        let mut index_writer = index.writer(heap_size_in_megabytes as usize * 1_000_000)?;
        index_test_products(&mut index_writer, &schema)?;
        let index_reader = index.reader()?;

        Ok(Self {
            schema,
            index,
            writer: index_writer,
            reader: index_reader,
        })
    }
}

pub struct ProductSchema {
    pub schema: Schema,
    pub id: Field,
    pub category_id: Field,
    pub tag_ids: Field,
    pub price: Field,
    pub positive_opinion_percent: Field,
    pub attr_facets: Field,
    pub date_created: Field,
}

impl ProductSchema {
    pub fn create() -> Self {
        let mut schema = Schema::builder();
        let id = schema.add_u64_field("id", INDEXED | STORED);
        let category_id = schema.add_u64_field("category_id", INDEXED | FAST);
        let tag_ids = schema.add_u64_field("tag_ids", IntOptions::default().set_fast(Cardinality::MultiValues));
        let price = schema.add_f64_field("price", INDEXED | FAST);
        let positive_opinion_percent = schema.add_u64_field("positive_opinion_percent", INDEXED | FAST);
        let attr_facets = schema.add_u64_field("attr_facets", INDEXED | FAST);
        let date_created = schema.add_date_field("date_created", INDEXED | FAST);
        Self {
            schema: schema.build(),
            id,
            category_id,
            tag_ids,
            price,
            positive_opinion_percent,
            attr_facets,
            date_created,
        }
    }
}

pub fn index_test_products(writer: &mut IndexWriter, schema: &ProductSchema) -> Result<u64> {
    writer.add_document(doc!(
        schema.id => 1_u64,
        schema.category_id => 1_u64,
        schema.tag_ids => 111_u64,
        schema.tag_ids => 112_u64,
        schema.tag_ids => 211_u64,
        schema.price => 9.99_f64,
        schema.positive_opinion_percent => 82_u64,
        schema.date_created => DateTime::parse_from_rfc3339("2019-12-31T23:59:59+00:00").unwrap().with_timezone(&Utc),
    ));
    writer.add_document(doc!(
        schema.id => 2_u64,
        schema.category_id => 1_u64,
        schema.tag_ids => 111_u64,
        schema.tag_ids => 211_u64,
        schema.tag_ids => 320_u64,
        schema.price => 10_f64,
        schema.positive_opinion_percent => 100_u64,
        schema.date_created => DateTime::parse_from_rfc3339("2020-01-01T00:00:00+00:00").unwrap().with_timezone(&Utc),
    ));
    writer.add_document(doc!(
        schema.id => 3_u64,
        schema.category_id => 2_u64,
        schema.tag_ids => 211_u64,
        schema.tag_ids => 311_u64,
        schema.price => 0.5_f64,
        schema.positive_opinion_percent => 71_u64,
    ));
    writer.add_document(doc!(
        schema.id => 4_u64,
        schema.category_id => 2_u64,
        schema.tag_ids => 320_u64,
        schema.price => 50_f64,
        schema.positive_opinion_percent => 85_u64,
        schema.date_created => DateTime::parse_from_rfc3339("2019-12-31T23:59:59+01:00").unwrap().with_timezone(&Utc),
    ));
    writer.add_document(doc!(
        schema.id => 5_u64,
        schema.category_id => 2_u64,
        schema.tag_ids => 311_u64,
        schema.tag_ids => 511_u64,
        schema.price => 100.01_f64,
        schema.positive_opinion_percent => 99_u64,
        schema.date_created => DateTime::parse_from_rfc3339("2019-12-31T23:59:59-01:00").unwrap().with_timezone(&Utc),
    ));
    writer.commit()
}
