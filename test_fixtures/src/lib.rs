use tantivy::{doc, IndexWriter, Result};
use tantivy::schema::{Field, Schema, FAST, INDEXED, STORED};

pub struct ProductSchema {
    pub schema: Schema,
    pub id: Field,
    pub category_id: Field,
    pub price: Field,
    pub attr_facets: Field,
}

impl ProductSchema {
    pub fn create() -> Self {
        let mut schema = Schema::builder();
        let id = schema.add_u64_field("id", INDEXED | STORED);
        let category_id = schema.add_u64_field("category_id", INDEXED | FAST);
        let price = schema.add_f64_field("price", INDEXED | FAST);
        let attr_facets = schema.add_u64_field("attr_facets", INDEXED | FAST);
        Self {
            schema: schema.build(),
            id,
            category_id,
            price,
            attr_facets,
        }
    }
}

pub fn index_test_products(writer: &mut IndexWriter, schema: &ProductSchema) -> Result<u64> {
    writer.add_document(doc!(
            schema.category_id => 1_u64,
            schema.price => 9.99_f64,
        ));
    writer.add_document(doc!(
            schema.category_id => 1_u64,
            schema.price => 10_f64,
        ));
    writer.add_document(doc!(
            schema.category_id => 2_u64,
            schema.price => 0.5_f64,
        ));
    writer.add_document(doc!(
            schema.category_id => 2_u64,
            schema.price => 50_f64,
        ));
    writer.add_document(doc!(
            schema.category_id => 2_u64,
            schema.price => 100.01_f64,
        ));
    writer.commit()
}
