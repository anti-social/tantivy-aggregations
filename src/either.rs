use tantivy::{DocId, Result, Score, Searcher};

use crate::agg::{Agg, AggSegmentContext, PreparedAgg, SegmentAgg};

use Either::*;

#[derive(PartialEq, Debug)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> Either<L, R> {
    pub fn as_ref(&self) -> Either<&L, &R> {
        match self {
            Left(v) => Left(&v),
            Right(v) => Right(&v),
        }
    }

    pub fn map<LF, RF, LT, RT>(self, left_fn: LF, right_fn: RF) -> Either<LT, RT>
    where
        LF: FnOnce(L) -> LT,
        RF: FnOnce(R) -> RT,
    {
        match self {
            Left(v) => Left(left_fn(v)),
            Right(v) => Right(right_fn(v)),
        }
    }

    pub fn unwrap<LF, RF, T>(self, left_fn: LF, right_fn: RF) -> T
    where
        LF: FnOnce(L) -> T,
        RF: FnOnce(R) -> T,
    {
        match self {
            Left(v) => left_fn(v),
            Right(v) => right_fn(v),
        }
    }

    pub fn unwrap_left(self) -> L {
        match self {
            Left(v) => v,
            Right(_) => panic!("expect left"),
        }
    }

    pub fn unwrap_right(self) -> R {
        match self {
            Left(_) => panic!("expect right"),
            Right(v) => v,
        }
    }
}

pub fn either_agg<L, R>(agg: Either<L, R>) -> EitherAgg<L, R>
where
    L: Agg,
    R: Agg,
{
    EitherAgg { which: agg }
}

pub struct EitherAgg<L, R>
where
    L: Agg,
    R: Agg,
{
    which: Either<L, R>
}

impl<L, R> Agg for EitherAgg<L, R>
where
    L: Agg,
    R: Agg,
{
    type Fruit = Either<L::Fruit, R::Fruit>;
    type Child = EitherPreparedAgg<L::Child, R::Child>;

    fn prepare(&self, searcher: &Searcher) -> Result<Self::Child> {
        Ok(Self::Child {
            which: match &self.which {
                Left(agg) => Left(agg.prepare(searcher)?),
                Right(agg) => Right(agg.prepare(searcher)?),
            }
        })
    }

    fn requires_scoring(&self) -> bool {
        self.which.as_ref()
            .unwrap(|a| a.requires_scoring(), |a| a.requires_scoring())
    }
}

pub struct EitherPreparedAgg<L, R>
where
    L: PreparedAgg,
    R: PreparedAgg,
{
    which: Either<L, R>
}

impl<L, R> PreparedAgg for EitherPreparedAgg<L, R>
where
    L: PreparedAgg,
    R: PreparedAgg,
{
    type Fruit = Either<L::Fruit, R::Fruit>;
    type Child = EitherSegmentAgg<L::Child, R::Child>;

    fn create_fruit(&self) -> Self::Fruit {
        self.which.as_ref()
            .map(|a| a.create_fruit(), |a| a.create_fruit())
    }

    fn for_segment(&self, ctx: &AggSegmentContext) -> Result<Self::Child> {
        Ok(Self::Child {
            which: match &self.which {
                Left(agg) => Left(agg.for_segment(ctx)?),
                Right(agg) => Right(agg.for_segment(ctx)?),
            }
        })
    }

    fn merge(&self, acc: &mut Self::Fruit, fruit: Self::Fruit) {
        match (&self.which, acc) {
            (Left(ref agg), Left(ref mut acc)) => {
                agg.merge(acc, fruit.unwrap_left())
            }
            (Right(ref agg), Right(ref mut acc)) => {
                agg.merge(acc, fruit.unwrap_right())
            }
            _ => panic!("invalid state")
        }
    }
}

pub struct EitherSegmentAgg<L, R>
where
    L: SegmentAgg,
    R: SegmentAgg,
{
    which: Either<L, R>
}

impl<L, R> SegmentAgg for EitherSegmentAgg<L, R>
where
    L: SegmentAgg,
    R: SegmentAgg,
{
    type Fruit = Either<L::Fruit, R::Fruit>;

    fn create_fruit(&self) -> Self::Fruit {
        self.which.as_ref()
            .map(|a| a.create_fruit(), |a| a.create_fruit())
    }

    fn collect(&mut self, doc: DocId, score: Score, fruit: &mut Self::Fruit) {
        match (&mut self.which, fruit) {
            (Left(agg), Left(fruit)) => {
                agg.collect(doc, score, fruit);
            },
            (Right(agg), Right(fruit)) => {
                agg.collect(doc, score, fruit);
            },
            _ => panic!("invalid state"),
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::{Result};
    use tantivy::query::AllQuery;
    use tantivy::schema::Field;

    use test_fixtures::ProductIndex;

    use crate::{AggSearcher, count_agg, max_agg_f64, min_agg_f64};
    use crate::agg::Agg;
    use super::{Either, EitherAgg, either_agg};

    #[test]
    fn test_either_agg() -> Result<()> {
        let mut product_index = ProductIndex::create_in_ram(3)?;
        product_index.index_test_products()?;
        let searcher = product_index.reader.searcher();

        assert_eq!(
            searcher.agg_search(&AllQuery, &count_or_min_and_max_agg(None))?,
            Either::Left(5_u64)
        );
        assert_eq!(
            searcher.agg_search(&AllQuery, &count_or_min_and_max_agg(Some(product_index.schema.price)))?,
            Either::Right((Some(0.5), Some(100.01)))
        );

        Ok(())
    }

    fn count_or_min_and_max_agg(min_max_field: Option<Field>) -> EitherAgg<
        impl Agg<Fruit = u64>,
        impl Agg<Fruit = (Option<f64>, Option<f64>)>
    > {
        either_agg(match min_max_field {
            Some(f) => Either::Right((min_agg_f64(f), max_agg_f64(f))),
            None => Either::Left(count_agg())
        })
    }
}