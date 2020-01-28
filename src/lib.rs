pub mod metric;
pub mod terms;

#[cfg(test)]
pub(crate) mod fixtures;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
