# Changelog

Changes in comparison to the upstream driver, forked at version REL42.2.5.

## [Unreleased]
- Adjust `postgresql.util.StreamWrapper` for compatibility with newer versions of Java.
  `StreamWrapper.finalize()` needs to catch `Throwable` now.
- Update `jackson-databind` to 2.15.1

## [x.x.x] (2022-07-28)
- Get schema instead of catalog and simplify get primary keys query (#39)
- Keep using `table_catalog` field to get schema in `getPrimaryKeys()` query for versions <= 3 (#40) (#42)

## [x.x.x] (2019-03-13)
- `information_schema.columns.is_generated` type changed on CrateDB >= 4.0.0

## [x.x.x] (2018-12-06)
- Add support for negative row counts.

## [x.x.x] (2018-07-19)
- Enable `loadBalanceHosts` by default

## [x.x.x] (2018-06-18)
- Do not register `org.postgres.Driver` with DriverManager

## [x.x.x] (2018-03-27)
- Update `getPrimaryKeys()` method in MetaData for CrateDB >= 2.3.0
- Update `getTables()` method to only return tables not views

## [x.x.x] (2017-08-18)
- Fix: Unsupported `DatabaseMetaData` methods returned a row with all values set to
  `null` instead of an empty result with no rows at all
- `getTables()` and `getColumns()` now use SQL-99 compliant `information_schema` columns
- Disable transactions in strict mode

## [x.x.x] (2017-02-03)
- Make `getCrateVersion()` on metadata public
- Re-implement `getMoreResults()` method
- Allow setting autocommit to false in non-strict mode
- Change default value of `assumeMinServerVersion` to 9.5

## [x.x.x] (2016-11-23)
- Implement `table_schema`/`schema_name` version check
- Use `table_schema` instead of `schema_name` with crate >= 0.57
- `PGStatement` / `PGConnection`: throw an exception if an unsupported feature is used

## [x.x.x] (2016-10-25)
- Implement DatabaseMetadata getTables/getColumns methods
- Exclude columns with '[' and '.' from result of getColumns
- Make get/set object to work with maps
- Fix: don't query for column_name in getTables
- Remove crate related tests
- Add support for the crate specific strict mode
- Implement the get schemas metadata method
- Use `Crate` as the database product name
- Implement `getPrimaryKeys()`
- Implement empty resultset
- Adapt metadata methods that return emptyresult
- Update pgtabletypes metadata method
- Update Crate keywords
- Add aliases for Crate -> PG types
- Fix set/get Array issues for use with Crate types
- Implement public `getTypeInfo()` metadata method
- Basic implementation of `get*` functions


[CrateDB JDBC driver]: https://github.com/crate/crate-jdbc
