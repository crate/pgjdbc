<img height="90" alt="Slonik Duke" align="right" src="docs/media/img/slonik_duke.png" />

# PostgreSQL JDBC Driver with adjustments for CrateDB


## About

The PostgreSQL JDBC Driver (PgJDBC for short) allows Java programs to connect
to a PostgreSQL database using standard, database independent Java code. Is an
open source JDBC driver written in Pure Java (Type 4), and communicates in the
PostgreSQL native network protocol.


## Details

This is a fork of the vanilla [PostgreSQL JDBC Driver] to support specific
details for [CrateDB], which can be inspected at [differences to the upstream
pgJDBC]. For example, this is to avoid certain statements and queries on
internal PostgreSQL tables which are not supported by CrateDB.

It is used by the [CrateDB legacy JDBC driver] for covering certain usage
scenarios around CrateDB and is not released otherwise.


## Contributing

For information on how to contribute to the project see the [Contributing
Guidelines](CONTRIBUTING.md).


[CrateDB]: https://github.com/crate/crate
[CrateDB legacy JDBC driver]: https://crate.io/docs/jdbc/
[differences to the upstream pgJDBC]: https://github.com/pgjdbc/pgjdbc/compare/master...crate:pgjdbc:REL42.2.5_crate
[PostgreSQL JDBC Driver]: https://jdbc.postgresql.org/
