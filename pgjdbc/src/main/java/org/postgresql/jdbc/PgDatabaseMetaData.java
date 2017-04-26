/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2015, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.jdbc;

import org.postgresql.Driver;
import org.postgresql.core.BaseStatement;
import org.postgresql.core.Field;
import org.postgresql.core.Oid;
import org.postgresql.core.ServerVersion;
import org.postgresql.util.GT;
import org.postgresql.util.JdbcBlackHole;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.*;

public class PgDatabaseMetaData implements DatabaseMetaData {

  public PgDatabaseMetaData(PgConnection conn) {
    this.connection = conn;
  }

  protected final PgConnection connection; // The connection association

  private int NAMEDATALEN = 0; // length for name datatype
  private int INDEX_MAX_KEYS = 0; // maximum number of keys in an index.

  protected int getMaxIndexKeys() throws SQLException {
    if (INDEX_MAX_KEYS == 0) {
      String sql;
      if (connection.haveMinimumServerVersion(ServerVersion.v8_0)) {
        sql = "SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys'";
      } else {
        String from;
        if (connection.haveMinimumServerVersion(ServerVersion.v7_3)) {
          from =
              "pg_catalog.pg_namespace n, pg_catalog.pg_type t1, pg_catalog.pg_type t2 WHERE t1.typnamespace=n.oid AND n.nspname='pg_catalog' AND ";
        } else {
          from = "pg_type t1, pg_type t2 WHERE ";
        }
        sql = "SELECT t1.typlen/t2.typlen FROM " + from
            + " t1.typelem=t2.oid AND t1.typname='oidvector'";
      }
      Statement stmt = connection.createStatement();
      ResultSet rs = null;
      try {
        rs = stmt.executeQuery(sql);
        if (!rs.next()) {
          stmt.close();
          throw new PSQLException(
              GT.tr(
                  "Unable to determine a value for MaxIndexKeys due to missing system catalog data."),
              PSQLState.UNEXPECTED_ERROR);
        }
        INDEX_MAX_KEYS = rs.getInt(1);
      } finally {
        JdbcBlackHole.close(rs);
        JdbcBlackHole.close(stmt);
      }
    }
    return INDEX_MAX_KEYS;
  }

  protected int getMaxNameLength() throws SQLException {
    if (NAMEDATALEN == 0) {
      String sql;
      if (connection.haveMinimumServerVersion(ServerVersion.v7_3)) {
        sql =
            "SELECT t.typlen FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n WHERE t.typnamespace=n.oid AND t.typname='name' AND n.nspname='pg_catalog'";
      } else {
        sql = "SELECT typlen FROM pg_type WHERE typname='name'";
      }
      Statement stmt = connection.createStatement();
      ResultSet rs = null;
      try {
        rs = stmt.executeQuery(sql);
        if (!rs.next()) {
          throw new PSQLException(GT.tr("Unable to find name datatype in the system catalogs."),
              PSQLState.UNEXPECTED_ERROR);
        }
        NAMEDATALEN = rs.getInt("typlen");
      } finally {
        JdbcBlackHole.close(rs);
        JdbcBlackHole.close(stmt);
      }
    }
    return NAMEDATALEN - 1;
  }


  public boolean allProceduresAreCallable() throws SQLException {
    return true; // For now...
  }

  public boolean allTablesAreSelectable() throws SQLException {
    return true; // For now...
  }

  public String getURL() throws SQLException {
    return connection.getURL();
  }

  public String getUserName() throws SQLException {
    return connection.getUserName();
  }

  public boolean isReadOnly() throws SQLException {
    return connection.isReadOnly();
  }

  public boolean nullsAreSortedHigh() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v7_2);
  }

  public boolean nullsAreSortedLow() throws SQLException {
    return false;
  }

  public boolean nullsAreSortedAtStart() throws SQLException {
    return false;
  }

  public boolean nullsAreSortedAtEnd() throws SQLException {
    return !connection.haveMinimumServerVersion(ServerVersion.v7_2);
  }

  /**
   * What is the name of this database product - we hope that it is PostgreSQL, so we return that
   * explicitly.
   *
   * @return the database product name
   *
   * @exception SQLException if a database access error occurs
   */
  public String getDatabaseProductName() throws SQLException {
    return "Crate";
  }

  public String getDatabaseProductVersion() throws SQLException {
    return connection.getDBVersionNumber();
  }

  public String getDriverName() throws SQLException {
    return "PostgreSQL Native Driver";
  }

  public String getDriverVersion() throws SQLException {
    return Driver.getVersion();
  }

  public int getDriverMajorVersion() {
    return Driver.MAJORVERSION;
  }

  public int getDriverMinorVersion() {
    return Driver.MINORVERSION;
  }

  /**
   * Does the database store tables in a local file? No - it stores them in a file on the server.
   *
   * @return true if so
   * @throws SQLException if a database access error occurs
   */
  public boolean usesLocalFiles() throws SQLException {
    return false;
  }

  /**
   * Does the database use a file for each table? Well, not really, since it doesn't use local files.
   *
   * @return true if so
   * @throws SQLException if a database access error occurs
   */
  public boolean usesLocalFilePerTable() throws SQLException {
    return false;
  }

  /**
   * Does the database treat mixed case unquoted SQL identifiers as case sensitive and as a result
   * store them in mixed case? A JDBC-Compliant driver will always return false.
   *
   * @return true if so
   * @throws SQLException if a database access error occurs
   */
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return true;
  }

  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  /**
   * Does the database treat mixed case quoted SQL identifiers as case sensitive and as a result
   * store them in mixed case? A JDBC compliant driver will always return true.
   *
   * @return true if so
   * @throws SQLException if a database access error occurs
   */
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  /**
   * What is the string used to quote SQL identifiers? This returns a space if identifier quoting
   * isn't supported. A JDBC Compliant driver will always use a double quote character.
   *
   * @return the quoting string
   * @throws SQLException if a database access error occurs
   */
  public String getIdentifierQuoteString() throws SQLException {
    return "\"";
  }

  public String getSQLKeywords() throws SQLException {
    return "alias,all,alter,analyzer,and,any,array,as,asc," +
      "always,array,add," +
      "bernoulli,between,blob,boolean,by,byte,begin," +
      "case,cast,catalogs,char_filters,clustered,coalesce,columns," +
      "constraint,copy,create,cross,current,current_date,current_time," +
      "current_timestamp,current_schema, column," +
      "date,day,delete,desc,describe,directory,distinct,distributed," +
      "double,drop,dynamic,delete,duplicate,default," +
      "else,end,escape,except,exists,explain,extends,extract," +
      "false,first,float,following,for,format,from,full,fulltext,functions," +
      "graphviz,group,geo_point,geo_shape,global,generated," +
      "having,hour," +
      "if,ignored,in,index,inner,insert,int,integer,intersect,interval," +
      "into,ip,is,isolation," +
      "join," +
      "last,left,like,limit,logical,long,local,level," +
      "materialized,minute,month,match," +
      "natural,not,null,nulls," +
      "object,off,offset,on,or,order,outer,over,optmize,only," +
      "partition,partitioned,partitions,plain,preceding,primary_key," +
      "range,recursive,refresh,reset,right,row,rows,repository,restore," +
      "schemas,second,select,set,shards,short,show,some,stratify," +
      "strict,string_type,substring,system,select,snapshot,session," +
      "table,tables,tablesample,text,then,time,timestamp,to,tokenizer," +
      "token_filters,true,type,try_cast,transaction,tablesample," +
      "transient," +
      "unbounded,union,update,using," +
      "values,view," +
      "when,where,with," +
      "year";
  }

  public String getNumericFunctions() throws SQLException {
    return "abs,ceil,floor,ln,log,random,round,sqrt,sin,asin,cos," +
            "acos,tan,atan";
  }

  public String getStringFunctions() throws SQLException {
    return "concat,format,substr,char_length,bit_length,octet_length,"+
            "lower,upper";
  }

  public String getSystemFunctions() throws SQLException {
    return "";
  }

  public String getTimeDateFunctions() throws SQLException {
    return "date_trunc,extract,date_format";
  }

  public String getSearchStringEscape() throws SQLException {
    // This method originally returned "\\\\" assuming that it
    // would be fed directly into pg's input parser so it would
    // need two backslashes. This isn't how it's supposed to be
    // used though. If passed as a PreparedStatement parameter
    // or fed to a DatabaseMetaData method then double backslashes
    // are incorrect. If you're feeding something directly into
    // a query you are responsible for correctly escaping it.
    // With 8.2+ this escaping is a little trickier because you
    // must know the setting of standard_conforming_strings, but
    // that's not our problem.

    return "\\";
  }

  /**
   * {@inheritDoc}
   *
   * <p>
   * Postgresql allows any high-bit character to be used in an unquoted identifier, so we can't
   * possibly list them all.
   *
   * From the file src/backend/parser/scan.l, an identifier is ident_start [A-Za-z\200-\377_]
   * ident_cont [A-Za-z\200-\377_0-9\$] identifier {ident_start}{ident_cont}*
   *
   * @return a string containing the extra characters
   * @throws SQLException if a database access error occurs
   */
  public String getExtraNameCharacters() throws SQLException {
    return "";
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 6.1+
   */
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.3+
   */
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v7_3);
  }

  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  public boolean nullPlusNonNullIsNull() throws SQLException {
    return true;
  }

  public boolean supportsConvert() throws SQLException {
    return false;
  }

  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    return false;
  }

  public boolean supportsTableCorrelationNames() throws SQLException {
    return true;
  }

  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return false;
  }

  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 6.4+
   */
  public boolean supportsOrderByUnrelated() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v6_4);
  }

  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 6.4+
   */
  public boolean supportsGroupByUnrelated() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v6_4);
  }

  /*
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 6.4+
   */
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v6_4);
  }

  /*
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.1+
   */
  public boolean supportsLikeEscapeClause() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v7_1);
  }

  public boolean supportsMultipleResultSets() throws SQLException {
    return true;
  }

  public boolean supportsMultipleTransactions() throws SQLException {
    return true;
  }

  public boolean supportsNonNullableColumns() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * This grammar is defined at:
   *
   * <p>
   * <a href="http://www.microsoft.com/msdn/sdk/platforms/doc/odbc/src/intropr.htm">http://www.
   * microsoft.com/msdn/sdk/platforms/doc/odbc/src/intropr.htm</a>
   *
   * <p>
   * In Appendix C. From this description, we seem to support the ODBC minimal (Level 0) grammar.
   *
   * @return true
   */
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return true;
  }

  /**
   * Does this driver support the Core ODBC SQL grammar. We need SQL-92 conformance for this.
   *
   * @return false
   * @throws SQLException if a database access error occurs
   */
  public boolean supportsCoreSQLGrammar() throws SQLException {
    return false;
  }

  /**
   * Does this driver support the Extended (Level 2) ODBC SQL grammar. We don't conform to the Core
   * (Level 1), so we can't conform to the Extended SQL Grammar.
   *
   * @return false
   * @throws SQLException if a database access error occurs
   */
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return false;
  }

  /**
   * Does this driver support the ANSI-92 entry level SQL grammar? All JDBC Compliant drivers must
   * return true. We currently report false until 'schema' support is added. Then this should be
   * changed to return true, since we will be mostly compliant (probably more compliant than many
   * other databases) And since this is a requirement for all JDBC drivers we need to get to the
   * point where we can return true.
   *
   * @return true if connected to PostgreSQL 7.3+
   * @throws SQLException if a database access error occurs
   */
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v7_3);
  }

  /**
   * {@inheritDoc}
   *
   * @return false
   */
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * @return false
   */
  public boolean supportsANSI92FullSQL() throws SQLException {
    return false;
  }

  /*
   * Is the SQL Integrity Enhancement Facility supported? Our best guess is that this means support
   * for constraints
   *
   * @return true
   *
   * @exception SQLException if a database access error occurs
   */
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.1+
   */
  public boolean supportsOuterJoins() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v7_1);
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.1+
   */
  public boolean supportsFullOuterJoins() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v7_1);
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.1+
   */
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v7_1);
  }

  /**
   * {@inheritDoc}
   * <p>
   * PostgreSQL doesn't have schemas, but when it does, we'll use the term "schema".
   *
   * @return {@code "schema"}
   */
  public String getSchemaTerm() throws SQLException {
    return "schema";
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code "function"}
   */
  public String getProcedureTerm() throws SQLException {
    return "function";
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code "database"}
   */
  public String getCatalogTerm() throws SQLException {
    return "database";
  }

  public boolean isCatalogAtStart() throws SQLException {
    return true;
  }

  public String getCatalogSeparator() throws SQLException {
    return ".";
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.3+
   */
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v7_3);
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.3+
   */
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v7_3);
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.3+
   */
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v7_3);
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.3+
   */
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v7_3);
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.3+
   */
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v7_3);
  }

  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  /**
   * We support cursors for gets only it seems. I dont see a method to get a positioned delete.
   *
   * @return false
   * @throws SQLException if a database access error occurs
   */
  public boolean supportsPositionedDelete() throws SQLException {
    return false; // For now...
  }

  public boolean supportsPositionedUpdate() throws SQLException {
    return false; // For now...
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 6.5+
   */
  public boolean supportsSelectForUpdate() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v6_5);
  }

  public boolean supportsStoredProcedures() throws SQLException {
    return true;
  }

  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return true;
  }

  public boolean supportsSubqueriesInExists() throws SQLException {
    return true;
  }

  public boolean supportsSubqueriesInIns() throws SQLException {
    return true;
  }

  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.1+
   */
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v7_1);
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 6.3+
   */
  public boolean supportsUnion() throws SQLException {
    return true; // since 6.3
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.1+
   */
  public boolean supportsUnionAll() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v7_1);
  }

  /**
   * {@inheritDoc} In PostgreSQL, Cursors are only open within transactions.
   */
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return false;
  }

  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return false;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Can statements remain open across commits? They may, but this driver cannot guarantee that. In
   * further reflection. we are talking a Statement object here, so the answer is yes, since the
   * Statement is only a vehicle to ExecSQL()
   *
   * @return true
   */
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Can statements remain open across rollbacks? They may, but this driver cannot guarantee that.
   * In further contemplation, we are talking a Statement object here, so the answer is yes, since
   * the Statement is only a vehicle to ExecSQL() in Connection
   *
   * @return true
   */
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return true;
  }

  public int getMaxCharLiteralLength() throws SQLException {
    return 0; // no limit
  }

  public int getMaxBinaryLiteralLength() throws SQLException {
    return 0; // no limit
  }

  public int getMaxColumnNameLength() throws SQLException {
    return getMaxNameLength();
  }

  public int getMaxColumnsInGroupBy() throws SQLException {
    return 0; // no limit
  }

  public int getMaxColumnsInIndex() throws SQLException {
    return getMaxIndexKeys();
  }

  public int getMaxColumnsInOrderBy() throws SQLException {
    return 0; // no limit
  }

  public int getMaxColumnsInSelect() throws SQLException {
    return 0; // no limit
  }

  /**
   * {@inheritDoc} What is the maximum number of columns in a table? From the CREATE TABLE reference
   * page...
   *
   * <p>
   * "The new class is created as a heap with no initial data. A class can have no more than 1600
   * attributes (realistically, this is limited by the fact that tuple sizes must be less than 8192
   * bytes)..."
   *
   * @return the max columns
   * @throws SQLException if a database access error occurs
   */
  public int getMaxColumnsInTable() throws SQLException {
    return 1600;
  }

  /**
   * {@inheritDoc} How many active connection can we have at a time to this database? Well, since it
   * depends on postmaster, which just does a listen() followed by an accept() and fork(), its
   * basically very high. Unless the system runs out of processes, it can be 65535 (the number of
   * aux. ports on a TCP/IP system). I will return 8192 since that is what even the largest system
   * can realistically handle,
   *
   * @return the maximum number of connections
   * @throws SQLException if a database access error occurs
   */
  public int getMaxConnections() throws SQLException {
    return 8192;
  }

  public int getMaxCursorNameLength() throws SQLException {
    return getMaxNameLength();
  }

  public int getMaxIndexLength() throws SQLException {
    return 0; // no limit (larger than an int anyway)
  }

  public int getMaxSchemaNameLength() throws SQLException {
    return getMaxNameLength();
  }

  public int getMaxProcedureNameLength() throws SQLException {
    return getMaxNameLength();
  }

  public int getMaxCatalogNameLength() throws SQLException {
    return getMaxNameLength();
  }

  public int getMaxRowSize() throws SQLException {
    if (connection.haveMinimumServerVersion(ServerVersion.v7_1)) {
      return 1073741824; // 1 GB
    } else {
      return 8192; // XXX could be altered
    }
  }

  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return false;
  }

  public int getMaxStatementLength() throws SQLException {
    if (connection.haveMinimumServerVersion(ServerVersion.v7_0)) {
      return 0; // actually whatever fits in size_t
    } else {
      return 16384;
    }
  }

  public int getMaxStatements() throws SQLException {
    return 0;
  }

  public int getMaxTableNameLength() throws SQLException {
    return getMaxNameLength();
  }

  public int getMaxTablesInSelect() throws SQLException {
    return 0; // no limit
  }

  public int getMaxUserNameLength() throws SQLException {
    return getMaxNameLength();
  }

  public int getDefaultTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_READ_COMMITTED;
  }

  public boolean supportsTransactions() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   * <p>
   * We only support TRANSACTION_SERIALIZABLE and TRANSACTION_READ_COMMITTED before 8.0; from 8.0
   * READ_UNCOMMITTED and REPEATABLE_READ are accepted aliases for READ_COMMITTED.
   */
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    if (level == Connection.TRANSACTION_SERIALIZABLE
        || level == Connection.TRANSACTION_READ_COMMITTED) {
      return true;
    } else if (connection.haveMinimumServerVersion(ServerVersion.v8_0)
        && (level == Connection.TRANSACTION_READ_UNCOMMITTED
            || level == Connection.TRANSACTION_REPEATABLE_READ)) {
      return true;
    } else {
      return false;
    }
  }

  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return true;
  }

  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return false;
  }

  /**
   * Does a data definition statement within a transaction force the transaction to commit? It seems
   * to mean something like:
   *
   * <pre>
   * CREATE TABLE T (A INT);
   * INSERT INTO T (A) VALUES (2);
   * BEGIN;
   * UPDATE T SET A = A + 1;
   * CREATE TABLE X (A INT);
   * SELECT A FROM T INTO X;
   * COMMIT;
   * </pre>
   *
   * does the CREATE TABLE call cause a commit? The answer is no.
   *
   * @return true if so
   * @throws SQLException if a database access error occurs
   */
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return false;
  }

  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return false;
  }

  /**
   * Turn the provided value into a valid string literal for direct inclusion into a query. This
   * includes the single quotes needed around it.
   *
   * @param s input value
   * @return string literal for direct inclusion into a query
   * @throws SQLException if something wrong happens
   */
  protected String escapeQuotes(String s) throws SQLException {
    StringBuilder sb = new StringBuilder();
    if (!connection.getStandardConformingStrings()
        && connection.haveMinimumServerVersion(ServerVersion.v8_1)) {
      sb.append("E");
    }
    sb.append("'");
    sb.append(connection.escapeString(s));
    sb.append("'");
    return sb.toString();
  }

  public ResultSet getProcedures(String catalog, String schemaPattern,
      String procedureNamePattern) throws SQLException {
    return emptyResult(
            col("PROCEDURE_CAT"),
            col("PROCEDURE_SCHEM"),
            col("PROCEDURE_NAME"),
            col("NUM_INPUT_PARAMS", Oid.INT4),
            col("NUM_OUTPUT_PARAMS", Oid.INT4),
            col("NUM_RESULT_SETS", Oid.INT4),
            col("REMARKS"),
            col("PROCEDURE_TYPE", Oid.INT2),
            col("SPECIFIC_NAME"));
  }

  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
      String procedureNamePattern, String columnNamePattern) throws SQLException {
    return emptyResult(
            col("PROCEDURE_CAT"),
            col("PROCEDURE_SCHEM"),
            col("PROCEDURE_NAME"),
            col("COLUMN_NAME"),
            col("COLUMN_TYPE", Oid.INT2),
            col("DATA_TYPE"),
            col("TYPE_NAME"),
            col("PRECISION", Oid.INT4),
            col("LENGTH", Oid.INT4),
            col("SCALE", Oid.INT2),
            col("RADIX", Oid.INT2),
            col("NULLABLE", Oid.INT2),
            col("REMARKS"),
            col("COLUMN_DEF"),
            col("SQL_DATA_TYPE", Oid.INT4),
            col("SQL_DATETIME_SUB", Oid.INT4),
            col("CHAR_OCTET_LENGTH", Oid.INT4),
            col("ORDINAL_POSITION", Oid.INT4),
            col("IS_NULLANLE"),
            col("SPECIFIC_NAME"));
  }

  public ResultSet getTables(String catalog,
                             String schemaPattern,
                             String tableNamePattern,
                             String types[]) throws SQLException {
    Field fields[] = new Field[10];
    fields[0] = new Field("TABLE_CAT", Oid.VARCHAR);
    fields[1] = new Field("TABLE_SCHEM", Oid.VARCHAR);
    fields[2] = new Field("TABLE_NAME", Oid.VARCHAR);
    fields[3] = new Field("TABLE_TYPE", Oid.VARCHAR);
    fields[4] = new Field("REMARKS", Oid.VARCHAR);
    fields[5] = new Field("TYPE_CAT", Oid.VARCHAR);
    fields[6] = new Field("TYPE_SCHEM", Oid.VARCHAR);
    fields[7] = new Field("TYPE_NAME", Oid.VARCHAR);
    fields[8] = new Field("SELF_REFERENCING_COL_NAME", Oid.VARCHAR);
    fields[9] = new Field("REF_GENERATION", Oid.VARCHAR);

    String schemaName = getCrateSchemaName();
    String stmt = "select " + schemaName + ", table_name" +
      " from information_schema.tables" +
      createInfoSchemaTableWhereClause(schemaName, schemaPattern, tableNamePattern, null) +
      " order by " + schemaName + ", table_name";
    ResultSet rs = connection.createStatement().executeQuery(stmt);

    List<byte[][]> tuples = new ArrayList<>();
    while (rs.next()) {
      byte[][] tuple = new byte[fields.length][];

      String schema = rs.getString(schemaName);
      if ("sys".equals(schema) || "information_schema".equals(schema)) {
        tuple[3] = connection.encodeString("SYSTEM TABLE");
      } else {
        tuple[3] = connection.encodeString("TABLE");
      }

      tuple[0] = null;
      tuple[2] = rs.getBytes("table_name");
      tuple[1] = schema.getBytes();
      tuple[4] = connection.encodeString("");
      tuple[5] = null;
      tuple[6] = null;
      tuple[7] = null;
      tuple[8] = connection.encodeString("_id");
      tuple[9] = connection.encodeString("SYSTEM");
      tuples.add(tuple);
    }
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(fields, tuples);
  }

  private String getCrateSchemaName() throws SQLException {
    return getCrateVersion().before("0.57.0") ? "schema_name" : "table_schema";
  }

  public CrateVersion getCrateVersion() throws SQLException {
    ResultSet rs = connection.createStatement()
      .executeQuery("select version['number'] as version from sys.nodes limit 1");
    if (rs.next()) {
      return new CrateVersion(rs.getString("version"));
    }
    throw new SQLException("unable to fetch Crate version");
  }

  private StringBuilder createInfoSchemaTableWhereClause(String schemaColumnName,
                                                         String schemaPattern,
                                                         String tableNamePattern,
                                                         String columnNamePattern) throws SQLException {
    StringBuilder where = new StringBuilder(" where ");
    if (schemaPattern == null) {
      where.append(schemaColumnName).append(" like '%'");
    } else if (schemaPattern.equals("")) {
      where.append(schemaColumnName).append(" is null");
    } else {
      where.append(schemaColumnName).append(" like '")
        .append(connection.escapeString(schemaPattern))
        .append("'");
    }

    if (columnNamePattern != null) {
      where.append(" and column_name like '")
        .append(connection.escapeString(columnNamePattern))
        .append("'");
    }

    if (tableNamePattern != null) {
      where.append(" and table_name like '")
        .append(connection.escapeString(tableNamePattern))
        .append("'");
    }
    return where;
  }

  private static final Map<String, Map<String, String>> tableTypeClauses;

  static {
    tableTypeClauses = new HashMap<String, Map<String, String>>();
    Map<String, String> ht = new HashMap<String, String>();
    tableTypeClauses.put("TABLE", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'");
    ht.put("NOSCHEMAS", "c.relkind = 'r' AND c.relname !~ '^pg_'");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("VIEW", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'v' AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema'");
    ht.put("NOSCHEMAS", "c.relkind = 'v' AND c.relname !~ '^pg_'");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("INDEX", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'i' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'");
    ht.put("NOSCHEMAS", "c.relkind = 'i' AND c.relname !~ '^pg_'");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("SEQUENCE", ht);
    ht.put("SCHEMAS", "c.relkind = 'S'");
    ht.put("NOSCHEMAS", "c.relkind = 'S'");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("TYPE", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'c' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'");
    ht.put("NOSCHEMAS", "c.relkind = 'c' AND c.relname !~ '^pg_'");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("SYSTEM TABLE", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'r' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema')");
    ht.put("NOSCHEMAS",
        "c.relkind = 'r' AND c.relname ~ '^pg_' AND c.relname !~ '^pg_toast_' AND c.relname !~ '^pg_temp_'");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("SYSTEM TOAST TABLE", ht);
    ht.put("SCHEMAS", "c.relkind = 'r' AND n.nspname = 'pg_toast'");
    ht.put("NOSCHEMAS", "c.relkind = 'r' AND c.relname ~ '^pg_toast_'");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("SYSTEM TOAST INDEX", ht);
    ht.put("SCHEMAS", "c.relkind = 'i' AND n.nspname = 'pg_toast'");
    ht.put("NOSCHEMAS", "c.relkind = 'i' AND c.relname ~ '^pg_toast_'");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("SYSTEM VIEW", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'v' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema') ");
    ht.put("NOSCHEMAS", "c.relkind = 'v' AND c.relname ~ '^pg_'");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("SYSTEM INDEX", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'i' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema') ");
    ht.put("NOSCHEMAS",
        "c.relkind = 'v' AND c.relname ~ '^pg_' AND c.relname !~ '^pg_toast_' AND c.relname !~ '^pg_temp_'");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("TEMPORARY TABLE", ht);
    ht.put("SCHEMAS", "c.relkind = 'r' AND n.nspname ~ '^pg_temp_' ");
    ht.put("NOSCHEMAS", "c.relkind = 'r' AND c.relname ~ '^pg_temp_' ");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("TEMPORARY INDEX", ht);
    ht.put("SCHEMAS", "c.relkind = 'i' AND n.nspname ~ '^pg_temp_' ");
    ht.put("NOSCHEMAS", "c.relkind = 'i' AND c.relname ~ '^pg_temp_' ");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("TEMPORARY VIEW", ht);
    ht.put("SCHEMAS", "c.relkind = 'v' AND n.nspname ~ '^pg_temp_' ");
    ht.put("NOSCHEMAS", "c.relkind = 'v' AND c.relname ~ '^pg_temp_' ");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("TEMPORARY SEQUENCE", ht);
    ht.put("SCHEMAS", "c.relkind = 'S' AND n.nspname ~ '^pg_temp_' ");
    ht.put("NOSCHEMAS", "c.relkind = 'S' AND c.relname ~ '^pg_temp_' ");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("FOREIGN TABLE", ht);
    ht.put("SCHEMAS", "c.relkind = 'f'");
    ht.put("NOSCHEMAS", "c.relkind = 'f'");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("MATERIALIZED VIEW", ht);
    ht.put("SCHEMAS", "c.relkind = 'm'");
    ht.put("NOSCHEMAS", "c.relkind = 'm'");
  }

  public ResultSet getSchemas() throws SQLException {
    return getSchemas(null, null);
  }

  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    StringBuilder stmt = new StringBuilder("select schema_name from information_schema.schemata");
    if (schemaPattern != null) {
      stmt.append(" where schema_name like '")
          .append(connection.escapeString(schemaPattern))
          .append("'");
    }
    stmt.append(" order by schema_name");

    ResultSet rs = connection.createStatement().executeQuery(stmt.toString());
    Field[] fields = new Field[2];
    fields[0] = new Field("TABLE_SCHEM", Oid.VARCHAR);
    fields[1] = new Field("TABLE_CAT", Oid.VARCHAR);

    List<byte[][]> tuples = new ArrayList<>();
    while (rs.next()) {
      tuples.add(new byte[][] {rs.getBytes("schema_name"), null});
    }
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(fields, tuples);
  }

  /**
   * PostgreSQL does not support multiple catalogs from a single connection, so to reduce confusion
   * we only return the current catalog. {@inheritDoc}
   */
  public ResultSet getCatalogs() throws SQLException {
    return emptyResult(col("TABLE_CAT"));
  }

  public ResultSet getTableTypes() throws SQLException {
    Field f[] = new Field[1];
    List<byte[][]> v = new ArrayList<>();
    f[0] = new Field("TABLE_TYPE", Oid.VARCHAR);
    v.add(new byte[][] {connection.encodeString("SYSTEM TABLE")});
    v.add(new byte[][] {connection.encodeString("TABLE")});
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, v);
  }

  public ResultSet getColumns(String catalog,
                              String schemaPattern,
                              String tableNamePattern,
                              String columnNamePattern) throws SQLException {
    Field[] fields = new Field[24];
    fields[0] = new Field("TABLE_CAT", Oid.VARCHAR);
    fields[1] = new Field("TABLE_SCHEM", Oid.VARCHAR);
    fields[2] = new Field("TABLE_NAME", Oid.VARCHAR);
    fields[3] = new Field("COLUMN_NAME", Oid.VARCHAR);
    fields[4] = new Field("DATA_TYPE", Oid.INT2);
    fields[5] = new Field("TYPE_NAME", Oid.VARCHAR);
    fields[6] = new Field("COLUMN_SIZE", Oid.INT4);
    fields[7] = new Field("BUFFER_LENGTH", Oid.VARCHAR);
    fields[8] = new Field("DECIMAL_DIGITS", Oid.INT4);
    fields[9] = new Field("NUM_PREC_RADIX", Oid.INT4);
    fields[10] = new Field("NULLABLE", Oid.INT4);
    fields[11] = new Field("REMARKS", Oid.VARCHAR);
    fields[12] = new Field("COLUMN_DEF", Oid.VARCHAR);
    fields[13] = new Field("SQL_DATA_TYPE", Oid.INT4);
    fields[14] = new Field("SQL_DATETIME_SUB", Oid.INT4);
    fields[15] = new Field("CHAR_OCTET_LENGTH", Oid.VARCHAR);
    fields[16] = new Field("ORDINAL_POSITION", Oid.INT4);
    fields[17] = new Field("IS_NULLABLE", Oid.VARCHAR);
    fields[18] = new Field("SCOPE_CATLOG", Oid.VARCHAR);
    fields[19] = new Field("SCOPE_SCHEMA", Oid.VARCHAR);
    fields[20] = new Field("SCOPE_TABLE", Oid.VARCHAR);
    fields[21] = new Field("SOURCE_DATA_TYPE", Oid.INT2);
    fields[22] = new Field("IS_AUTOINCREMENT", Oid.VARCHAR);
    fields[23] = new Field("IS_GENERATEDCOLUMN", Oid.VARCHAR);

    String schemaName = getCrateSchemaName();
    String stmt = "select " + schemaName + ", table_name, column_name, data_type, ordinal_position" +
      " from information_schema.columns " +
      createInfoSchemaTableWhereClause(schemaName, schemaPattern, tableNamePattern, columnNamePattern) +
      " and column_name not like '%[%]' and column_name not like '%.%'" +
      " order by " + schemaName + ", table_name, ordinal_position";
    ResultSet rs = connection.createStatement().executeQuery(stmt);

    List<byte[][]> tuples = new ArrayList<>();
    while (rs.next()) {
      byte[][] tuple = new byte[fields.length][];
      tuple[0] = null;
      tuple[1] = rs.getBytes(schemaName);
      tuple[2] = rs.getBytes("table_name");
      tuple[3] = rs.getBytes("column_name");
      String sqlType = rs.getString("data_type");
      tuple[4] = connection.encodeString(Integer.toString(sqlTypeOfCrateType(sqlType)));
      tuple[5] = sqlType.getBytes();
      tuple[6] = null;
      tuple[7] = null;
      tuple[8] = null;
      tuple[9] = connection.encodeString("10");
      tuple[10] = connection.encodeString(Integer.toString(columnNullable));
      tuple[11] = null;
      tuple[12] = null;
      tuple[13] = null;
      tuple[14] = null;
      tuple[15] = null;
      tuple[16] = rs.getBytes("ordinal_position");
      tuple[17] = connection.encodeString("YES");
      tuple[18] = null;
      tuple[19] = null;
      tuple[20] = null;
      tuple[21] = null;
      tuple[22] = connection.encodeString("NO");
      tuple[23] = connection.encodeString("NO");
      tuples.add(tuple);
    }
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(fields, tuples);
  }

  private int sqlTypeOfCrateType(String dataType) {
    switch (dataType) {
      case "byte":
        return Types.TINYINT;
      case "long":
        return Types.BIGINT;
      case "integer":
        return Types.INTEGER;
      case "short":
        return Types.SMALLINT;
      case "float":
        return Types.REAL;
      case "double":
        return Types.DOUBLE;
      case "string":
      case "ip":
        return Types.VARCHAR;
      case "boolean":
        return Types.BOOLEAN;
      case "timestamp":
        return Types.TIMESTAMP;
      case "object":
        return Types.STRUCT;
      case "string_array":
      case "ip_array":
      case "integer_array":
      case "long_array":
      case "short_array":
      case "byte_array":
      case "float_array":
      case "double_array":
      case "timestamp_array":
      case "object_array":
        return Types.ARRAY;
      default:
        return Types.OTHER;
    }
  }

  public ResultSet getColumnPrivileges(String catalog, String schema, String table,
      String columnNamePattern) throws SQLException {
    return emptyResult(
            col("TABLE_CAT"),
            col("TABLE_SCHEM"),
            col("TABLE_NAME"),
            col("COLUMN_NAME"),
            col("GRANTOR"),
            col("GRANTEE"),
            col("PRIVILEGE"),
            col("IS_GRANTABLE"));
  }

  public ResultSet getTablePrivileges(String catalog, String schemaPattern,
      String tableNamePattern) throws SQLException {
    return emptyResult(
            col("TABLE_CAT"),
            col("TABLE_SCHEM"),
            col("TABLE_NAME"),
            col("GRANTOR"),
            col("GRANTEE"),
            col("PRIVILEGE"),
            col("IS_GRANTABLE"));
  }

  private static void sortStringArray(String s[]) {
    for (int i = 0; i < s.length - 1; i++) {
      for (int j = i + 1; j < s.length; j++) {
        if (s[i].compareTo(s[j]) > 0) {
          String tmp = s[i];
          s[i] = s[j];
          s[j] = tmp;
        }
      }
    }
  }

  /**
   * Parse an String of ACLs into a List of ACLs.
   */
  private static List<String> parseACLArray(String aclString) {
    List<String> acls = new ArrayList<String>();
    if (aclString == null || aclString.isEmpty()) {
      return acls;
    }
    boolean inQuotes = false;
    // start at 1 because of leading "{"
    int beginIndex = 1;
    char prevChar = ' ';
    for (int i = beginIndex; i < aclString.length(); i++) {

      char c = aclString.charAt(i);
      if (c == '"' && prevChar != '\\') {
        inQuotes = !inQuotes;
      } else if (c == ',' && !inQuotes) {
        acls.add(aclString.substring(beginIndex, i));
        beginIndex = i + 1;
      }
      prevChar = c;
    }
    // add last element removing the trailing "}"
    acls.add(aclString.substring(beginIndex, aclString.length() - 1));

    // Strip out enclosing quotes, if any.
    for (int i = 0; i < acls.size(); i++) {
      String acl = acls.get(i);
      if (acl.startsWith("\"") && acl.endsWith("\"")) {
        acl = acl.substring(1, acl.length() - 1);
        acls.set(i, acl);
      }
    }
    return acls;
  }

  /**
   * Add the user described by the given acl to the Lists of users with the privileges described by
   * the acl.
   */
  private static void addACLPrivileges(String acl, Map<String, Map<String, List<String[]>>> privileges) {
    int equalIndex = acl.lastIndexOf("=");
    int slashIndex = acl.lastIndexOf("/");
    if (equalIndex == -1) {
      return;
    }

    String user = acl.substring(0, equalIndex);
    String grantor = null;
    if (user.isEmpty()) {
      user = "PUBLIC";
    }
    String privs;
    if (slashIndex != -1) {
      privs = acl.substring(equalIndex + 1, slashIndex);
      grantor = acl.substring(slashIndex + 1, acl.length());
    } else {
      privs = acl.substring(equalIndex + 1, acl.length());
    }

    for (int i = 0; i < privs.length(); i++) {
      char c = privs.charAt(i);
      if (c != '*') {
        String sqlpriv;
        String grantable;
        if (i < privs.length() - 1 && privs.charAt(i + 1) == '*') {
          grantable = "YES";
        } else {
          grantable = "NO";
        }
        switch (c) {
          case 'a':
            sqlpriv = "INSERT";
            break;
          case 'r':
            sqlpriv = "SELECT";
            break;
          case 'w':
            sqlpriv = "UPDATE";
            break;
          case 'd':
            sqlpriv = "DELETE";
            break;
          case 'D':
            sqlpriv = "TRUNCATE";
            break;
          case 'R':
            sqlpriv = "RULE";
            break;
          case 'x':
            sqlpriv = "REFERENCES";
            break;
          case 't':
            sqlpriv = "TRIGGER";
            break;
          // the following can't be granted to a table, but
          // we'll keep them for completeness.
          case 'X':
            sqlpriv = "EXECUTE";
            break;
          case 'U':
            sqlpriv = "USAGE";
            break;
          case 'C':
            sqlpriv = "CREATE";
            break;
          case 'T':
            sqlpriv = "CREATE TEMP";
            break;
          default:
            sqlpriv = "UNKNOWN";
        }

        Map<String, List<String[]>> usersWithPermission = privileges.get(sqlpriv);
        String[] grant = {grantor, grantable};

        if (usersWithPermission == null) {
          usersWithPermission = new HashMap<String, List<String[]>>();
          List<String[]> permissionByGrantor = new ArrayList<String[]>();
          permissionByGrantor.add(grant);
          usersWithPermission.put(user, permissionByGrantor);
          privileges.put(sqlpriv, usersWithPermission);
        } else {
          List<String[]> permissionByGrantor = usersWithPermission.get(user);
          if (permissionByGrantor == null) {
            permissionByGrantor = new ArrayList<String[]>();
            permissionByGrantor.add(grant);
            usersWithPermission.put(user, permissionByGrantor);
          } else {
            permissionByGrantor.add(grant);
          }
        }
      }
    }
  }

  /**
   * Take the a String representing an array of ACLs and return a Map mapping the SQL permission
   * name to a List of usernames who have that permission.
   *
   * @param aclArray ACL array
   * @param owner owner
   * @return a Map mapping the SQL permission name
   */
  public Map<String, Map<String, List<String[]>>> parseACL(String aclArray, String owner) {
    if (aclArray == null) {
      // null acl is a shortcut for owner having full privs
      String perms = "arwdRxt";
      if (connection.haveMinimumServerVersion(ServerVersion.v8_2)) {
        // 8.2 Removed the separate RULE permission
        perms = "arwdxt";
      } else if (connection.haveMinimumServerVersion(ServerVersion.v8_4)) {
        // 8.4 Added a separate TRUNCATE permission
        perms = "arwdDxt";
      }
      aclArray = "{" + owner + "=" + perms + "/" + owner + "}";
    }

    List<String> acls = parseACLArray(aclArray);
    Map<String, Map<String, List<String[]>>> privileges =
        new HashMap<String, Map<String, List<String[]>>>();
    for (String acl : acls) {
      addACLPrivileges(acl, privileges);
    }
    return privileges;
  }

  public ResultSet getBestRowIdentifier(String catalog, String schema, String table,
      int scope, boolean nullable) throws SQLException {
    return emptyResult(
            col("SCOPE"),
            col("COLUMN_NAME"),
            col("DATA_TYPE"),
            col("TYPE_NAME"),
            col("COLUMN_SIZE"),
            col("BUFFER_LENGTH"),
            col("DECIMAL_DIGITS"),
            col("PSEUDO_COLUMNS"));
  }

  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    return emptyResult(
            col("SCOPE", Oid.INT2),
            col("COLUMN_NAME"),
            col("DATA_TYPE", Oid.INT4),
            col("TYPE_NAME"),
            col("COLUMN_SIZE", Oid.INT4),
            col("BUFFER_LENGTH", Oid.INT4),
            col("DECIMAL_DIGITS", Oid.INT2),
            col("PSEUDO_COLUMN", Oid.INT2));
  }

  public ResultSet getPrimaryKeys(String catalog, String schema, String table)
          throws SQLException {
    StringBuilder sql = new StringBuilder("");
    Field[] fields = new Field[6];
    fields[0] = new Field("TABLE_CAT", Oid.VARCHAR);
    fields[1] = new Field("TABLE_SCHEM", Oid.VARCHAR);
    fields[2] = new Field("TABLE_NAME", Oid.VARCHAR);
    fields[3] = new Field("COLUMN_NAME", Oid.VARCHAR);
    fields[4] = new Field("KEY_SEQ", Oid.VARCHAR);
    fields[5] = new Field("PK_NAME", Oid.VARCHAR);

    String schemaName = getCrateSchemaName();
    sql.append("SELECT NULL AS TABLE_CAT, " +
        schemaName + " AS TABLE_SCHEM, " +
        "table_name as TABLE_NAME, " +
        "constraint_name AS COLUMN_NAMES, " +
        "0 AS KEY_SEQ, " +
        "NULL AS PK_NAME " +
        "FROM information_schema.table_constraints " +
        "WHERE '_id' != ANY(constraint_name) ");
    //noinspection StatementWithEmptyBody
    if (schema != null) {
      sql.append("AND " + schemaName + "= '" + connection.escapeString(schema) + "' ");
    }
    sql.append("AND table_name = '" + connection.escapeString(table) + "' ")
            .append("ORDER BY TABLE_SCHEM, TABLE_NAME");
    ResultSet rs = connection.createStatement().executeQuery(sql.toString());

    List<byte[][]> tuples = new ArrayList<>();
    while (rs.next()) {
      byte[] tableCat = rs.getBytes(1);
      byte[] tableSchem = rs.getBytes(2);
      byte[] tableName = rs.getBytes(3);
      byte[] pkName = rs.getBytes(6);
      String[] pkColumsn = (String[]) rs.getArray(4).getArray();
      for (int i = 0; i < pkColumsn.length; i++) {
        byte[][] tuple = new byte[fields.length][];
        tuple[0] = tableCat;
        tuple[1] = tableSchem;
        tuple[2] = tableName;
        tuple[3] = connection.encodeString(pkColumsn[i]);
        tuple[4] = connection.encodeString(Integer.toString(i));
        tuple[5] = pkName;
        tuples.add(tuple);
      }
    }
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(fields, tuples);
  }

  /**
   * @param primaryCatalog primary catalog
   * @param primarySchema primary schema
   * @param primaryTable if provided will get the keys exported by this table
   * @param foreignCatalog foreign catalog
   * @param foreignSchema foreign schema
   * @param foreignTable if provided will get the keys imported by this table
   * @return ResultSet
   * @throws SQLException if something wrong happens
   */
  protected ResultSet getImportedExportedKeys(String primaryCatalog, String primarySchema,
      String primaryTable, String foreignCatalog, String foreignSchema, String foreignTable)
          throws SQLException {
    Field f[] = new Field[14];

    f[0] = new Field("PKTABLE_CAT", Oid.VARCHAR);
    f[1] = new Field("PKTABLE_SCHEM", Oid.VARCHAR);
    f[2] = new Field("PKTABLE_NAME", Oid.VARCHAR);
    f[3] = new Field("PKCOLUMN_NAME", Oid.VARCHAR);
    f[4] = new Field("FKTABLE_CAT", Oid.VARCHAR);
    f[5] = new Field("FKTABLE_SCHEM", Oid.VARCHAR);
    f[6] = new Field("FKTABLE_NAME", Oid.VARCHAR);
    f[7] = new Field("FKCOLUMN_NAME", Oid.VARCHAR);
    f[8] = new Field("KEY_SEQ", Oid.INT2);
    f[9] = new Field("UPDATE_RULE", Oid.INT2);
    f[10] = new Field("DELETE_RULE", Oid.INT2);
    f[11] = new Field("FK_NAME", Oid.VARCHAR);
    f[12] = new Field("PK_NAME", Oid.VARCHAR);
    f[13] = new Field("DEFERRABILITY", Oid.INT2);


    String select;
    String from;
    String where = "";

    /*
     * The addition of the pg_constraint in 7.3 table should have really helped us out here, but it
     * comes up just a bit short. - The conkey, confkey columns aren't really useful without
     * contrib/array unless we want to issues separate queries. - Unique indexes that can support
     * foreign keys are not necessarily added to pg_constraint. Also multiple unique indexes
     * covering the same keys can be created which make it difficult to determine the PK_NAME field.
     */

    if (connection.haveMinimumServerVersion(ServerVersion.v7_4)) {
      String sql =
          "SELECT NULL::text AS PKTABLE_CAT, pkn.nspname AS PKTABLE_SCHEM, pkc.relname AS PKTABLE_NAME, pka.attname AS PKCOLUMN_NAME, "
              + "NULL::text AS FKTABLE_CAT, fkn.nspname AS FKTABLE_SCHEM, fkc.relname AS FKTABLE_NAME, fka.attname AS FKCOLUMN_NAME, "
              + "pos.n AS KEY_SEQ, "
              + "CASE con.confupdtype "
              + " WHEN 'c' THEN " + DatabaseMetaData.importedKeyCascade
              + " WHEN 'n' THEN " + DatabaseMetaData.importedKeySetNull
              + " WHEN 'd' THEN " + DatabaseMetaData.importedKeySetDefault
              + " WHEN 'r' THEN " + DatabaseMetaData.importedKeyRestrict
              + " WHEN 'a' THEN " + DatabaseMetaData.importedKeyNoAction
              + " ELSE NULL END AS UPDATE_RULE, "
              + "CASE con.confdeltype "
              + " WHEN 'c' THEN " + DatabaseMetaData.importedKeyCascade
              + " WHEN 'n' THEN " + DatabaseMetaData.importedKeySetNull
              + " WHEN 'd' THEN " + DatabaseMetaData.importedKeySetDefault
              + " WHEN 'r' THEN " + DatabaseMetaData.importedKeyRestrict
              + " WHEN 'a' THEN " + DatabaseMetaData.importedKeyNoAction
              + " ELSE NULL END AS DELETE_RULE, "
              + "con.conname AS FK_NAME, pkic.relname AS PK_NAME, "
              + "CASE "
              + " WHEN con.condeferrable AND con.condeferred THEN "
              + DatabaseMetaData.importedKeyInitiallyDeferred
              + " WHEN con.condeferrable THEN " + DatabaseMetaData.importedKeyInitiallyImmediate
              + " ELSE " + DatabaseMetaData.importedKeyNotDeferrable
              + " END AS DEFERRABILITY "
              + " FROM "
              + " pg_catalog.pg_namespace pkn, pg_catalog.pg_class pkc, pg_catalog.pg_attribute pka, "
              + " pg_catalog.pg_namespace fkn, pg_catalog.pg_class fkc, pg_catalog.pg_attribute fka, "
              + " pg_catalog.pg_constraint con, ";
      if (connection.haveMinimumServerVersion(ServerVersion.v8_0)) {
        sql += " pg_catalog.generate_series(1, " + getMaxIndexKeys() + ") pos(n), ";
      } else {
        sql += " information_schema._pg_keypositions() pos(n), ";
      }
      sql += " pg_catalog.pg_depend dep, pg_catalog.pg_class pkic "
          + " WHERE pkn.oid = pkc.relnamespace AND pkc.oid = pka.attrelid AND pka.attnum = con.confkey[pos.n] AND con.confrelid = pkc.oid "
          + " AND fkn.oid = fkc.relnamespace AND fkc.oid = fka.attrelid AND fka.attnum = con.conkey[pos.n] AND con.conrelid = fkc.oid "
          + " AND con.contype = 'f' AND con.oid = dep.objid AND pkic.oid = dep.refobjid AND pkic.relkind = 'i' AND dep.classid = 'pg_constraint'::regclass::oid AND dep.refclassid = 'pg_class'::regclass::oid ";
      if (primarySchema != null && !"".equals(primarySchema)) {
        sql += " AND pkn.nspname = " + escapeQuotes(primarySchema);
      }
      if (foreignSchema != null && !"".equals(foreignSchema)) {
        sql += " AND fkn.nspname = " + escapeQuotes(foreignSchema);
      }
      if (primaryTable != null && !"".equals(primaryTable)) {
        sql += " AND pkc.relname = " + escapeQuotes(primaryTable);
      }
      if (foreignTable != null && !"".equals(foreignTable)) {
        sql += " AND fkc.relname = " + escapeQuotes(foreignTable);
      }

      if (primaryTable != null) {
        sql += " ORDER BY fkn.nspname,fkc.relname,con.conname,pos.n";
      } else {
        sql += " ORDER BY pkn.nspname,pkc.relname, con.conname,pos.n";
      }

      return createMetaDataStatement().executeQuery(sql);
    } else if (connection.haveMinimumServerVersion(ServerVersion.v7_3)) {
      select = "SELECT DISTINCT n1.nspname as pnspname,n2.nspname as fnspname, ";
      from = " FROM pg_catalog.pg_namespace n1 "
          + " JOIN pg_catalog.pg_class c1 ON (c1.relnamespace = n1.oid) "
          + " JOIN pg_catalog.pg_index i ON (c1.oid=i.indrelid) "
          + " JOIN pg_catalog.pg_class ic ON (i.indexrelid=ic.oid) "
          + " JOIN pg_catalog.pg_attribute a ON (ic.oid=a.attrelid), "
          + " pg_catalog.pg_namespace n2 "
          + " JOIN pg_catalog.pg_class c2 ON (c2.relnamespace=n2.oid), "
          + " pg_catalog.pg_trigger t1 "
          + " JOIN pg_catalog.pg_proc p1 ON (t1.tgfoid=p1.oid), "
          + " pg_catalog.pg_trigger t2 "
          + " JOIN pg_catalog.pg_proc p2 ON (t2.tgfoid=p2.oid) ";
      if (primarySchema != null && !"".equals(primarySchema)) {
        where += " AND n1.nspname = " + escapeQuotes(primarySchema);
      }
      if (foreignSchema != null && !"".equals(foreignSchema)) {
        where += " AND n2.nspname = " + escapeQuotes(foreignSchema);
      }
    } else {
      select = "SELECT DISTINCT NULL::text as pnspname, NULL::text as fnspname, ";
      from = " FROM pg_class c1 "
          + " JOIN pg_index i ON (c1.oid=i.indrelid) "
          + " JOIN pg_class ic ON (i.indexrelid=ic.oid) "
          + " JOIN pg_attribute a ON (ic.oid=a.attrelid), "
          + " pg_class c2, "
          + " pg_trigger t1 "
          + " JOIN pg_proc p1 ON (t1.tgfoid=p1.oid), "
          + " pg_trigger t2 "
          + " JOIN pg_proc p2 ON (t2.tgfoid=p2.oid) ";
    }

    String sql = select
        + "c1.relname as prelname, "
        + "c2.relname as frelname, "
        + "t1.tgconstrname, "
        + "a.attnum as keyseq, "
        + "ic.relname as fkeyname, "
        + "t1.tgdeferrable, "
        + "t1.tginitdeferred, "
        + "t1.tgnargs,t1.tgargs, "
        + "p1.proname as updaterule, "
        + "p2.proname as deleterule "
        + from
        + "WHERE "
        // isolate the update rule
        + "(t1.tgrelid=c1.oid "
        + "AND t1.tgisconstraint "
        + "AND t1.tgconstrrelid=c2.oid "
        + "AND p1.proname ~ '^RI_FKey_.*_upd$') "

        + "AND "
        // isolate the delete rule
        + "(t2.tgrelid=c1.oid "
        + "AND t2.tgisconstraint "
        + "AND t2.tgconstrrelid=c2.oid "
        + "AND p2.proname ~ '^RI_FKey_.*_del$') "

        + "AND i.indisprimary "
        + where;

    if (primaryTable != null) {
      sql += "AND c1.relname=" + escapeQuotes(primaryTable);
    }
    if (foreignTable != null) {
      sql += "AND c2.relname=" + escapeQuotes(foreignTable);
    }

    sql += "ORDER BY ";

    // orderby is as follows getExported, orders by FKTABLE,
    // getImported orders by PKTABLE
    // getCrossReference orders by FKTABLE, so this should work for both,
    // since when getting crossreference, primaryTable will be defined

    if (primaryTable != null) {
      if (connection.haveMinimumServerVersion(ServerVersion.v7_3)) {
        sql += "fnspname,";
      }
      sql += "frelname";
    } else {
      if (connection.haveMinimumServerVersion(ServerVersion.v7_3)) {
        sql += "pnspname,";
      }
      sql += "prelname";
    }

    sql += ",keyseq";

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(sql);

    // returns the following columns
    // and some example data with a table defined as follows

    // create table people ( id int primary key);
    // create table policy ( id int primary key);
    // create table users ( id int primary key, people_id int references people(id), policy_id int
    // references policy(id))

    // prelname | frelname | tgconstrname | keyseq | fkeyName | tgdeferrable | tginitdeferred
    // 1 | 2 | 3 | 4 | 5 | 6 | 7

    // people | users | <unnamed> | 1 | people_pkey | f | f

    // | tgnargs | tgargs | updaterule | deleterule
    // | 8 | 9 | 10 | 11
    // | 6 | <unnamed>\000users\000people\000UNSPECIFIED\000people_id\000id\000 |
    // RI_FKey_noaction_upd | RI_FKey_noaction_del

    List<byte[][]> tuples = new ArrayList<byte[][]>();

    while (rs.next()) {
      byte tuple[][] = new byte[14][];

      tuple[1] = rs.getBytes(1); // PKTABLE_SCHEM
      tuple[5] = rs.getBytes(2); // FKTABLE_SCHEM
      tuple[2] = rs.getBytes(3); // PKTABLE_NAME
      tuple[6] = rs.getBytes(4); // FKTABLE_NAME
      String fKeyName = rs.getString(5);
      String updateRule = rs.getString(12);

      if (updateRule != null) {
        // Rules look like this RI_FKey_noaction_del so we want to pull out the part between the
        // 'Key_' and the last '_' s

        String rule = updateRule.substring(8, updateRule.length() - 4);

        int action = java.sql.DatabaseMetaData.importedKeyNoAction;

        if (rule == null || "noaction".equals(rule)) {
          action = java.sql.DatabaseMetaData.importedKeyNoAction;
        }
        if ("cascade".equals(rule)) {
          action = java.sql.DatabaseMetaData.importedKeyCascade;
        } else if ("setnull".equals(rule)) {
          action = java.sql.DatabaseMetaData.importedKeySetNull;
        } else if ("setdefault".equals(rule)) {
          action = java.sql.DatabaseMetaData.importedKeySetDefault;
        } else if ("restrict".equals(rule)) {
          action = java.sql.DatabaseMetaData.importedKeyRestrict;
        }

        tuple[9] = connection.encodeString(Integer.toString(action));

      }

      String deleteRule = rs.getString(13);

      if (deleteRule != null) {

        String rule = deleteRule.substring(8, deleteRule.length() - 4);

        int action = java.sql.DatabaseMetaData.importedKeyNoAction;
        if ("cascade".equals(rule)) {
          action = java.sql.DatabaseMetaData.importedKeyCascade;
        } else if ("setnull".equals(rule)) {
          action = java.sql.DatabaseMetaData.importedKeySetNull;
        } else if ("setdefault".equals(rule)) {
          action = java.sql.DatabaseMetaData.importedKeySetDefault;
        } else if ("restrict".equals(rule)) {
          action = java.sql.DatabaseMetaData.importedKeyRestrict;
        }
        tuple[10] = connection.encodeString(Integer.toString(action));
      }


      int keySequence = rs.getInt(6); // KEY_SEQ

      // Parse the tgargs data
      String fkeyColumn = "";
      String pkeyColumn = "";
      String fkName = "";
      // Note, I am guessing at most of this, but it should be close
      // if not, please correct
      // the keys are in pairs and start after the first four arguments
      // the arguments are separated by \000

      String targs = rs.getString(11);

      // args look like this
      // <unnamed>\000ww\000vv\000UNSPECIFIED\000m\000a\000n\000b\000
      // we are primarily interested in the column names which are the last items in the string

      List<String> tokens = tokenize(targs, "\\000");
      if (!tokens.isEmpty()) {
        fkName = tokens.get(0);
      }

      if (fkName.startsWith("<unnamed>")) {
        fkName = targs;
      }

      int element = 4 + (keySequence - 1) * 2;
      if (tokens.size() > element) {
        fkeyColumn = tokens.get(element);
      }

      element++;
      if (tokens.size() > element) {
        pkeyColumn = tokens.get(element);
      }

      tuple[3] = connection.encodeString(pkeyColumn); // PKCOLUMN_NAME
      tuple[7] = connection.encodeString(fkeyColumn); // FKCOLUMN_NAME

      tuple[8] = rs.getBytes(6); // KEY_SEQ
      // FK_NAME this will give us a unique name for the foreign key
      tuple[11] = connection.encodeString(fkName);
      tuple[12] = rs.getBytes(7); // PK_NAME

      // DEFERRABILITY
      int deferrability = java.sql.DatabaseMetaData.importedKeyNotDeferrable;
      boolean deferrable = rs.getBoolean(8);
      boolean initiallyDeferred = rs.getBoolean(9);
      if (deferrable) {
        if (initiallyDeferred) {
          deferrability = java.sql.DatabaseMetaData.importedKeyInitiallyDeferred;
        } else {
          deferrability = java.sql.DatabaseMetaData.importedKeyInitiallyImmediate;
        }
      }
      tuple[13] = connection.encodeString(Integer.toString(deferrability));

      tuples.add(tuple);
    }
    rs.close();
    stmt.close();

    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, tuples);
  }

  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    return emptyResult(
            col("PKTABLE_CAT"),
            col("PKTABLE_SCHEM"),
            col("PKTABLE_NAME"),
            col("PKCOLUMN_NAME"),
            col("FKTABLE_CAT"),
            col("FKTABLE_SCHEM"),
            col("FKTABLE_NAME"),
            col("FKCOLUMN_NAME"),
            col("KEY_SEQ"),
            col("UPDATE_RULE"),
            col("DELETE_RULE"),
            col("FK_NAME"),
            col("PK_NAME"),
            col("DEFERRABILITY"));
  }

  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    return emptyResult(
            col("PKTABLE_CAT"),
            col("PKTABLE_SCHEM"),
            col("PKTABLE_NAME"),
            col("PKCOLUMN_NAME"),
            col("FKTABLE_CAT"),
            col("FKTABLE_SCHEM"),
            col("FKTABLE_NAME"),
            col("FKCOLUMN_NAME"),
            col("KEY_SEQ"),
            col("UPDATE_RULE"),
            col("DELETE_RULE"),
            col("FK_NAME"),
            col("PK_NAME"),
            col("DEFERRABILITY"));
  }

  public ResultSet getCrossReference(String primaryCatalog, String primarySchema,
      String primaryTable, String foreignCatalog, String foreignSchema, String foreignTable)
          throws SQLException {
    return emptyResult(
            col("PKTABLE_CAT"),
            col("PKTABLE_SCHEM"),
            col("PKTABLE_NAME"),
            col("PKCOLUMN_NAME"),
            col("FKTABLE_CAT"),
            col("FKTABLE_SCHEM"),
            col("FKTABLE_NAME"),
            col("FKCOLUMN_NAME"),
            col("KEY_SEQ"),
            col("UPDATE_RULE"),
            col("DELETE_RULE"),
            col("FK_NAME"),
            col("PK_NAME"),
            col("DEFERRABILITY"));
  }

  public ResultSet getTypeInfo() throws SQLException {

    Field f[] = new Field[18];
    List<byte[][]> v = new ArrayList<>(); // The new ResultSet tuple stuff

    byte bNullable[] = connection.encodeString(Integer.toString(typeNullable));
    byte bSearchable[] = connection.encodeString(Integer.toString(typeSearchable));
    byte bPredBasic[] = connection.encodeString(Integer.toString(typePredBasic));
    byte bPredNone[] = connection.encodeString(Integer.toString(typePredNone));
    byte bTrue[] = connection.encodeString("t");
    byte bFalse[] = connection.encodeString("f");
    byte bZero[] = connection.encodeString("0");
    byte b10[] = connection.encodeString("10");

    f[0] = col("TYPE_NAME");
    f[1] = col("DATA_TYPE", Oid.INT2);
    f[2] = col("PRECISION", Oid.INT4);
    f[3] = col("LITERAL_PREFIX");
    f[4] = col("LITERAL_SUFFIX");
    f[5] = col("CREATE_PARAMS");
    f[6] = col("NULLABLE", Oid.INT2);
    f[7] = col("CASE_SENSITIVE", Oid.BOOL);
    f[8] = col("SEARCHABLE", Oid.INT2);
    f[9] = col("UNSIGNED_ATTRIBUTE", Oid.BOOL);
    f[10] = col("FIXED_PREC_SCALE", Oid.BOOL);
    f[11] = col("AUTO_INCREMENT", Oid.BOOL);
    f[12] = col("LOCAL_TYPE_NAME");
    f[13] = col("MINIMUM_SCALE", Oid.INT2);
    f[14] = col("MAXIMUM_SCALE", Oid.INT2);
    f[15] = col("SQL_DATA_TYPE", Oid.INT4);
    f[16] = col("SQL_DATETIME_SUB", Oid.INT4);
    f[17] = col("NUM_PREC_RADIX", Oid.INT4);

    byte[][] row = new byte[18][];
    row[0] = connection.encodeString("byte");
    row[1] = connection.encodeString(Integer.toString(Types.TINYINT));
    row[2] = connection.encodeString("3");
    row[3] = null;
    row[4] = null;
    row[5] = null;
    row[6] = bNullable;
    row[7] = bFalse;
    row[8] = bPredBasic;
    row[9] = bTrue;
    row[10] = bFalse;
    row[11] = bFalse;
    row[12] = row[0];
    row[13] = bZero;
    row[14] = bZero;
    row[15] = null;
    row[16] = null;
    row[17] = b10;
    v.add(row.clone());

    row[0] = connection.encodeString("long");
    row[1] = connection.encodeString(Integer.toString(Types.BIGINT));
    row[2] = connection.encodeString("19");
    row[3] = null;
    row[4] = null;
    row[5] = null;
    row[6] = bNullable;
    row[7] = bFalse;
    row[8] = bPredBasic;
    row[9] = bTrue;
    row[10] = bFalse;
    row[11] = bFalse;
    row[12] = row[0];
    row[13] = bZero;
    row[14] = bZero;
    row[15] = null;
    row[16] = null;
    row[17] = b10;
    v.add(row.clone());

    row[0] = connection.encodeString("integer");
    row[1] = connection.encodeString(Integer.toString(Types.INTEGER));
    row[2] = b10;
    row[3] = null;
    row[4] = null;
    row[5] = null;
    row[6] = bNullable;
    row[7] = bFalse;
    row[8] = bPredBasic;
    row[9] = bTrue;
    row[10] = bFalse;
    row[11] = bFalse;
    row[12] = row[0];
    row[13] = bZero;
    row[14] = bZero;
    row[15] = null;
    row[16] = null;
    row[17] = b10;
    v.add(row.clone());

    row[0] = connection.encodeString("short");
    row[1] = connection.encodeString(Integer.toString(Types.SMALLINT));
    row[2] = connection.encodeString("5");
    row[3] = null;
    row[4] = null;
    row[5] = null;
    row[6] = bNullable;
    row[7] = bFalse;
    row[8] = bPredBasic;
    row[9] = bTrue;
    row[10] = bFalse;
    row[11] = bFalse;
    row[12] = row[0];
    row[13] = bZero;
    row[14] = bZero;
    row[15] = null;
    row[16] = null;
    row[17] = b10;
    v.add(row.clone());

    row[0] = connection.encodeString("float");
    row[1] = connection.encodeString(Integer.toString(Types.REAL));
    row[2] = connection.encodeString("7");
    row[3] = null;
    row[4] = null;
    row[5] = null;
    row[6] = bNullable;
    row[7] = bFalse;
    row[8] = bPredBasic;
    row[9] = bTrue;
    row[10] = bFalse;
    row[11] = bFalse;
    row[12] = row[0];
    row[13] = bZero;
    row[14] = connection.encodeString("6");
    row[15] = null;
    row[16] = null;
    row[17] = b10;
    v.add(row.clone());

    row[0] = connection.encodeString("double");
    row[1] = connection.encodeString(Integer.toString(Types.DOUBLE));
    row[2] = connection.encodeString("15");
    row[3] = null;
    row[4] = null;
    row[5] = null;
    row[6] = bNullable;
    row[7] = bFalse;
    row[8] = bPredBasic;
    row[9] = bTrue;
    row[10] = bFalse;
    row[11] = bFalse;
    row[12] = row[0];
    row[13] = bZero;
    row[14] = connection.encodeString("14");
    row[15] = null;
    row[16] = null;
    row[17] = b10;
    v.add(row.clone());

    row[0] = connection.encodeString("string");
    row[1] = connection.encodeString(Integer.toString(Types.VARCHAR));
    row[2] = null;
    row[3] = null;
    row[4] = null;
    row[5] = null;
    row[6] = bNullable;
    row[7] = bTrue;
    row[8] = bSearchable;
    row[9] = bTrue;
    row[10] = bFalse;
    row[11] = bFalse;
    row[12] = row[0];
    row[13] = bZero;
    row[14] = bZero;
    row[15] = null;
    row[16] = null;
    row[17] = b10;
    v.add(row.clone());

    row[0] = connection.encodeString("ip");
    row[1] = connection.encodeString(Integer.toString(Types.VARCHAR));
    row[2] = connection.encodeString("15");
    row[3] = null;
    row[4] = null;
    row[5] = null;
    row[6] = bNullable;
    row[7] = bFalse;
    row[8] = bSearchable;
    row[9] = bTrue;
    row[10] = bFalse;
    row[11] = bFalse;
    row[12] = row[0];
    row[13] = bZero;
    row[14] = bZero;
    row[15] = null;
    row[16] = null;
    row[17] = b10;
    v.add(row.clone());

    row[0] = connection.encodeString("boolean");
    row[1] = connection.encodeString(Integer.toString(Types.BOOLEAN));
    row[2] = null;
    row[3] = null;
    row[4] = null;
    row[5] = null;
    row[6] = bNullable;
    row[7] = bFalse;
    row[8] = bPredBasic;
    row[9] = bTrue;
    row[10] = bFalse;
    row[11] = bFalse;
    row[12] = row[0];
    row[13] = bZero;
    row[14] = bZero;
    row[15] = null;
    row[16] = null;
    row[17] = b10;
    v.add(row.clone());

    row[0] = connection.encodeString("timestamp");
    row[1] = connection.encodeString(Integer.toString(Types.TIMESTAMP));
    row[2] = null;
    row[3] = null;
    row[4] = null;
    row[5] = null;
    row[6] = bNullable;
    row[7] = bTrue;
    row[8] = bPredBasic;
    row[9] = bTrue;
    row[10] = bFalse;
    row[11] = bFalse;
    row[12] = row[0];
    row[13] = bZero;
    row[14] = bZero;
    row[15] = null;
    row[16] = null;
    row[17] = b10;
    v.add(row.clone());

    row[0] = connection.encodeString("object");
    row[1] = connection.encodeString(Integer.toString(Types.STRUCT));
    row[2] = null;
    row[3] = null;
    row[4] = null;
    row[5] = null;
    row[6] = bNullable;
    row[7] = bFalse;
    row[8] = bPredNone;
    row[9] = bTrue;
    row[10] = bFalse;
    row[11] = bFalse;
    row[12] = row[0];
    row[13] = bZero;
    row[14] = bZero;
    row[15] = null;
    row[16] = null;
    row[17] = b10;
    v.add(row.clone());

    String[] arrayTypes = new String[]{"string_array", "ip_array", "long_array",
            "integer_array", "short_array", "boolean_array", "byte_array",
            "float_array", "double_array", "object_array"};
    for (int i = 11; i < 11 + arrayTypes.length; i++) {
      row[0] = connection.encodeString(arrayTypes[i - 11]);
      row[1] = connection.encodeString(Integer.toString(Types.ARRAY));
      row[2] = null;
      row[3] = null;
      row[4] = null;
      row[5] = null;
      row[6] = bNullable;
      row[7] = bFalse;
      row[8] = bPredNone;
      row[9] = bTrue;
      row[10] = bFalse;
      row[11] = bFalse;
      row[12] = row[0];
      row[13] = bZero;
      row[14] = bZero;
      row[15] = null;
      row[16] = null;
      row[17] = b10;
      v.add(row.clone());
    }

    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, v);
  }

  public ResultSet getIndexInfo(String catalog, String schema, String tableName,
      boolean unique, boolean approximate) throws SQLException {
    return emptyResult(
            col("TABLE_CAT"),
            col("TABLE_SCHEM"),
            col("TABLE_NAME"),
            col("NON_UNIQUE"),
            col("INDEX_QUALIFIER"),
            col("INDEX_NAME"),
            col("TYPE"),
            col("ORDINAL_POSITION"),
            col("COLUMN_NAME"),
            col("ASC_OR_DESC"),
            col("CARDINALITY"),
            col("PAGES"),
            col("FILTER_CONDITION"));
  }

  /**
   * Tokenize based on words not on single characters.
   */
  private static List<String> tokenize(String input, String delimiter) {
    List<String> result = new ArrayList<String>();
    int start = 0;
    int end = input.length();
    int delimiterSize = delimiter.length();

    while (start < end) {
      int delimiterIndex = input.indexOf(delimiter, start);
      if (delimiterIndex < 0) {
        result.add(input.substring(start));
        break;
      } else {
        String token = input.substring(start, delimiterIndex);
        result.add(token);
        start = delimiterIndex + delimiterSize;
      }
    }
    return result;
  }

  // ** JDBC 2 Extensions **

  public boolean supportsResultSetType(int type) throws SQLException {
    // The only type we don't support
    return type != ResultSet.TYPE_SCROLL_SENSITIVE;
  }


  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    // These combinations are not supported!
    if (type == ResultSet.TYPE_SCROLL_SENSITIVE) {
      return false;
    }

    // We do support Updateable ResultSets
    if (concurrency == ResultSet.CONCUR_UPDATABLE) {
      return true;
    }

    // Everything else we do
    return true;
  }


  /* lots of unsupported stuff... */
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    return true;
  }

  public boolean ownDeletesAreVisible(int type) throws SQLException {
    return true;
  }

  public boolean ownInsertsAreVisible(int type) throws SQLException {
    // indicates that
    return true;
  }

  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    return false;
  }

  public boolean othersDeletesAreVisible(int i) throws SQLException {
    return false;
  }

  public boolean othersInsertsAreVisible(int type) throws SQLException {
    return false;
  }

  public boolean updatesAreDetected(int type) throws SQLException {
    return false;
  }

  public boolean deletesAreDetected(int i) throws SQLException {
    return false;
  }

  public boolean insertsAreDetected(int type) throws SQLException {
    return false;
  }

  public boolean supportsBatchUpdates() throws SQLException {
    return true;
  }

  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern,
      int[] types) throws SQLException {
    return emptyResult(
            col("TYPE_CAT"),
            col("TYPE_SCHEM"),
            col("TYPE_NAME"),
            col("CLASS_NAME"),
            col("DATA_TYPE"),
            col("REMARKS"),
            col("BASE_TYPE"));
  }


  public java.sql.Connection getConnection() throws SQLException {
    return connection;
  }

  /* I don't find these in the spec!?! */

  public boolean rowChangesAreDetected(int type) throws SQLException {
    return false;
  }

  public boolean rowChangesAreVisible(int type) throws SQLException {
    return false;
  }

  protected java.sql.Statement createMetaDataStatement() throws SQLException {
    return connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.CONCUR_READ_ONLY);
  }

  public long getMaxLogicalLobSize() throws SQLException {
    return 0;
  }

  public boolean supportsRefCursors() throws SQLException {
    return true;
  }

  public RowIdLifetime getRowIdLifetime() throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "getRowIdLifetime()");
  }

  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return true;
  }

  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
  }

  public ResultSet getClientInfoProperties() throws SQLException {
    return emptyResult(
            col("NAME"),
            col("MAX_LEN", Oid.INT4),
            col("DEFAULT_VALUE"),
            col("DESCRIPTION"));
  }

  public boolean providesQueryObjectGenerator() throws SQLException {
    return false;
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {
    return emptyResult(
            col("FUNCTION_CAT"),
            col("FUNCTION_SCHEM"),
            col("FUNCTION_NAME"),
            col("REMARKS"),
            col("FUNCTION_TYPE", Oid.INT2),
            col("SPECIFIC_NAME"));
  }

  public ResultSet getFunctionColumns(String catalog, String schemaPattern,
      String functionNamePattern, String columnNamePattern) throws SQLException {
    return emptyResult(
            col("FUNCTION_CAT"),
            col("FUNCTION_SCHEM"),
            col("FUNCTION_NAME"),
            col("COLUMN_NAME"),
            col("COLUMN_TYPE", Oid.INT2),
            col("DATA_TYPE", Oid.INT4),
            col("TYPE_NAME"),
            col("PRECISION", Oid.INT4),
            col("LENGTH", Oid.INT4),
            col("SCALE", Oid.INT2),
            col("RADIX", Oid.INT2),
            col("NULLABLE", Oid.INT2),
            col("REMARKS"),
            col("CHAR_OCTET_LENGTH", Oid.INT4),
            col("ORDINAL_POSITION", Oid.INT4),
            col("IS_NULLABLE"),
            col("SPECIFIC_NAME"));
  }

  public int getJDBCMajorVersion() throws SQLException {
    // FIXME: dependent on JDBC version
    return 4;
  }

  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
      String columnNamePattern) throws SQLException {
    return emptyResult(
            col("TABLE_CAT"),
            col("TABLE_SCHEM"),
            col("TABLE_NAME"),
            col("COLUMN_NAME"),
            col("DATA_TYPE", Oid.INT4),
            col("COLUMN_SIZE", Oid.INT4),
            col("DECIMAL_DIGITS", Oid.INT4),
            col("NUM_PREC_RADIX", Oid.INT4),
            col("COLUMN_USAGE"),
            col("REMARKS"),
            col("CHAR_OCTET_LENGTH", Oid.INT4),
            col("IS_NULLABLE"));
  }

  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return true;
  }

  public boolean supportsSavepoints() throws SQLException {
    return connection.haveMinimumServerVersion(ServerVersion.v8_0);
  }

  public boolean supportsNamedParameters() throws SQLException {
    return false;
  }

  public boolean supportsMultipleOpenResults() throws SQLException {
    return false;
  }

  public boolean supportsGetGeneratedKeys() throws SQLException {
    // We don't support returning generated keys by column index,
    // but that should be a rarer case than the ones we do support.
    //
    return connection.haveMinimumServerVersion(ServerVersion.v8_2);
  }

  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    return emptyResult(
            col("TYPE_CAT"),
            col("TYPE_SCHEM"),
            col("TYPE_NAME"),
            col("SUPERTYPE_CAT"),
            col("SUPERTYPE_SCHEM"),
            col("SUPERTYPE_NAME"));
  }

  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    return emptyResult(
            col("TABLE_CAT"),
            col("TABLE_SCHEM"),
            col("TABLE_NAME"),
            col("SUPERTABLE_NAME"));
  }

  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
      String attributeNamePattern) throws SQLException {
    return emptyResult(
            col("TYPE_CAT"),
            col("TYPE_SCHEM"),
            col("TYPE_NAME"),
            col("ATTR_NAME"),
            col("DATA_TYPE"),
            col("ATTR_TYPE_NAME"),
            col("ATTR_SIZE"),
            col("DECIMAL_DIGITS"),
            col("NUM_PREC_RADIX"),
            col("NULLABLE"),
            col("REMARKS"),
            col("ATTR_DEF"),
            col("SQL_DATA_TYPE"),
            col("SQL_DATETIME_SUB"),
            col("CHAR_OCTET_LENGTH"),
            col("ORDINAL_POSITION"),
            col("IS_NULLABLE"),
            col("SCOPE_CATALOG"),
            col("SCOPE_SCHEMA"),
            col("SCOPE_TABLE"),
            col("SOURCE_DATA_TYPE"));
  }

  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return true;
  }

  public int getResultSetHoldability() throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  public int getDatabaseMajorVersion() throws SQLException {
    return connection.getServerMajorVersion();
  }

  public int getDatabaseMinorVersion() throws SQLException {
    return connection.getServerMinorVersion();
  }

  public int getJDBCMinorVersion() throws SQLException {
    return 0; // This class implements JDBC 3.0
  }

  public int getSQLStateType() throws SQLException {
    return sqlStateSQL99;
  }

  public boolean locatorsUpdateCopy() throws SQLException {
    /*
     * Currently LOB's aren't updateable at all, so it doesn't matter what we return. We don't throw
     * the notImplemented Exception because the 1.5 JDK's CachedRowSet calls this method regardless
     * of whether large objects are used.
     */
    return true;
  }

  public boolean supportsStatementPooling() throws SQLException {
    return false;
  }

  // ********************************************************
  // END OF PUBLIC INTERFACE
  // ********************************************************


  private static Field col(String name, int oid) {
    return new Field(name, oid);
  }

  private static Field col(String name) {
    return new Field(name, Oid.VARCHAR);
  }

  private final ResultSet emptyResult(Field... fields) throws SQLException {
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(fields, Collections.<byte[][]>emptyList());
  }
}
