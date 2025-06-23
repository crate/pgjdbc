/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.jdbc;

import org.postgresql.Driver;
import org.postgresql.core.BaseStatement;
import org.postgresql.core.Field;
import org.postgresql.core.Oid;
import org.postgresql.core.ServerVersion;
import org.postgresql.core.Tuple;
import org.postgresql.core.TypeInfo;
import org.postgresql.util.DriverInfo;
import org.postgresql.util.GT;
import org.postgresql.util.JdbcBlackHole;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PgDatabaseMetaData implements DatabaseMetaData {

  private static final Map<String, String> REFERENCE_GENERATIONS;

  static {
    Map<String, String> referenceMaps = new HashMap<>();
    referenceMaps.put("SYSTEM GENERATED", "SYSTEM");
    referenceMaps.put("USER GENERATED","USER");
    REFERENCE_GENERATIONS = Collections.unmodifiableMap(referenceMaps);
  }

  public PgDatabaseMetaData(PgConnection conn) {
    this.connection = conn;
  }

  private @Nullable String keywords;

  protected final PgConnection connection; // The connection association

  private int nameDataLength; // length for name datatype
  private int indexMaxKeys; // maximum number of keys in an index.

  protected int getMaxIndexKeys() throws SQLException {
    if (indexMaxKeys == 0) {
      String sql;
      sql = "SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys'";

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
        indexMaxKeys = rs.getInt(1);
      } finally {
        JdbcBlackHole.close(rs);
        JdbcBlackHole.close(stmt);
      }
    }
    return indexMaxKeys;
  }

  protected int getMaxNameLength() throws SQLException {
    if (nameDataLength == 0) {
      String sql;
      sql = "SELECT t.typlen FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n "
            + "WHERE t.typnamespace=n.oid AND t.typname='name' AND n.nspname='pg_catalog'";

      Statement stmt = connection.createStatement();
      ResultSet rs = null;
      try {
        rs = stmt.executeQuery(sql);
        if (!rs.next()) {
          throw new PSQLException(GT.tr("Unable to find name datatype in the system catalogs."),
              PSQLState.UNEXPECTED_ERROR);
        }
        nameDataLength = rs.getInt("typlen");
      } finally {
        JdbcBlackHole.close(rs);
        JdbcBlackHole.close(stmt);
      }
    }
    return nameDataLength - 1;
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    return true; // For now...
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    return true; // For now...
  }

  @Override
  public String getURL() throws SQLException {
    return connection.getURL();
  }

  @Override
  public String getUserName() throws SQLException {
    return connection.getUserName();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return connection.isReadOnly();
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    return true;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    return false;
  }

  /**
   * Retrieves the name of this database product. We hope that it is CrateDB, so we return that
   * explicitly.
   *
   * @return "Crate"
   */
  @Override
  public String getDatabaseProductName() throws SQLException {
    return "Crate";
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return connection.getDBVersionNumber();
  }

  @Override
  public String getDriverName() {
    return DriverInfo.DRIVER_NAME;
  }

  @Override
  public String getDriverVersion() {
    return DriverInfo.DRIVER_VERSION;
  }

  @Override
  public int getDriverMajorVersion() {
    return DriverInfo.MAJOR_VERSION;
  }

  @Override
  public int getDriverMinorVersion() {
    return DriverInfo.MINOR_VERSION;
  }

  /**
   * Does the database store tables in a local file? No - it stores them in a file on the server.
   *
   * @return true if so
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean usesLocalFiles() throws SQLException {
    return false;
  }

  /**
   * Does the database use a file for each table? Well, not really, since it doesn't use local files.
   *
   * @return true if so
   * @throws SQLException if a database access error occurs
   */
  @Override
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
  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return true;
  }

  @Override
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
  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
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
  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return "\"";
  }

  /**
   * {@inheritDoc}
   *
   * <p>From PostgreSQL 9.0+ return the keywords from pg_catalog.pg_get_keywords()</p>
   *
   * @return a comma separated list of keywords we use
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getSQLKeywords() throws SQLException {
    connection.checkClosed();
    return "alias,all,alter,analyzer,and,any,array,as,asc,"
      + "always,array,add,"
      + "bernoulli,between,blob,boolean,by,byte,begin,"
      + "case,cast,catalogs,char_filters,clustered,coalesce,columns,"
      + "constraint,copy,create,cross,current,current_date,current_time,"
      + "current_timestamp,current_schema, column,"
      + "date,day,delete,desc,describe,directory,distinct,distributed,"
      + "double,drop,dynamic,delete,duplicate,default,"
      + "else,end,escape,except,exists,explain,extends,extract,"
      + "false,first,float,following,for,format,from,full,fulltext,functions,"
      + "graphviz,group,geo_point,geo_shape,global,generated,"
      + "having,hour,"
      + "if,ignored,in,index,inner,insert,int,integer,intersect,interval,"
      + "into,ip,is,isolation,"
      + "join,"
      + "last,left,like,limit,logical,long,local,level,"
      + "materialized,minute,month,match,"
      + "natural,not,null,nulls,"
      + "object,off,offset,on,or,order,outer,over,optmize,only,"
      + "partition,partitioned,partitions,plain,preceding,primary_key,"
      + "range,recursive,refresh,reset,right,row,rows,repository,restore,"
      + "schemas,second,select,set,shards,short,show,some,stratify,"
      + "strict,string_type,substring,system,select,snapshot,session,"
      + "table,tables,tablesample,text,then,time,timestamp,to,tokenizer,"
      + "token_filters,true,type,try_cast,transaction,tablesample,"
      + "transient,"
      + "unbounded,union,update,using,"
      + "values,view,"
      + "when,where,with,"
      + "year";
  }

  @Override
  @SuppressWarnings("deprecation")
  public String getNumericFunctions() throws SQLException {
    return "abs,ceil,floor,ln,log,random,round,sqrt,sin,asin,cos,"
      + "acos,tan,atan";
  }

  @Override
  @SuppressWarnings("deprecation")
  public String getStringFunctions() throws SQLException {
    return "concat,format,substr,char_length,bit_length,octet_length,"
      + "lower,upper";
  }

  @Override
  @SuppressWarnings("deprecation")
  public String getSystemFunctions() throws SQLException {
    return "";
  }

  @Override
  @SuppressWarnings("deprecation")
  public String getTimeDateFunctions() throws SQLException {
    return "date_trunc,extract,date_format";
  }

  @Override
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
   * <p>Postgresql allows any high-bit character to be used in an unquoted identifier, so we can't
   * possibly list them all.</p>
   *
   * <p>From the file src/backend/parser/scan.l, an identifier is ident_start [A-Za-z\200-\377_]
   * ident_cont [A-Za-z\200-\377_0-9\$] identifier {ident_start}{ident_cont}*</p>
   *
   * @return a string containing the extra characters
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getExtraNameCharacters() throws SQLException {
    return "";
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 6.1+
   */
  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.3+
   */
  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 6.4+
   */
  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 6.4+
   */
  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    return true;
  }

  /*
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 6.4+
   */
  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return true;
  }

  /*
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.1+
   */
  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This grammar is defined at:
   * <a href="http://www.microsoft.com/msdn/sdk/platforms/doc/odbc/src/intropr.htm">
   *     http://www.microsoft.com/msdn/sdk/platforms/doc/odbc/src/intropr.htm</a></p>
   *
   * <p>In Appendix C. From this description, we seem to support the ODBC minimal (Level 0) grammar.</p>
   *
   * @return true
   */
  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return true;
  }

  /**
   * Does this driver support the Core ODBC SQL grammar. We need SQL-92 conformance for this.
   *
   * @return false
   * @throws SQLException if a database access error occurs
   */
  @Override
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
  @Override
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
  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return false
   */
  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * @return false
   */
  @Override
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
  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.1+
   */
  @Override
  public boolean supportsOuterJoins() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.1+
   */
  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.1+
   */
  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   * <p>PostgreSQL doesn't have schemas, but when it does, we'll use the term "schema".</p>
   *
   * @return {@code "schema"}
   */
  @Override
  public String getSchemaTerm() throws SQLException {
    return "schema";
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code "function"}
   */
  @Override
  public String getProcedureTerm() throws SQLException {
    return "function";
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code "database"}
   */
  @Override
  public String getCatalogTerm() throws SQLException {
    return "database";
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    return true;
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return ".";
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.3+
   */
  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.3+
   */
  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.3+
   */
  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.3+
   */
  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.3+
   */
  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  /**
   * We support cursors for gets only it seems. I dont see a method to get a positioned delete.
   *
   * @return false
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return false; // For now...
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return false; // For now...
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 6.5+
   */
  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.1+
   */
  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 6.3+
   */
  @Override
  public boolean supportsUnion() throws SQLException {
    return true; // since 6.3
  }

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 7.1+
   */
  @Override
  public boolean supportsUnionAll() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc} In PostgreSQL, Cursors are only open within transactions.
   */
  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return false;
  }

  /**
   * {@inheritDoc}
   * <p>Can statements remain open across commits? They may, but this driver cannot guarantee that. In
   * further reflection. we are talking a Statement object here, so the answer is yes, since the
   * Statement is only a vehicle to ExecSQL()</p>
   *
   * @return true
   */
  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   * <p>Can statements remain open across rollbacks? They may, but this driver cannot guarantee that.
   * In further contemplation, we are talking a Statement object here, so the answer is yes, since
   * the Statement is only a vehicle to ExecSQL() in Connection</p>
   *
   * @return true
   */
  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return true;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    return 0; // no limit
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    return 0; // no limit
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return getMaxNameLength();
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    return 0; // no limit
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    return getMaxIndexKeys();
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    return 0; // no limit
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    return 0; // no limit
  }

  /**
   * {@inheritDoc} What is the maximum number of columns in a table? From the CREATE TABLE reference
   * page...
   *
   * <p>"The new class is created as a heap with no initial data. A class can have no more than 1600
   * attributes (realistically, this is limited by the fact that tuple sizes must be less than 8192
   * bytes)..."</p>
   *
   * @return the max columns
   * @throws SQLException if a database access error occurs
   */
  @Override
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
  @Override
  public int getMaxConnections() throws SQLException {
    return 8192;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    return getMaxNameLength();
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    return 0; // no limit (larger than an int anyway)
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    return getMaxNameLength();
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    return getMaxNameLength();
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    return getMaxNameLength();
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    return 1073741824; // 1 GB
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return false;
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    return 0; // actually whatever fits in size_t
  }

  @Override
  public int getMaxStatements() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    return getMaxNameLength();
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    return 0; // no limit
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    return getMaxNameLength();
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    if (connection.isStrict()) {
      return Connection.TRANSACTION_NONE;
    } else {
      return Connection.TRANSACTION_READ_COMMITTED;
    }
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return !connection.isStrict();
  }

  /**
   * {@inheritDoc}
   * <p>We only support TRANSACTION_SERIALIZABLE and TRANSACTION_READ_COMMITTED before 8.0; from 8.0
   * READ_UNCOMMITTED and REPEATABLE_READ are accepted aliases for READ_COMMITTED.</p>
   */
  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    if (connection.isStrict()) {
      return level == Connection.TRANSACTION_NONE;
    } else {
      switch (level) {
        case Connection.TRANSACTION_READ_UNCOMMITTED:
        case Connection.TRANSACTION_READ_COMMITTED:
        case Connection.TRANSACTION_REPEATABLE_READ:
        case Connection.TRANSACTION_SERIALIZABLE:
          return true;
        default:
          return false;
      }
    }
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return !connection.isStrict();
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return false;
  }

  /**
   * <p>Does a data definition statement within a transaction force the transaction to commit? It seems
   * to mean something like:</p>
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
   * <p>Does the CREATE TABLE call cause a commit? The answer is no.</p>
   *
   * @return true if so
   * @throws SQLException if a database access error occurs
   */
  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return false;
  }

  /**
   * Turn the provided value into a valid string literal for direct inclusion into a query. This
   * includes the single quotes needed around it.
   *
   * @param s input value
   *
   * @return string literal for direct inclusion into a query
   * @throws SQLException if something wrong happens
   */
  protected String escapeQuotes(String s) throws SQLException {
    StringBuilder sb = new StringBuilder();
    if (!connection.getStandardConformingStrings()) {
      sb.append("E");
    }
    sb.append("'");
    sb.append(connection.escapeString(s));
    sb.append("'");
    return sb.toString();
  }

  @Override
  public ResultSet getProcedures(@Nullable String catalog, @Nullable String schemaPattern,
      @Nullable String procedureNamePattern)
      throws SQLException {
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

  @Override
  public ResultSet getProcedureColumns(@Nullable String catalog, @Nullable String schemaPattern,
      @Nullable String procedureNamePattern, @Nullable String columnNamePattern)
      throws SQLException {
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

  @Override
  public ResultSet getTables(@Nullable String catalog, @Nullable String schemaPattern,
      @Nullable String tableNamePattern, String @Nullable [] types) throws SQLException {
    Field[] fields = new Field[10];
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
    String infoSchemaTableWhereClause = createInfoSchemaTableWhereClause(schemaName, schemaPattern, tableNamePattern,
            null).toString();
    StringBuilder stmt = getTablesStatement(schemaName, infoSchemaTableWhereClause);

    ResultSet rs = connection.createStatement().executeQuery(stmt.toString());

    List<Tuple> tuples = new ArrayList<>();
    while (rs.next()) {
      Tuple tuple = new Tuple(fields.length);
      String schema = rs.getString(schemaName);

      tuple.set(1, schema.getBytes());
      tuple.set(2, rs.getBytes("table_name"));
      tuple.set(4, connection.encodeString(""));
      tuple.set(5, null);
      tuple.set(6, null);
      tuple.set(7, null);
      if (getCrateVersion().before("2.0.0")) {
        tuple.set(0, null);
        if ("sys".equals(schema) || "information_schema".equals(schema)) {
          tuple.set(3, connection.encodeString("SYSTEM TABLE"));
        } else {
          tuple.set(3, connection.encodeString("TABLE"));
        }
        tuple.set(8, connection.encodeString("_id"));
        tuple.set(9, connection.encodeString("SYSTEM"));
      } else {
        tuple.set(0, rs.getBytes("table_catalog"));
        tuple.set(3, rs.getBytes("table_type"));
        tuple.set(8, rs.getBytes("self_referencing_column_name"));
        tuple.set(9, connection.encodeString(getReferenceGeneration(rs.getString("reference_generation"))));
      }
      tuples.add(tuple);
    }
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(fields, tuples);
  }

  private StringBuilder getTablesStatement(String schemaName, String infoSchemaTableWhereClause) throws SQLException {
    StringBuilder builder = new StringBuilder("SELECT ")
            .append(schemaName)
            .append(", table_name");
    if (getCrateVersion().compareTo("2.0.0") >= 0) {
      builder.append(", table_catalog, table_type, self_referencing_column_name, reference_generation");
    }
    builder.append(" FROM information_schema.tables")
            .append(infoSchemaTableWhereClause);
    if (getCrateVersion().compareTo("2.0.0") >= 0) {
      builder.append("AND table_type = 'BASE TABLE'");
    }
    builder.append(" ORDER BY ")
            .append(schemaName)
            .append(", table_name");
    return builder;
  }

  private static String getReferenceGeneration(String referenceGeneration) {
    if (REFERENCE_GENERATIONS.containsKey(referenceGeneration)) {
      return REFERENCE_GENERATIONS.get(referenceGeneration);
    } else {
      return null;
    }
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
    tableTypeClauses = new HashMap<>();
    Map<String, String> ht = new HashMap<>();
    tableTypeClauses.put("TABLE", ht);
    ht.put("SCHEMAS", "c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'");
    ht.put("NOSCHEMAS", "c.relkind = 'r' AND c.relname !~ '^pg_'");
    ht = new HashMap<>();
    tableTypeClauses.put("PARTITIONED TABLE", ht);
    ht.put("SCHEMAS", "c.relkind = 'p' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'");
    ht.put("NOSCHEMAS", "c.relkind = 'p' AND c.relname !~ '^pg_'");
    ht = new HashMap<>();
    tableTypeClauses.put("VIEW", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'v' AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema'");
    ht.put("NOSCHEMAS", "c.relkind = 'v' AND c.relname !~ '^pg_'");
    ht = new HashMap<>();
    tableTypeClauses.put("INDEX", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'i' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'");
    ht.put("NOSCHEMAS", "c.relkind = 'i' AND c.relname !~ '^pg_'");
    ht = new HashMap<>();
    tableTypeClauses.put("PARTITIONED INDEX", ht);
    ht.put("SCHEMAS", "c.relkind = 'I' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'");
    ht.put("NOSCHEMAS", "c.relkind = 'I' AND c.relname !~ '^pg_'");
    ht = new HashMap<>();
    tableTypeClauses.put("SEQUENCE", ht);
    ht.put("SCHEMAS", "c.relkind = 'S'");
    ht.put("NOSCHEMAS", "c.relkind = 'S'");
    ht = new HashMap<>();
    tableTypeClauses.put("TYPE", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'c' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'");
    ht.put("NOSCHEMAS", "c.relkind = 'c' AND c.relname !~ '^pg_'");
    ht = new HashMap<>();
    tableTypeClauses.put("SYSTEM TABLE", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'r' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema')");
    ht.put("NOSCHEMAS",
        "c.relkind = 'r' AND c.relname ~ '^pg_' AND c.relname !~ '^pg_toast_' AND c.relname !~ '^pg_temp_'");
    ht = new HashMap<>();
    tableTypeClauses.put("SYSTEM TOAST TABLE", ht);
    ht.put("SCHEMAS", "c.relkind = 'r' AND n.nspname = 'pg_toast'");
    ht.put("NOSCHEMAS", "c.relkind = 'r' AND c.relname ~ '^pg_toast_'");
    ht = new HashMap<>();
    tableTypeClauses.put("SYSTEM TOAST INDEX", ht);
    ht.put("SCHEMAS", "c.relkind = 'i' AND n.nspname = 'pg_toast'");
    ht.put("NOSCHEMAS", "c.relkind = 'i' AND c.relname ~ '^pg_toast_'");
    ht = new HashMap<>();
    tableTypeClauses.put("SYSTEM VIEW", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'v' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema') ");
    ht.put("NOSCHEMAS", "c.relkind = 'v' AND c.relname ~ '^pg_'");
    ht = new HashMap<>();
    tableTypeClauses.put("SYSTEM INDEX", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'i' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema') ");
    ht.put("NOSCHEMAS",
        "c.relkind = 'v' AND c.relname ~ '^pg_' AND c.relname !~ '^pg_toast_' AND c.relname !~ '^pg_temp_'");
    ht = new HashMap<>();
    tableTypeClauses.put("TEMPORARY TABLE", ht);
    ht.put("SCHEMAS", "c.relkind IN ('r','p') AND n.nspname ~ '^pg_temp_' ");
    ht.put("NOSCHEMAS", "c.relkind IN ('r','p') AND c.relname ~ '^pg_temp_' ");
    ht = new HashMap<>();
    tableTypeClauses.put("TEMPORARY INDEX", ht);
    ht.put("SCHEMAS", "c.relkind = 'i' AND n.nspname ~ '^pg_temp_' ");
    ht.put("NOSCHEMAS", "c.relkind = 'i' AND c.relname ~ '^pg_temp_' ");
    ht = new HashMap<>();
    tableTypeClauses.put("TEMPORARY VIEW", ht);
    ht.put("SCHEMAS", "c.relkind = 'v' AND n.nspname ~ '^pg_temp_' ");
    ht.put("NOSCHEMAS", "c.relkind = 'v' AND c.relname ~ '^pg_temp_' ");
    ht = new HashMap<>();
    tableTypeClauses.put("TEMPORARY SEQUENCE", ht);
    ht.put("SCHEMAS", "c.relkind = 'S' AND n.nspname ~ '^pg_temp_' ");
    ht.put("NOSCHEMAS", "c.relkind = 'S' AND c.relname ~ '^pg_temp_' ");
    ht = new HashMap<>();
    tableTypeClauses.put("FOREIGN TABLE", ht);
    ht.put("SCHEMAS", "c.relkind = 'f'");
    ht.put("NOSCHEMAS", "c.relkind = 'f'");
    ht = new HashMap<>();
    tableTypeClauses.put("MATERIALIZED VIEW", ht);
    ht.put("SCHEMAS", "c.relkind = 'm'");
    ht.put("NOSCHEMAS", "c.relkind = 'm'");
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return getSchemas(null, null);
  }

  @Override
  public ResultSet getSchemas(@Nullable String catalog, @Nullable String schemaPattern)
      throws SQLException {
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

    List<Tuple> tuples = new ArrayList<>();
    while (rs.next()) {
      Tuple tuple = new Tuple(2);
      tuple.set(0, rs.getBytes("schema_name"));
      tuple.set(1, null);
      tuples.add(tuple);
    }
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(fields, tuples);
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    String sql = "SELECT datname AS TABLE_CAT FROM pg_catalog.pg_database"
        + " WHERE datallowconn = true"
        + " ORDER BY datname";
    return emptyResult(col("TABLE_CAT"));
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    Field[] f = new Field[1];
    List<Tuple> tuples = new ArrayList<>();

    f[0] = new Field("TABLE_TYPE", Oid.VARCHAR);
    Tuple tuple1 = new Tuple(1);
    tuple1.set(0, connection.encodeString("SYSTEM_TABLE"));
    tuples.add(tuple1);

    Tuple tuple2 = new Tuple(1);
    tuple2.set(0, connection.encodeString("TABLE"));
    tuples.add(tuple2);

    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, tuples);
  }

  @Override
  public ResultSet getColumns(@Nullable String catalog, @Nullable String schemaPattern,
      @Nullable String tableNamePattern,
      @Nullable String columnNamePattern) throws SQLException {

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
    String infoSchemaTableWhereClause = createInfoSchemaTableWhereClause(schemaName, schemaPattern, tableNamePattern,
            columnNamePattern).toString();
    String stmt = getColumnsStatement(schemaName, infoSchemaTableWhereClause);
    ResultSet rs = connection.createStatement().executeQuery(stmt);

    List<Tuple> tuples = new ArrayList<>();

    while (rs.next()) {
      Tuple tuple = new Tuple(fields.length);
      tuple.set(1, rs.getBytes(schemaName));
      tuple.set(2, rs.getBytes("table_name"));
      tuple.set(3, rs.getBytes("column_name"));
      String sqlType = rs.getString("data_type");
      tuple.set(4, connection.encodeString(Integer.toString(sqlTypeOfCrateType(sqlType))));
      tuple.set(5, sqlType.getBytes());
      tuple.set(6, null);
      tuple.set(7, null);
      tuple.set(10, connection.encodeString(Integer.toString(columnNullable)));
      tuple.set(11, null);
      tuple.set(13, null);
      tuple.set(14, null);
      tuple.set(16, rs.getBytes("ordinal_position"));
      tuple.set(18, null);
      tuple.set(19, null);
      tuple.set(20, null);
      tuple.set(21, null);
      tuple.set(22, connection.encodeString("NO"));

      if (getCrateVersion().before("2.0.0")) {
        tuple.set(0, null);
        tuple.set(8, null);
        tuple.set(9, connection.encodeString("10"));
        tuple.set(12, null);
        tuple.set(15, null);
        tuple.set(17, connection.encodeString("YES"));
        tuple.set(23, connection.encodeString("NO"));
      } else {
        tuple.set(0, rs.getBytes("table_catalog"));
        tuple.set(8, rs.getBytes("numeric_precision"));
        tuple.set(9, rs.getBytes("numeric_precision_radix"));
        tuple.set(12, rs.getBytes("column_default"));
        tuple.set(15, rs.getBytes("character_octet_length"));
        tuple.set(17, connection.encodeString(rs.getBoolean("is_nullable") ? "YES" : "NO"));
        if (getCrateVersion().before("4.0.0")) {
          tuple.set(23, connection.encodeString(rs.getBoolean("is_generated") ? "YES" : "NO"));
        } else {
          tuple.set(23, rs.getBytes("is_generated"));
        }
      }
      tuples.add(tuple);
    }
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(fields, tuples);
  }

  private String getColumnsStatement(String schemaName, String infoSchemaTableWhereClause) throws SQLException {
    String select;
    if (getCrateVersion().before("2.0.0")) {
      select = "SELECT " + schemaName + ", table_name, column_name, data_type, ordinal_position";
    } else {
      select = "SELECT " + schemaName + ", table_name, column_name, data_type, ordinal_position, table_catalog,"
              + " numeric_precision, numeric_precision_radix, column_default, character_octet_length, is_nullable,"
              + "is_generated";
    }
    return select
            + " FROM information_schema.columns "
            + infoSchemaTableWhereClause
            + " AND column_name NOT LIKE '%[%]' AND column_name NOT LIKE '%.%'"
            + " ORDER BY " + schemaName + ", table_name, ordinal_position";
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

  @Override
  public ResultSet getColumnPrivileges(@Nullable String catalog, @Nullable String schema,
      String table, @Nullable String columnNamePattern) throws SQLException {

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

  @Override
  public ResultSet getTablePrivileges(@Nullable String catalog, @Nullable String schemaPattern,
      @Nullable String tableNamePattern) throws SQLException {

    return emptyResult(
            col("TABLE_CAT"),
            col("TABLE_SCHEM"),
            col("TABLE_NAME"),
            col("GRANTOR"),
            col("GRANTEE"),
            col("PRIVILEGE"),
            col("IS_GRANTABLE"));
  }

  /**
   * Parse an String of ACLs into a List of ACLs.
   */
  private static List<String> parseACLArray(String aclString) {
    List<String> acls = new ArrayList<>();
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
  private static void addACLPrivileges(String acl,
      Map<String, Map<String, List<@Nullable String[]>>> privileges) {
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
          case 'p':
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

        Map<String, List<@Nullable String[]>> usersWithPermission = privileges.get(sqlpriv);
        //noinspection Java8MapApi
        if (usersWithPermission == null) {
          usersWithPermission = new HashMap<>();
          privileges.put(sqlpriv, usersWithPermission);
        }

        List<@Nullable String[]> permissionByGrantor = usersWithPermission.get(user);
        //noinspection Java8MapApi
        if (permissionByGrantor == null) {
          permissionByGrantor = new ArrayList<>();
          usersWithPermission.put(user, permissionByGrantor);
        }

        @Nullable String[] grant = {grantor, grantable};
        permissionByGrantor.add(grant);
      }
    }
  }

  /**
   * Take the a String representing an array of ACLs and return a Map mapping the SQL permission
   * name to a List of usernames who have that permission.
   * For instance: {@code SELECT -> user1 -> list of [grantor, grantable]}
   *
   * @param aclArray ACL array
   * @param owner owner
   * @return a Map mapping the SQL permission name
   */
  public Map<String, Map<String, List<@Nullable String[]>>> parseACL(@Nullable String aclArray,
      String owner) {
    if (aclArray == null) {
      // arwdxt -- 8.2 Removed the separate RULE permission
      // arwdDxt -- 8.4 Added a separate TRUNCATE permission
      String perms = connection.haveMinimumServerVersion(ServerVersion.v8_4) ? "arwdDxt" : "arwdxt";

      aclArray = "{" + owner + "=" + perms + "/" + owner + "}";
    }

    List<String> acls = parseACLArray(aclArray);
    Map<String, Map<String, List<@Nullable String[]>>> privileges =
        new HashMap<>();
    for (String acl : acls) {
      addACLPrivileges(acl, privileges);
    }
    return privileges;
  }

  @Override
  public ResultSet getBestRowIdentifier(
      @Nullable String catalog, @Nullable String schema, String table,
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

  @Override
  public ResultSet getVersionColumns(
      @Nullable String catalog, @Nullable String schema, String table)
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

  @Override
  public ResultSet getPrimaryKeys(@Nullable String catalog, @Nullable String schema, String table)
      throws SQLException {
    CrateVersion version = getCrateVersion();
    if (version.before("2.3.0")) {
      Field[] fields = new Field[6];
      fields[0] = new Field("TABLE_CAT", Oid.VARCHAR);
      fields[1] = new Field("TABLE_SCHEM", Oid.VARCHAR);
      fields[2] = new Field("TABLE_NAME", Oid.VARCHAR);
      fields[3] = new Field("COLUMN_NAME", Oid.VARCHAR);
      fields[4] = new Field("KEY_SEQ", Oid.VARCHAR);
      fields[5] = new Field("PK_NAME", Oid.VARCHAR);

      String schemaName = getCrateSchemaName();
      StringBuilder sql = new StringBuilder("SELECT NULL AS TABLE_CAT, "
                                              + "  " + schemaName + " AS TABLE_SCHEM, "
                                              + "  table_name as TABLE_NAME, "
                                              + "  constraint_name AS COLUMN_NAMES, "
                                              + "  0 AS KEY_SEQ, "
                                              + "  NULL AS PK_NAME "
                                              + "FROM information_schema.table_constraints "
                                              + "WHERE '_id' != ANY(constraint_name) "
                                              + "  AND table_name = '" + connection.escapeString(table) + "' ");
      if (schema != null) {
        sql.append("  AND " + schemaName + "= '" + connection.escapeString(schema) + "' ");
      }
      sql.append("ORDER BY TABLE_SCHEM, TABLE_NAME");
      ResultSet rs = connection.createStatement().executeQuery(sql.toString());

      List<Tuple> tuples = new ArrayList<>();
      while (rs.next()) {
        byte[] tableCat = rs.getBytes(1);
        byte[] tableSchem = rs.getBytes(2);
        byte[] tableName = rs.getBytes(3);
        byte[] pkName = rs.getBytes(6);
        String[] pkColumsn = (String[]) rs.getArray(4).getArray();
        for (int i = 0; i < pkColumsn.length; i++) {
          Tuple tuple = new Tuple(fields.length);
          tuple.set(0, tableCat);
          tuple.set(1, tableSchem);
          tuple.set(2, tableName);
          tuple.set(3, connection.encodeString(pkColumsn[i]));
          tuple.set(4, connection.encodeString(Integer.toString(i)));
          tuple.set(5, pkName);
          tuples.add(tuple);
        }
      }
      return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(fields, tuples);
    } else {
      // Before 3.0.0 kcu.table_schema used to return 'public' (see https://github.com/crate/crate/pull/7028/)
      // and we used table_catalog field to get a schema.
      // With https://github.com/crate/crate/pull/12652 table_catalog returns 'crate' but we can safely use table_schema for versions > 3.0.0.
      String schemaField = "table_catalog"; // Before 3.0.0
      if (version.after("3.0.0")) {
        schemaField = "table_schema";
      }

      // Before 5.1.0 catalog used to return schema and we returned NULL, from 5.1.0 can use actual catalog ('crate').
      String catalogField;
      if (version.before("5.1.0")) {
        catalogField = "NULL";
      } else {
        catalogField = "kcu.table_catalog";
      }
      StringBuilder sql = new StringBuilder("SELECT " + catalogField + " AS \"TABLE_CAT\",\n"
                  + "  kcu." + schemaField + " AS \"TABLE_SCHEM\",\n"
                  + "  kcu.table_name AS \"TABLE_NAME\",\n"
                  + "  kcu.column_name AS \"COLUMN_NAME\",\n"
                  + "  kcu.ordinal_position AS \"KEY_SEQ\",\n"
                  + "  kcu.constraint_name AS \"PK_NAME\"\n"
                  + " FROM information_schema.key_column_usage kcu\n"
                  + " WHERE kcu.table_name = '" + connection.escapeString(table) + "'\n");
      if (schema != null) {
        sql.append("  AND kcu." + schemaField + " = '" + connection.escapeString(schema) + "'\n");
      }
      sql.append("ORDER BY \"TABLE_SCHEM\", \"TABLE_NAME\", \"KEY_SEQ\";");
      return createMetaDataStatement().executeQuery(sql.toString());
    }
  }

  // TODO: can it be removed?
  /*
  This is for internal use only to see if a resultset is updateable.
  Unique keys can also be used so we add them to the query.
   */
  protected ResultSet getPrimaryUniqueKeys(@Nullable String catalog, @Nullable String schema, String table)
      throws SQLException {
    String sql;
    sql = "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, "
        + "  ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME, "
        + "  (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS PK_NAME, "
        + "  information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM, "
        + "  a.attnotnull AS IS_NOT_NULL "
        + "FROM pg_catalog.pg_class ct "
        + "  JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid) "
        + "  JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) "
        + "  JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid) "
        + "  JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) "
        // primary as well as unique keys can be used to uniquely identify a row to update
        + "WHERE (i.indisprimary OR ( "
        + "    i.indisunique "
        + "    AND i.indisvalid "
        // partial indexes are not allowed - indpred will not be null if this is a partial index
        + "    AND i.indpred IS NULL "
        // indexes with expressions are not allowed
        + "    AND i.indexprs IS NULL "
        + "  )) ";

    if (schema != null && !schema.isEmpty()) {
      sql += " AND n.nspname = " + escapeQuotes(schema);
    }

    if (table != null && !table.isEmpty()) {
      sql += " AND ct.relname = " + escapeQuotes(table);
    }

    sql = "SELECT "
        + "       result.TABLE_CAT, "
        + "       result.TABLE_SCHEM, "
        + "       result.TABLE_NAME, "
        + "       result.COLUMN_NAME, "
        + "       result.KEY_SEQ, "
        + "       result.PK_NAME, "
        + "       result.IS_NOT_NULL "
        + "FROM "
        + "     (" + sql + " ) result"
        + " where "
        + " result.A_ATTNUM = (result.KEYS).x ";
    sql += " ORDER BY result.table_name, result.pk_name, result.key_seq";

    return createMetaDataStatement().executeQuery(sql);
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
  protected ResultSet getImportedExportedKeys(
      @Nullable String primaryCatalog, @Nullable String primarySchema, @Nullable String primaryTable,
      @Nullable String foreignCatalog, @Nullable String foreignSchema, @Nullable String foreignTable)
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

  @Override
  public ResultSet getImportedKeys(@Nullable String catalog, @Nullable String schema, String table)
      throws SQLException {
    return getImportedExportedKeys(null, null, null, catalog, schema, table);
  }

  @Override
  public ResultSet getExportedKeys(@Nullable String catalog, @Nullable String schema, String table)
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

  @Override
  public ResultSet getCrossReference(
      @Nullable String primaryCatalog, @Nullable String primarySchema, String primaryTable,
      @Nullable String foreignCatalog, @Nullable String foreignSchema, String foreignTable)
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

  @Override
  public ResultSet getTypeInfo() throws SQLException {

    Field[] f = new Field[18];
    List<Tuple> v = new ArrayList<>();

    byte[] bNullable = connection.encodeString(Integer.toString(typeNullable));
    byte[] bSearchable = connection.encodeString(Integer.toString(typeSearchable));
    byte[] bPredBasic = connection.encodeString(Integer.toString(typePredBasic));
    byte[] bPredNone = connection.encodeString(Integer.toString(typePredNone));
    byte[] bTrue = connection.encodeString("t");
    byte[] bFalse = connection.encodeString("f");
    byte[] bZero = connection.encodeString("0");
    byte[] b10 = connection.encodeString("10");

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

    Tuple row1 = new Tuple(f.length);
    row1.set(0, connection.encodeString("byte"));
    row1.set(1, connection.encodeString(Integer.toString(Types.TINYINT)));
    row1.set(2, connection.encodeString("3"));
    row1.set(3, null);
    row1.set(4, null);
    row1.set(5, null);
    row1.set(6, bNullable);
    row1.set(7, bFalse);
    row1.set(8, bPredBasic);
    row1.set(9, bTrue);
    row1.set(10, bFalse);
    row1.set(11, bFalse);
    row1.set(12, row1.get(0));
    row1.set(13, bZero);
    row1.set(14, bZero);
    row1.set(15, null);
    row1.set(16, null);
    row1.set(17, b10);
    v.add(row1);

    Tuple row2 = new Tuple(f.length);
    row2.set(0, connection.encodeString("long"));
    row2.set(1, connection.encodeString(Integer.toString(Types.BIGINT)));
    row2.set(2, connection.encodeString("19"));
    row2.set(3, null);
    row2.set(4, null);
    row2.set(5, null);
    row2.set(6, bNullable);
    row2.set(7, bFalse);
    row2.set(8, bPredBasic);
    row2.set(9, bTrue);
    row2.set(10, bFalse);
    row2.set(11, bFalse);
    row2.set(12, row2.get(0));
    row2.set(13, bZero);
    row2.set(14, bZero);
    row2.set(15, null);
    row2.set(16, null);
    row2.set(17, b10);
    v.add(row2);

    Tuple row3 = new Tuple(f.length);
    row3.set(0, connection.encodeString("integer"));
    row3.set(1, connection.encodeString(Integer.toString(Types.INTEGER)));
    row3.set(2, b10);
    row3.set(3, null);
    row3.set(4, null);
    row3.set(5, null);
    row3.set(6, bNullable);
    row3.set(7, bFalse);
    row3.set(8, bPredBasic);
    row3.set(9, bTrue);
    row3.set(10, bFalse);
    row3.set(11, bFalse);
    row3.set(12, row3.get(0));
    row3.set(13, bZero);
    row3.set(14, bZero);
    row3.set(15, null);
    row3.set(16, null);
    row3.set(17, b10);
    v.add(row3);

    Tuple row4 = new Tuple(f.length);
    row4.set(0, connection.encodeString("short"));
    row4.set(1, connection.encodeString(Integer.toString(Types.SMALLINT)));
    row4.set(2, connection.encodeString("5"));
    row4.set(3, null);
    row4.set(4, null);
    row4.set(5, null);
    row4.set(6, bNullable);
    row4.set(7, bFalse);
    row4.set(8, bPredBasic);
    row4.set(9, bTrue);
    row4.set(10, bFalse);
    row4.set(11, bFalse);
    row4.set(12, row4.get(0));
    row4.set(13, bZero);
    row4.set(14, bZero);
    row4.set(15, null);
    row4.set(16, null);
    row4.set(17, b10);
    v.add(row4);

    Tuple row5 = new Tuple(f.length);
    row5.set(0, connection.encodeString("float"));
    row5.set(1, connection.encodeString(Integer.toString(Types.REAL)));
    row5.set(2, connection.encodeString("7"));
    row5.set(3, null);
    row5.set(4, null);
    row5.set(5, null);
    row5.set(6, bNullable);
    row5.set(7, bFalse);
    row5.set(8, bPredBasic);
    row5.set(9, bTrue);
    row5.set(10, bFalse);
    row5.set(11, bFalse);
    row5.set(12, row5.get(0));
    row5.set(13, bZero);
    row5.set(14, connection.encodeString("6"));
    row5.set(15, null);
    row5.set(16, null);
    row5.set(17, b10);
    v.add(row5);

    Tuple row6 = new Tuple(f.length);
    row6.set(0, connection.encodeString("double"));
    row6.set(1, connection.encodeString(Integer.toString(Types.DOUBLE)));
    row6.set(2, connection.encodeString("15"));
    row6.set(3, null);
    row6.set(4, null);
    row6.set(5, null);
    row6.set(6, bNullable);
    row6.set(7, bFalse);
    row6.set(8, bPredBasic);
    row6.set(9, bTrue);
    row6.set(10, bFalse);
    row6.set(11, bFalse);
    row6.set(12, row6.get(0));
    row6.set(13, bZero);
    row6.set(14, connection.encodeString("14"));
    row6.set(15, null);
    row6.set(16, null);
    row6.set(17, b10);
    v.add(row6);

    Tuple row7 = new Tuple(f.length);
    row7.set(0, connection.encodeString("string"));
    row7.set(1, connection.encodeString(Integer.toString(Types.VARCHAR)));
    row7.set(2, null);
    row7.set(3, null);
    row7.set(4, null);
    row7.set(5, null);
    row7.set(6, bNullable);
    row7.set(7, bTrue);
    row7.set(8, bSearchable);
    row7.set(9, bTrue);
    row7.set(10, bFalse);
    row7.set(11, bFalse);
    row7.set(12, row7.get(0));
    row7.set(13, bZero);
    row7.set(14, bZero);
    row7.set(15, null);
    row7.set(16, null);
    row7.set(17, b10);
    v.add(row7);

    Tuple row8 = new Tuple(f.length);
    row8.set(0, connection.encodeString("ip"));
    row8.set(1, connection.encodeString(Integer.toString(Types.VARCHAR)));
    row8.set(2, connection.encodeString("15"));
    row8.set(3, null);
    row8.set(4, null);
    row8.set(5, null);
    row8.set(6, bNullable);
    row8.set(7, bFalse);
    row8.set(8, bSearchable);
    row8.set(9, bTrue);
    row8.set(10, bFalse);
    row8.set(11, bFalse);
    row8.set(12, row8.get(0));
    row8.set(13, bZero);
    row8.set(14, bZero);
    row8.set(15, null);
    row8.set(16, null);
    row8.set(17, b10);
    v.add(row8);

    Tuple row9 = new Tuple(f.length);
    row9.set(0, connection.encodeString("boolean"));
    row9.set(1, connection.encodeString(Integer.toString(Types.BOOLEAN)));
    row9.set(2, null);
    row9.set(3, null);
    row9.set(4, null);
    row9.set(5, null);
    row9.set(6, bNullable);
    row9.set(7, bFalse);
    row9.set(8, bPredBasic);
    row9.set(9, bTrue);
    row9.set(10, bFalse);
    row9.set(11, bFalse);
    row9.set(12, row9.get(0));
    row9.set(13, bZero);
    row9.set(14, bZero);
    row9.set(15, null);
    row9.set(16, null);
    row9.set(17, b10);
    v.add(row9);

    Tuple row10 = new Tuple(f.length);
    row10.set(0, connection.encodeString("timestamp"));
    row10.set(1, connection.encodeString(Integer.toString(Types.TIMESTAMP)));
    row10.set(2, null);
    row10.set(3, null);
    row10.set(4, null);
    row10.set(5, null);
    row10.set(6, bNullable);
    row10.set(7, bTrue);
    row10.set(8, bPredBasic);
    row10.set(9, bTrue);
    row10.set(10, bFalse);
    row10.set(11, bFalse);
    row10.set(12, row10.get(0));
    row10.set(13, bZero);
    row10.set(14, bZero);
    row10.set(15, null);
    row10.set(16, null);
    row10.set(17, b10);
    v.add(row10);

    Tuple row11 = new Tuple(f.length);
    row11.set(0, connection.encodeString("object"));
    row11.set(1, connection.encodeString(Integer.toString(Types.STRUCT)));
    row11.set(2, null);
    row11.set(3, null);
    row11.set(4, null);
    row11.set(5, null);
    row11.set(6, bNullable);
    row11.set(7, bFalse);
    row11.set(8, bPredNone);
    row11.set(9, bTrue);
    row11.set(10, bFalse);
    row11.set(11, bFalse);
    row11.set(12, row11.get(0));
    row11.set(13, bZero);
    row11.set(14, bZero);
    row11.set(15, null);
    row11.set(16, null);
    row11.set(17, b10);
    v.add(row11);
    String[] arrayTypes = new String[]{"string_array", "ip_array", "long_array",
      "integer_array", "short_array", "boolean_array", "byte_array",
      "float_array", "double_array", "object_array"};
    for (int i = 11; i < 11 + arrayTypes.length; i++) {
      Tuple row = new Tuple(f.length);
      row.set(0, connection.encodeString(arrayTypes[i - 11]));
      row.set(1, connection.encodeString(Integer.toString(Types.ARRAY)));
      row.set(2, null);
      row.set(3, null);
      row.set(4, null);
      row.set(5, null);
      row.set(6, bNullable);
      row.set(7, bFalse);
      row.set(8, bPredNone);
      row.set(9, bTrue);
      row.set(10, bFalse);
      row.set(11, bFalse);
      row.set(12, row.get(0));
      row.set(13, bZero);
      row.set(14, bZero);
      row.set(15, null);
      row.set(16, null);
      row.set(17, b10);
      v.add(row);
    }

    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, v);
  }

  @Override
  public ResultSet getIndexInfo(
      @Nullable String catalog, @Nullable String schema, String tableName,
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

  // ** JDBC 2 Extensions **

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    // The only type we don't support
    return type != ResultSet.TYPE_SCROLL_SENSITIVE;
  }

  @Override
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
  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    return true;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    return true;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    // indicates that
    return true;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int i) throws SQLException {
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean deletesAreDetected(int i) throws SQLException {
    return false;
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    return true;
  }

  @Override
  public ResultSet getUDTs(@Nullable String catalog, @Nullable String schemaPattern,
      @Nullable String typeNamePattern, int @Nullable [] types) throws SQLException {
    String sql = "select "
        + "null as type_cat, n.nspname as type_schem, t.typname as type_name,  null as class_name, "
        + "CASE WHEN t.typtype='c' then " + Types.STRUCT + " else "
        + Types.DISTINCT
        + " end as data_type, pg_catalog.obj_description(t.oid, 'pg_type')  "
        + "as remarks, CASE WHEN t.typtype = 'd' then  (select CASE";
    TypeInfo typeInfo = connection.getTypeInfo();

    StringBuilder sqlwhen = new StringBuilder();
    for (Iterator<Integer> i = typeInfo.getPGTypeOidsWithSQLTypes(); i.hasNext(); ) {
      Integer typOid = i.next();
      // NB: Java Integers are signed 32-bit integers, but oids are unsigned 32-bit integers.
      // We must therefore map it to a positive long value before writing it into the query,
      // or we'll be unable to correctly handle ~ half of the oid space.
      long longTypOid = typeInfo.intOidToLong(typOid);
      int sqlType = typeInfo.getSQLType(typOid);

      sqlwhen.append(" when base_type.oid = ").append(longTypOid).append(" then ").append(sqlType);
    }
    sql += sqlwhen.toString();

    sql += " else " + Types.OTHER + " end from pg_type base_type where base_type.oid=t.typbasetype) "
        + "else null end as base_type "
        + "from pg_catalog.pg_type t, pg_catalog.pg_namespace n where t.typnamespace = n.oid and n.nspname != 'pg_catalog' and n.nspname != 'pg_toast'";

    StringBuilder toAdd = new StringBuilder();
    if (types != null) {
      toAdd.append(" and (false ");
      for (int type : types) {
        if (type == Types.STRUCT) {
          toAdd.append(" or t.typtype = 'c'");
        } else if (type == Types.DISTINCT) {
          toAdd.append(" or t.typtype = 'd'");
        }
      }
      toAdd.append(" ) ");
    } else {
      toAdd.append(" and t.typtype IN ('c','d') ");
    }
    // spec says that if typeNamePattern is a fully qualified name
    // then the schema and catalog are ignored

    if (typeNamePattern != null) {
      // search for qualifier
      int firstQualifier = typeNamePattern.indexOf('.');
      int secondQualifier = typeNamePattern.lastIndexOf('.');

      if (firstQualifier != -1) {
        // if one of them is -1 they both will be
        if (firstQualifier != secondQualifier) {
          // we have a catalog.schema.typename, ignore catalog
          schemaPattern = typeNamePattern.substring(firstQualifier + 1, secondQualifier);
        } else {
          // we just have a schema.typename
          schemaPattern = typeNamePattern.substring(0, firstQualifier);
        }
        // strip out just the typeName
        typeNamePattern = typeNamePattern.substring(secondQualifier + 1);
      }
      toAdd.append(" and t.typname like ").append(escapeQuotes(typeNamePattern));
    }

    // schemaPattern may have been modified above
    if (schemaPattern != null) {
      toAdd.append(" and n.nspname like ").append(escapeQuotes(schemaPattern));
    }
    sql += toAdd.toString();

    if (connection.getHideUnprivilegedObjects()
        && connection.haveMinimumServerVersion(ServerVersion.v9_2)) {
      sql += " AND has_type_privilege(t.oid, 'USAGE')";
    }

    sql += " order by data_type, type_schem, type_name";
    return createMetaDataStatement().executeQuery(sql);
  }

  @Override
  public Connection getConnection() throws SQLException {
    return connection;
  }

  protected Statement createMetaDataStatement() throws SQLException {
    return connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public long getMaxLogicalLobSize() throws SQLException {
    return 0;
  }

  @Override
  public boolean supportsRefCursors() throws SQLException {
    return true;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getRowIdLifetime()");
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return true;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    return emptyResult(
            col("NAME"),
            col("MAX_LEN", Oid.INT4),
            col("DEFAULT_VALUE"),
            col("DESCRIPTION"));
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  @Override
  public ResultSet getFunctions(@Nullable String catalog, @Nullable String schemaPattern,
      @Nullable String functionNamePattern)
      throws SQLException {
    return emptyResult(
            col("FUNCTION_CAT"),
            col("FUNCTION_SCHEM"),
            col("FUNCTION_NAME"),
            col("REMARKS"),
            col("FUNCTION_TYPE", Oid.INT2),
            col("SPECIFIC_NAME"));
  }

  @Override
  public ResultSet getFunctionColumns(@Nullable String catalog, @Nullable String schemaPattern,
      @Nullable String functionNamePattern, @Nullable String columnNamePattern)
      throws SQLException {
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

  @Override
  public ResultSet getPseudoColumns(@Nullable String catalog, @Nullable String schemaPattern,
      @Nullable String tableNamePattern, @Nullable String columnNamePattern)
      throws SQLException {
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

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    // We don't support returning generated keys by column index,
    // but that should be a rarer case than the ones we do support.
    //
    return true;
  }

  @Override
  public ResultSet getSuperTypes(@Nullable String catalog, @Nullable String schemaPattern,
      @Nullable String typeNamePattern)
      throws SQLException {
    return emptyResult(
            col("TYPE_CAT"),
            col("TYPE_SCHEM"),
            col("TYPE_NAME"),
            col("SUPERTYPE_CAT"),
            col("SUPERTYPE_SCHEM"),
            col("SUPERTYPE_NAME"));
  }

  @Override
  public ResultSet getSuperTables(@Nullable String catalog, @Nullable String schemaPattern,
      @Nullable String tableNamePattern)
      throws SQLException {
    return emptyResult(
            col("TABLE_CAT"),
            col("TABLE_SCHEM"),
            col("TABLE_NAME"),
            col("SUPERTABLE_NAME"));
  }

  @Override
  public ResultSet getAttributes(@Nullable String catalog, @Nullable String schemaPattern,
      @Nullable String typeNamePattern, @Nullable String attributeNamePattern) throws SQLException {
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

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return true;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    return connection.getServerMajorVersion();
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return connection.getServerMinorVersion();
  }

  @Override
  public int getJDBCMajorVersion() {
    return DriverInfo.JDBC_MAJOR_VERSION;
  }

  @Override
  public int getJDBCMinorVersion() {
    return DriverInfo.JDBC_MINOR_VERSION;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    return sqlStateSQL;
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    /*
     * Currently LOB's aren't updateable at all, so it doesn't matter what we return. We don't throw
     * the notImplemented Exception because the 1.5 JDK's CachedRowSet calls this method regardless
     * of whether large objects are used.
     */
    return true;
  }

  @Override
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

  private ResultSet emptyResult(Field... fields) throws SQLException {
    List<Tuple> tuples = Collections.emptyList();
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(fields, tuples);
  }
}
