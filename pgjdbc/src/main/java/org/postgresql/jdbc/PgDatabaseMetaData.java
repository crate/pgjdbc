/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.jdbc;

import static org.postgresql.util.internal.Nullness.castNonNull;

import org.postgresql.Driver;
import org.postgresql.core.BaseStatement;
import org.postgresql.core.Field;
import org.postgresql.core.Oid;
import org.postgresql.core.ServerVersion;
import org.postgresql.core.Tuple;
import org.postgresql.core.TypeInfo;
import org.postgresql.util.ByteConverter;
import org.postgresql.util.DriverInfo;
import org.postgresql.util.GT;
import org.postgresql.util.JdbcBlackHole;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;

public class PgDatabaseMetaData implements DatabaseMetaData {

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
   * Retrieves the name of this database product. We hope that it is PostgreSQL, so we return that
   * explicitly.
   *
   * @return "PostgreSQL"
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
    String keywords = this.keywords;
    if (keywords == null) {
      if (connection.haveMinimumServerVersion(ServerVersion.v9_0)) {
        // Exclude SQL:2003 keywords (https://github.com/ronsavage/SQL/blob/master/sql-2003-2.bnf)
        // from the returned list, ugly but required by jdbc spec.
        String sql = "select string_agg(word, ',') from pg_catalog.pg_get_keywords() "
            + "where word <> ALL ('{a,abs,absolute,action,ada,add,admin,after,all,allocate,alter,"
            + "always,and,any,are,array,as,asc,asensitive,assertion,assignment,asymmetric,at,atomic,"
            + "attribute,attributes,authorization,avg,before,begin,bernoulli,between,bigint,binary,"
            + "blob,boolean,both,breadth,by,c,call,called,cardinality,cascade,cascaded,case,cast,"
            + "catalog,catalog_name,ceil,ceiling,chain,char,char_length,character,character_length,"
            + "character_set_catalog,character_set_name,character_set_schema,characteristics,"
            + "characters,check,checked,class_origin,clob,close,coalesce,cobol,code_units,collate,"
            + "collation,collation_catalog,collation_name,collation_schema,collect,column,"
            + "column_name,command_function,command_function_code,commit,committed,condition,"
            + "condition_number,connect,connection_name,constraint,constraint_catalog,constraint_name,"
            + "constraint_schema,constraints,constructors,contains,continue,convert,corr,"
            + "corresponding,count,covar_pop,covar_samp,create,cross,cube,cume_dist,current,"
            + "current_collation,current_date,current_default_transform_group,current_path,"
            + "current_role,current_time,current_timestamp,current_transform_group_for_type,current_user,"
            + "cursor,cursor_name,cycle,data,date,datetime_interval_code,datetime_interval_precision,"
            + "day,deallocate,dec,decimal,declare,default,defaults,deferrable,deferred,defined,definer,"
            + "degree,delete,dense_rank,depth,deref,derived,desc,describe,descriptor,deterministic,"
            + "diagnostics,disconnect,dispatch,distinct,domain,double,drop,dynamic,dynamic_function,"
            + "dynamic_function_code,each,element,else,end,end-exec,equals,escape,every,except,"
            + "exception,exclude,excluding,exec,execute,exists,exp,external,extract,false,fetch,filter,"
            + "final,first,float,floor,following,for,foreign,fortran,found,free,from,full,function,"
            + "fusion,g,general,get,global,go,goto,grant,granted,group,grouping,having,hierarchy,hold,"
            + "hour,identity,immediate,implementation,in,including,increment,indicator,initially,"
            + "inner,inout,input,insensitive,insert,instance,instantiable,int,integer,intersect,"
            + "intersection,interval,into,invoker,is,isolation,join,k,key,key_member,key_type,language,"
            + "large,last,lateral,leading,left,length,level,like,ln,local,localtime,localtimestamp,"
            + "locator,lower,m,map,match,matched,max,maxvalue,member,merge,message_length,"
            + "message_octet_length,message_text,method,min,minute,minvalue,mod,modifies,module,month,"
            + "more,multiset,mumps,name,names,national,natural,nchar,nclob,nesting,new,next,no,none,"
            + "normalize,normalized,not,\"null\",nullable,nullif,nulls,number,numeric,object,"
            + "octet_length,octets,of,old,on,only,open,option,options,or,order,ordering,ordinality,"
            + "others,out,outer,output,over,overlaps,overlay,overriding,pad,parameter,parameter_mode,"
            + "parameter_name,parameter_ordinal_position,parameter_specific_catalog,"
            + "parameter_specific_name,parameter_specific_schema,partial,partition,pascal,path,"
            + "percent_rank,percentile_cont,percentile_disc,placing,pli,position,power,preceding,"
            + "precision,prepare,preserve,primary,prior,privileges,procedure,public,range,rank,read,"
            + "reads,real,recursive,ref,references,referencing,regr_avgx,regr_avgy,regr_count,"
            + "regr_intercept,regr_r2,regr_slope,regr_sxx,regr_sxy,regr_syy,relative,release,"
            + "repeatable,restart,result,return,returned_cardinality,returned_length,"
            + "returned_octet_length,returned_sqlstate,returns,revoke,right,role,rollback,rollup,"
            + "routine,routine_catalog,routine_name,routine_schema,row,row_count,row_number,rows,"
            + "savepoint,scale,schema,schema_name,scope_catalog,scope_name,scope_schema,scroll,"
            + "search,second,section,security,select,self,sensitive,sequence,serializable,server_name,"
            + "session,session_user,set,sets,similar,simple,size,smallint,some,source,space,specific,"
            + "specific_name,specifictype,sql,sqlexception,sqlstate,sqlwarning,sqrt,start,state,"
            + "statement,static,stddev_pop,stddev_samp,structure,style,subclass_origin,submultiset,"
            + "substring,sum,symmetric,system,system_user,table,table_name,tablesample,temporary,then,"
            + "ties,time,timestamp,timezone_hour,timezone_minute,to,top_level_count,trailing,"
            + "transaction,transaction_active,transactions_committed,transactions_rolled_back,"
            + "transform,transforms,translate,translation,treat,trigger,trigger_catalog,trigger_name,"
            + "trigger_schema,trim,true,type,uescape,unbounded,uncommitted,under,union,unique,unknown,"
            + "unnamed,unnest,update,upper,usage,user,user_defined_type_catalog,user_defined_type_code,"
            + "user_defined_type_name,user_defined_type_schema,using,value,values,var_pop,var_samp,"
            + "varchar,varying,view,when,whenever,where,width_bucket,window,with,within,without,work,"
            + "write,year,zone}'::text[])";

        Statement stmt = null;
        ResultSet rs = null;
        try {
          stmt = connection.createStatement();
          rs = stmt.executeQuery(sql);
          if (!rs.next()) {
            throw new PSQLException(GT.tr("Unable to find keywords in the system catalogs."),
                PSQLState.UNEXPECTED_ERROR);
          }
          keywords = rs.getString(1);
        } finally {
          JdbcBlackHole.close(rs);
          JdbcBlackHole.close(stmt);
        }
      } else {
        // Static list from PG8.2 src/backend/parser/keywords.c with SQL:2003 excluded.
        keywords = "abort,access,aggregate,also,analyse,analyze,backward,bit,cache,checkpoint,class,"
            + "cluster,comment,concurrently,connection,conversion,copy,csv,database,delimiter,"
            + "delimiters,disable,do,enable,encoding,encrypted,exclusive,explain,force,forward,freeze,"
            + "greatest,handler,header,if,ilike,immutable,implicit,index,indexes,inherit,inherits,"
            + "instead,isnull,least,limit,listen,load,location,lock,mode,move,nothing,notify,notnull,"
            + "nowait,off,offset,oids,operator,owned,owner,password,prepared,procedural,quote,reassign,"
            + "recheck,reindex,rename,replace,reset,restrict,returning,rule,setof,share,show,stable,"
            + "statistics,stdin,stdout,storage,strict,sysid,tablespace,temp,template,truncate,trusted,"
            + "unencrypted,unlisten,until,vacuum,valid,validator,verbose,volatile";
      }
      this.keywords = castNonNull(keywords);
    }
    return keywords;
  }

  @Override
  @SuppressWarnings("deprecation")
  public String getNumericFunctions() throws SQLException {
    return EscapedFunctions.ABS + ',' + EscapedFunctions.ACOS + ',' + EscapedFunctions.ASIN + ','
        + EscapedFunctions.ATAN + ',' + EscapedFunctions.ATAN2 + ',' + EscapedFunctions.CEILING
        + ',' + EscapedFunctions.COS + ',' + EscapedFunctions.COT + ',' + EscapedFunctions.DEGREES
        + ',' + EscapedFunctions.EXP + ',' + EscapedFunctions.FLOOR + ',' + EscapedFunctions.LOG
        + ',' + EscapedFunctions.LOG10 + ',' + EscapedFunctions.MOD + ',' + EscapedFunctions.PI
        + ',' + EscapedFunctions.POWER + ',' + EscapedFunctions.RADIANS + ','
        + EscapedFunctions.ROUND + ',' + EscapedFunctions.SIGN + ',' + EscapedFunctions.SIN + ','
        + EscapedFunctions.SQRT + ',' + EscapedFunctions.TAN + ',' + EscapedFunctions.TRUNCATE;

  }

  @Override
  @SuppressWarnings("deprecation")
  public String getStringFunctions() throws SQLException {
    String funcs = EscapedFunctions.ASCII + ',' + EscapedFunctions.CHAR + ','
        + EscapedFunctions.CONCAT + ',' + EscapedFunctions.LCASE + ',' + EscapedFunctions.LEFT + ','
        + EscapedFunctions.LENGTH + ',' + EscapedFunctions.LTRIM + ',' + EscapedFunctions.REPEAT
        + ',' + EscapedFunctions.RTRIM + ',' + EscapedFunctions.SPACE + ','
        + EscapedFunctions.SUBSTRING + ',' + EscapedFunctions.UCASE;

    // Currently these don't work correctly with parameterized
    // arguments, so leave them out. They reorder the arguments
    // when rewriting the query, but no translation layer is provided,
    // so a setObject(N, obj) will not go to the correct parameter.
    // ','+EscapedFunctions.INSERT+','+EscapedFunctions.LOCATE+
    // ','+EscapedFunctions.RIGHT+

    funcs += ',' + EscapedFunctions.REPLACE;

    return funcs;
  }

  @Override
  @SuppressWarnings("deprecation")
  public String getSystemFunctions() throws SQLException {
    return EscapedFunctions.DATABASE + ',' + EscapedFunctions.IFNULL + ',' + EscapedFunctions.USER;
  }

  @Override
  @SuppressWarnings("deprecation")
  public String getTimeDateFunctions() throws SQLException {
    String timeDateFuncs = EscapedFunctions.CURDATE + ',' + EscapedFunctions.CURTIME + ','
        + EscapedFunctions.DAYNAME + ',' + EscapedFunctions.DAYOFMONTH + ','
        + EscapedFunctions.DAYOFWEEK + ',' + EscapedFunctions.DAYOFYEAR + ','
        + EscapedFunctions.HOUR + ',' + EscapedFunctions.MINUTE + ',' + EscapedFunctions.MONTH + ','
        + EscapedFunctions.MONTHNAME + ',' + EscapedFunctions.NOW + ',' + EscapedFunctions.QUARTER
        + ',' + EscapedFunctions.SECOND + ',' + EscapedFunctions.WEEK + ',' + EscapedFunctions.YEAR;

    timeDateFuncs += ',' + EscapedFunctions.TIMESTAMPADD;

    // +','+EscapedFunctions.TIMESTAMPDIFF;

    return timeDateFuncs;
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

  /**
   * {@inheritDoc}
   *
   * @return true if connected to PostgreSQL 6.4+
   */
  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return true;
  }

  /**
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

  /**
   * Is the SQL Integrity Enhancement Facility supported? Our best guess is that this means support
   * for constraints
   *
   * @return true
   *
   * @throws SQLException if a database access error occurs
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
   *
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
   *
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
   *
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
    String sql =
        "SELECT setting FROM pg_catalog.pg_settings WHERE name='default_transaction_isolation'";

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      String level = null;
      if (rs.next()) {
        level = rs.getString(1);
      }
      if (level == null) {
        throw new PSQLException(
            GT.tr(
                "Unable to determine a value for DefaultTransactionIsolation due to missing "
                    + " entry in pg_catalog.pg_settings WHERE name='default_transaction_isolation'."),
            PSQLState.UNEXPECTED_ERROR);
      }
      // PostgreSQL returns the value in lower case, so using "toLowerCase" here would be
      // slightly more efficient.
      switch (level.toLowerCase(Locale.ROOT)) {
        case "read uncommitted":
          return Connection.TRANSACTION_READ_UNCOMMITTED;
        case "repeatable read":
          return Connection.TRANSACTION_REPEATABLE_READ;
        case "serializable":
          return Connection.TRANSACTION_SERIALIZABLE;
        case "read committed":
        default: // Best guess.
          return Connection.TRANSACTION_READ_COMMITTED;
      }
    }
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * <p>We only support TRANSACTION_SERIALIZABLE and TRANSACTION_READ_COMMITTED before 8.0; from 8.0
   * READ_UNCOMMITTED and REPEATABLE_READ are accepted aliases for READ_COMMITTED.</p>
   */
  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
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

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return true;
  }

  @Override
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

    String stmt = "select schema_name, table_name "
        + "from information_schema.tables "
        + createInfoSchemaTableWhereClause(schemaPattern, tableNamePattern, null)
        + " order by schema_name, table_name";
    ResultSet rs = connection.createStatement().executeQuery(stmt);

    List<Tuple> tuples = new ArrayList<>();
    while (rs.next()) {
      byte[] @Nullable [] tuple = new byte[fields.length][];
      String schema = rs.getString("schema_name");
      if ("sys".equals(schema) || "information_schema".equals(schema)) {
        tuple[3] = connection.encodeString("SYSTEM TABLE");
      } else {
        tuple[3] = connection.encodeString("TABLE");
      }
      tuple[0] = null;
      tuple[1] = rs.getBytes("schema_name");
      tuple[2] = rs.getBytes("table_name");
      tuple[4] = connection.encodeString("");
      tuple[5] = null;
      tuple[6] = null;
      tuple[7] = null;
      tuple[8] = connection.encodeString("_id");
      tuple[9] = connection.encodeString("SYSTEM");
      tuples.add(new Tuple(tuple));
    }
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(fields, tuples);
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
    boolean hasSchemata = getDatabaseMinorVersion() > 45;
    String table = hasSchemata ? "information_schema.schemata" : "information_schema.tables";
    StringBuilder stmt = new StringBuilder("select schema_name from " + table);
    if (schemaPattern != null) {
      stmt.append(" where schema_name like '")
          .append(connection.escapeString(schemaPattern))
          .append("'");
    }
    if (!hasSchemata) {
      stmt.append(" group by schema_name");
    }
    stmt.append(" order by schema_name");

    ResultSet rs = connection.createStatement().executeQuery(stmt.toString());
    Field[] fields = new Field[2];
    fields[0] = new Field("TABLE_SCHEM", Oid.VARCHAR);
    fields[1] = new Field("TABLE_CAT", Oid.VARCHAR);

    List<Tuple> tuples = new ArrayList<>();
    while (rs.next()) {
      tuples.add(new Tuple(new byte[] @Nullable [] {rs.getBytes("schema_name"), null}));
    }
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(fields, tuples);
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    return emptyResult(col("TABLE_CAT"));
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    String[] types = tableTypeClauses.keySet().toArray(new String[0]);
    Arrays.sort(types);

    Field[] f = new Field[1];
    List<Tuple> v = new ArrayList<>();
    f[0] = new Field("TABLE_TYPE", Oid.VARCHAR);
    for (String type : types) {
      byte[] @Nullable [] tuple = new byte[1][];
      tuple[0] = connection.encodeString(type);
      v.add(new Tuple(tuple));
    }

    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, v);
  }

  private StringBuilder createInfoSchemaTableWhereClause(@Nullable String schemaPattern,
      @Nullable String tableNamePattern, @Nullable String columnNamePattern) throws SQLException {
    StringBuilder where = new StringBuilder(" where ");
    if (schemaPattern == null) {
      where.append("schema_name like '%'");
    } else if (schemaPattern.equals("")) {
      where.append("schema_name is null");
    } else {
      where.append("schema_name like '")
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

  @Override
  public ResultSet getColumns(@Nullable String catalog, @Nullable String schemaPattern,
      @Nullable String tableNamePattern, @Nullable String columnNamePattern) throws SQLException {
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

    String stmt = "select schema_name, table_name, column_name, data_type, ordinal_position "
        + "from information_schema.columns "
        + createInfoSchemaTableWhereClause(schemaPattern, tableNamePattern, columnNamePattern)
        + " and column_name not like '%[%]' and column_name not like '%.%'"
        + " order by schema_name, table_name, ordinal_position";
    ResultSet rs = connection.createStatement().executeQuery(stmt);

    List<Tuple> tuples = new ArrayList<>();
    while (rs.next()) {
      byte[] @Nullable [] tuple = new byte[fields.length][];
      tuple[0] = null;
      tuple[1] = rs.getBytes("schema_name");
      tuple[2] = rs.getBytes("table_name");
      tuple[3] = rs.getBytes("column_name");
      tuple[4] = connection.encodeString(
          Integer.toString(sqlTypeOfCrateType(castNonNull(rs.getString("data_type")))));
      tuple[5] = rs.getBytes("data_type");
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
      tuples.add(new Tuple(tuple));
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
    Field[] fields = new Field[6];
    fields[0] = new Field("TABLE_CAT", Oid.VARCHAR);
    fields[1] = new Field("TABLE_SCHEM", Oid.VARCHAR);
    fields[2] = new Field("TABLE_NAME", Oid.VARCHAR);
    fields[3] = new Field("COLUMN_NAME", Oid.VARCHAR);
    fields[4] = new Field("KEY_SEQ", Oid.VARCHAR);
    fields[5] = new Field("PK_NAME", Oid.VARCHAR);

    StringBuilder sql = new StringBuilder("SELECT NULL AS TABLE_CAT, "
        + "schema_name AS TABLE_SCHEM, "
        + "table_name as TABLE_NAME, "
        + "constraint_name AS COLUMN_NAMES, "
        + "0 AS KEY_SEQ, "
        + "NULL AS PK_NAME "
        + "FROM information_schema.table_constraints "
        + "WHERE '_id' != ANY(constraint_name) ");
    if (schema != null) {
      sql.append("AND schema_name = '" + connection.escapeString(schema) + "' ");
    }
    sql.append("AND table_name = '" + connection.escapeString(table) + "' ")
        .append("ORDER BY TABLE_SCHEM, TABLE_NAME");
    ResultSet rs = connection.createStatement().executeQuery(sql.toString());

    List<Tuple> tuples = new ArrayList<>();
    while (rs.next()) {
      byte @Nullable [] tableCat = rs.getBytes(1);
      byte @Nullable [] tableSchem = rs.getBytes(2);
      byte @Nullable [] tableName = rs.getBytes(3);
      byte @Nullable [] pkName = rs.getBytes(6);
      String[] pkColumsn = (String[]) castNonNull(rs.getArray(4)).getArray();
      for (int i = 0; i < pkColumsn.length; i++) {
        byte[] @Nullable [] tuple = new byte[fields.length][];
        tuple[0] = tableCat;
        tuple[1] = tableSchem;
        tuple[2] = tableName;
        tuple[3] = connection.encodeString(pkColumsn[i]);
        tuple[4] = connection.encodeString(Integer.toString(i));
        tuple[5] = pkName;
        tuples.add(new Tuple(tuple));
      }
    }
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(fields, tuples);
  }

  /*
  This is for internal use only to see if a resultset is updateable.
  Unique keys can also be used so we add them to the query.
   */
  protected ResultSet getPrimaryUniqueKeys(@Nullable String catalog, @Nullable String schema, String table)
      throws SQLException {

    StringBuilder sql = new StringBuilder();
    sql.append(
        // language=sql
        "SELECT "
            + "       result.TABLE_CAT AS \"TABLE_CAT\", "
            + "       result.TABLE_SCHEM AS \"TABLE_SCHEM\", "
            + "       result.TABLE_NAME AS \"TABLE_NAME\", "
            + "       result.COLUMN_NAME AS \"COLUMN_NAME\", "
            + "       result.KEY_SEQ AS \"KEY_SEQ\", "
            + "       result.PK_NAME AS \"PK_NAME\", "
            + "       result.IS_NOT_NULL AS \"IS_NOT_NULL\""
            + "FROM "
            + "     (");
    sql.append(
        // language=sql
        "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, "
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
        + "  )) ");

    List<String> args = new ArrayList<>(2);
    if (schema != null && !schema.isEmpty()) {
      sql.append(" AND n.nspname = ?");
      args.add(schema);
    } else {
      // The table name carries no schema qualifier, so it resolves through the session's
      // search_path. pg_table_is_visible() applies the same visibility rules the server uses,
      // so only the relation the query actually references is matched. Without it, identically
      // named tables in other schemas are matched too, which can mis-classify a result set as
      // updatable when those tables share a constraint name but differ in their key columns.
      // pg_table_is_visible() has existed since PostgreSQL 7.3.
      sql.append(" AND pg_catalog.pg_table_is_visible(ct.oid)");
    }

    if (table != null && !table.isEmpty()) {
      sql.append(" AND ct.relname = ?");
      args.add(table);
    }

    sql.append(
        " ) result"
          + " where "
          + " result.A_ATTNUM = (result.KEYS).x ");
    sql.append(" ORDER BY result.table_name, result.pk_name, result.key_seq");

    return executeMetadataStatement(sql.toString(), args);
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

    Field[] f = new Field[14];
    List<Tuple> v = new ArrayList<>(); // The new ResultSet tuple stuff

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

    if (primaryCatalog != null && !primaryCatalog.equals(connection.getCatalog())
        || foreignCatalog != null && !foreignCatalog.equals(connection.getCatalog()))  {
      return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, v);
    }
    /*
     * The addition of the pg_constraint in 7.3 table should have really helped us out here, but it
     * comes up just a bit short. - The conkey, confkey columns aren't really useful without
     * contrib/array unless we want to issues separate queries. - Unique indexes that can support
     * foreign keys are not necessarily added to pg_constraint. Also multiple unique indexes
     * covering the same keys can be created which make it difficult to determine the PK_NAME field.
     */

    StringBuilder sql = new StringBuilder(
        // language=sql
        "SELECT current_database() AS \"PKTABLE_CAT\", pkn.nspname AS \"PKTABLE_SCHEM\", pkc.relname AS \"PKTABLE_NAME\", pka.attname AS \"PKCOLUMN_NAME\", "
            + "current_database() AS \"FKTABLE_CAT\", fkn.nspname AS \"FKTABLE_SCHEM\", fkc.relname AS \"FKTABLE_NAME\", fka.attname AS \"FKCOLUMN_NAME\", "
            + "pos.n AS \"KEY_SEQ\", "
            + "CASE con.confupdtype "
            + " WHEN 'c' THEN " + DatabaseMetaData.importedKeyCascade
            + " WHEN 'n' THEN " + DatabaseMetaData.importedKeySetNull
            + " WHEN 'd' THEN " + DatabaseMetaData.importedKeySetDefault
            + " WHEN 'r' THEN " + DatabaseMetaData.importedKeyRestrict
            + " WHEN 'p' THEN " + DatabaseMetaData.importedKeyRestrict
            + " WHEN 'a' THEN " + DatabaseMetaData.importedKeyNoAction
            + " ELSE NULL END AS \"UPDATE_RULE\", "
            + "CASE con.confdeltype "
            + " WHEN 'c' THEN " + DatabaseMetaData.importedKeyCascade
            + " WHEN 'n' THEN " + DatabaseMetaData.importedKeySetNull
            + " WHEN 'd' THEN " + DatabaseMetaData.importedKeySetDefault
            + " WHEN 'r' THEN " + DatabaseMetaData.importedKeyRestrict
            + " WHEN 'p' THEN " + DatabaseMetaData.importedKeyRestrict
            + " WHEN 'a' THEN " + DatabaseMetaData.importedKeyNoAction
            + " ELSE NULL END AS \"DELETE_RULE\", "
            + "con.conname AS \"FK_NAME\", pkic.relname AS \"PK_NAME\", "
            + "CASE "
            + " WHEN con.condeferrable AND con.condeferred THEN "
            + DatabaseMetaData.importedKeyInitiallyDeferred
            + " WHEN con.condeferrable THEN " + DatabaseMetaData.importedKeyInitiallyImmediate
            + " ELSE " + DatabaseMetaData.importedKeyNotDeferrable
            + " END AS \"DEFERRABILITY\" "
            + " FROM "
            + " pg_catalog.pg_namespace pkn, pg_catalog.pg_class pkc, pg_catalog.pg_attribute pka, "
            + " pg_catalog.pg_namespace fkn, pg_catalog.pg_class fkc, pg_catalog.pg_attribute fka, "
            + " pg_catalog.pg_constraint con, "
            + " pg_catalog.generate_series(1, " + getMaxIndexKeys() + ") pos(n), "
            + " pg_catalog.pg_class pkic");
    // Starting in Postgres 9.0, pg_constraint was augmented with the conindid column, which
    // contains the oid of the index supporting the constraint. This makes it unnecessary to do a
    // further join on pg_depend.
    if (!connection.haveMinimumServerVersion(ServerVersion.v9_0)) {
      sql.append(", pg_catalog.pg_depend dep ");
    }
    sql.append(
        // language=sql
        " WHERE pkn.oid = pkc.relnamespace AND pkc.oid = pka.attrelid AND pka.attnum = con.confkey[pos.n] AND con.confrelid = pkc.oid "
            + " AND fkn.oid = fkc.relnamespace AND fkc.oid = fka.attrelid AND fka.attnum = con.conkey[pos.n] AND con.conrelid = fkc.oid "
            + " AND con.contype = 'f' ");
    /*
    In version 11 we added Partitioned indexes indicated by relkind = 'I'
    I could have done this using lower(relkind) = 'i' but chose to be explicit
    for clarity
    */
    List<String> args = new ArrayList<>();
    if (!connection.haveMinimumServerVersion(ServerVersion.v11)) {
      sql.append("AND pkic.relkind = 'i' ");
    } else {
      sql.append("AND (pkic.relkind = 'i' OR pkic.relkind = 'I')");
    }

    if (!connection.haveMinimumServerVersion(ServerVersion.v9_0)) {
      sql.append(" AND con.oid = dep.objid AND pkic.oid = dep.refobjid AND dep.classid = 'pg_constraint'::regclass::oid AND dep.refclassid = 'pg_class'::regclass::oid ");
    } else {
      sql.append(" AND pkic.oid = con.conindid ");
    }

    if (primarySchema != null) {
      sql.append(" AND pkn.nspname = ?");
      args.add(primarySchema);
    }
    if (foreignSchema != null) {
      sql.append(" AND fkn.nspname = ?");
      args.add(foreignSchema);
    }
    if (primaryTable != null) {
      sql.append(" AND pkc.relname = ?");
      args.add(primaryTable);
    }
    if (foreignTable != null) {
      sql.append(" AND fkc.relname = ?");
      args.add(foreignTable);
    }

    if (primaryTable != null) {
      sql.append(" ORDER BY fkn.nspname,fkc.relname,con.conname,pos.n");
    } else {
      sql.append(" ORDER BY pkn.nspname,pkc.relname, con.conname,pos.n");
    }
    return executeMetadataStatement(sql.toString(), args);
  }

  @Override
  public ResultSet getImportedKeys(@Nullable String catalog, @Nullable String schema, String table)
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
    List<Tuple> v = new ArrayList<>(); // The new ResultSet tuple stuff

    f[0] = new Field("TYPE_NAME", Oid.VARCHAR);
    f[1] = new Field("DATA_TYPE", Oid.INT2);
    f[2] = new Field("PRECISION", Oid.INT4);
    f[3] = new Field("LITERAL_PREFIX", Oid.VARCHAR);
    f[4] = new Field("LITERAL_SUFFIX", Oid.VARCHAR);
    f[5] = new Field("CREATE_PARAMS", Oid.VARCHAR);
    f[6] = new Field("NULLABLE", Oid.INT2);
    f[7] = new Field("CASE_SENSITIVE", Oid.BOOL);
    f[8] = new Field("SEARCHABLE", Oid.INT2);
    f[9] = new Field("UNSIGNED_ATTRIBUTE", Oid.BOOL);
    f[10] = new Field("FIXED_PREC_SCALE", Oid.BOOL);
    f[11] = new Field("AUTO_INCREMENT", Oid.BOOL);
    f[12] = new Field("LOCAL_TYPE_NAME", Oid.VARCHAR);
    f[13] = new Field("MINIMUM_SCALE", Oid.INT2);
    f[14] = new Field("MAXIMUM_SCALE", Oid.INT2);
    f[15] = new Field("SQL_DATA_TYPE", Oid.INT4);
    f[16] = new Field("SQL_DATETIME_SUB", Oid.INT4);
    f[17] = new Field("NUM_PREC_RADIX", Oid.INT4);

    String sql = "SELECT t.typname,t.oid FROM pg_catalog.pg_type t"
          + " JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) "
          + " WHERE n.nspname  != 'pg_toast'"
          + " AND "
          + " (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))";

    if (connection.getHideUnprivilegedObjects() && connection.haveMinimumServerVersion(ServerVersion.v9_2)) {
      sql += " AND has_type_privilege(t.oid, 'USAGE')";
    }

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(sql);
    // cache some results, this will keep memory usage down, and speed
    // things up a little.
    byte[] bZero = connection.encodeString("0");
    byte[] b10 = connection.encodeString("10");
    byte[] bf = connection.encodeString("f");
    byte[] bt = connection.encodeString("t");
    byte[] bliteral = connection.encodeString("'");
    byte[] bNullable =
              connection.encodeString(Integer.toString(DatabaseMetaData.typeNullable));
    byte[] bSearchable =
              connection.encodeString(Integer.toString(DatabaseMetaData.typeSearchable));

    TypeInfo ti = connection.getTypeInfo();
    if (ti instanceof TypeInfoCache) {
      ((TypeInfoCache) ti).cacheSQLTypes();
    }

    while (rs.next()) {
      byte[] @Nullable [] tuple = new byte[19][];
      String typname = castNonNull(rs.getString(1));
      int typeOid = (int) rs.getLong(2);

      tuple[0] = connection.encodeString(typname);
      int sqlType = connection.getTypeInfo().getSQLType(typname);
      tuple[1] =
          connection.encodeString(Integer.toString(sqlType));

      /* this is just for sorting below, the result set never sees this */
      tuple[18] = BigInteger.valueOf(sqlType).toByteArray();

      tuple[2] = connection
          .encodeString(Integer.toString(connection.getTypeInfo().getMaximumPrecision(typeOid)));

      // Using requiresQuoting(oid) would might trigger select statements that might fail with NPE
      // if oid in question is being dropped.
      // requiresQuotingSqlType is not bulletproof, however, it solves the most visible NPE.
      if (connection.getTypeInfo().requiresQuotingSqlType(sqlType)) {
        tuple[3] = bliteral;
        tuple[4] = bliteral;
      }

      tuple[6] = bNullable; // all types can be null
      tuple[7] = connection.getTypeInfo().isCaseSensitive(typeOid) ? bt : bf;
      tuple[8] = bSearchable; // any thing can be used in the WHERE clause
      tuple[9] = connection.getTypeInfo().isSigned(typeOid) ? bf : bt;
      tuple[10] = bf; // false for now - must handle money
      tuple[11] = bf; // false - it isn't autoincrement
      tuple[13] = bZero; // min scale is zero
      // only numeric can supports a scale.
      tuple[14] = typeOid == Oid.NUMERIC ? connection.encodeString("1000") : bZero;

      // 12 - LOCAL_TYPE_NAME is null
      // 15 & 16 are unused so we return null
      tuple[17] = b10; // everything is base 10
      v.add(new Tuple(tuple));

      // add pseudo-type serial, bigserial, smallserial
      if ("int4".equals(typname)) {
        byte[] @Nullable [] tuple1 = tuple.clone();

        tuple1[0] = connection.encodeString("serial");
        tuple1[11] = bt;
        v.add(new Tuple(tuple1));
      } else if ("int8".equals(typname)) {
        byte[] @Nullable [] tuple1 = tuple.clone();

        tuple1[0] = connection.encodeString("bigserial");
        tuple1[11] = bt;
        v.add(new Tuple(tuple1));
      } else if ("int2".equals(typname) && connection.haveMinimumServerVersion(ServerVersion.v9_2)) {
        byte[] @Nullable [] tuple1 = tuple.clone();

        tuple1[0] = connection.encodeString("smallserial");
        tuple1[11] = bt;
        v.add(new Tuple(tuple1));
      }

    }
    rs.close();
    stmt.close();

    Collections.sort(v, new Comparator<Tuple>() {
      @Override
      public int compare(Tuple o1, Tuple o2) {
        int i1 = ByteConverter.bytesToInt(castNonNull(o1.get(18)));
        int i2 = ByteConverter.bytesToInt(castNonNull(o2.get(18)));
        return i1 < i2 ? -1 : (i1 == i2 ? 0 : 1);
      }
    });
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
    return emptyResult(
        col("TYPE_CAT"),
        col("TYPE_SCHEM"),
        col("TYPE_NAME"),
        col("CLASS_NAME"),
        col("DATA_TYPE"),
        col("REMARKS"),
        col("BASE_TYPE"));
  }

  @Override
  public Connection getConnection() throws SQLException {
    return connection;
  }

  protected Statement createMetaDataStatement() throws SQLException {
    Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.CONCUR_READ_ONLY);
    statement.closeOnCompletion();
    return statement;
  }

  private ResultSet executeMetadataStatement(String sql, List<String> args) throws SQLException {
    return prepareMetaDataStatement(sql, args).executeQuery();
  }

  private PreparedStatement prepareMetaDataStatement(String sql, List<String> args) throws SQLException {
    PreparedStatement ps = connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.CONCUR_READ_ONLY);
    for (int i = 0; i < args.size(); i++) {
      String arg = args.get(i);
      ps.setString(i + 1, arg);
    }
    // We know the statements won't be reused after processing the ResultSet, so we configure
    // the statement to close on ResultSet.close. It enables the query to release to the pool,
    // so the query gets server-prepared
    ps.closeOnCompletion();
    return ps;
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
    int columns = 17;
    String currentCatalog = connection.getCatalog();

    Field[] f = new Field[columns];
    List<Tuple> v = new ArrayList<>();

    f[0] = new Field("FUNCTION_CAT", Oid.VARCHAR);
    f[1] = new Field("FUNCTION_SCHEM", Oid.VARCHAR);
    f[2] = new Field("FUNCTION_NAME", Oid.VARCHAR);
    f[3] = new Field("COLUMN_NAME", Oid.VARCHAR);
    f[4] = new Field("COLUMN_TYPE", Oid.INT2);
    f[5] = new Field("DATA_TYPE", Oid.INT2);
    f[6] = new Field("TYPE_NAME", Oid.VARCHAR);
    f[7] = new Field("PRECISION", Oid.INT2);
    f[8] = new Field("LENGTH", Oid.INT4);
    f[9] = new Field("SCALE", Oid.INT2);
    f[10] = new Field("RADIX", Oid.INT2);
    f[11] = new Field("NULLABLE", Oid.INT2);
    f[12] = new Field("REMARKS", Oid.VARCHAR);
    f[13] = new Field("CHAR_OCTET_LENGTH", Oid.INT4);
    f[14] = new Field("ORDINAL_POSITION", Oid.INT4);
    f[15] = new Field("IS_NULLABLE", Oid.VARCHAR);
    f[16] = new Field("SPECIFIC_NAME", Oid.VARCHAR);

    if (catalog != null && !catalog.equals(currentCatalog)) {
      return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, v);
    }

    StringBuilder sql = new StringBuilder(
        "SELECT current_database() AS current_database, n.nspname,p.proname,p.prorettype,p.proargtypes, t.typtype,t.typrelid, "
        + " p.proargnames, p.proargmodes, p.proallargtypes, p.oid "
        + " FROM pg_catalog.pg_proc p, pg_catalog.pg_namespace n, pg_catalog.pg_type t "
        + " WHERE p.pronamespace=n.oid AND p.prorettype=t.oid ");
    List<String> args = new ArrayList<>();
    if (schemaPattern != null) {
      sql.append(" AND n.nspname LIKE ?");
      args.add(schemaPattern);
    }
    if (functionNamePattern != null) {
      sql.append(" AND p.proname LIKE ?");
      args.add(functionNamePattern);
    }
    sql.append(" ORDER BY n.nspname, p.proname, p.oid::text ");

    byte[] isnullableUnknown = new byte[0];

    PreparedStatement stmt = prepareMetaDataStatement(sql.toString(), args);
    ResultSet rs = stmt.executeQuery();
    byte[] catalogName = currentCatalog.getBytes(Charset.defaultCharset());
    while (rs.next()) {
      byte[] schema = rs.getBytes("nspname");
      byte[] functionName = rs.getBytes("proname");
      byte[] specificName =
          connection.encodeString(rs.getString("proname") + "_" + rs.getString("oid"));
      int returnType = (int) rs.getLong("prorettype");
      String returnTypeType = rs.getString("typtype");
      int returnTypeRelid = (int) rs.getLong("typrelid");

      String strArgTypes = castNonNull(rs.getString("proargtypes"));
      StringTokenizer st = new StringTokenizer(strArgTypes);
      List<Long> argTypes = new ArrayList<>();
      while (st.hasMoreTokens()) {
        argTypes.add(Long.valueOf(st.nextToken()));
      }

      String[] argNames = null;
      Array argNamesArray = rs.getArray("proargnames");
      if (argNamesArray != null) {
        argNames = (String[]) argNamesArray.getArray();
      }

      String[] argModes = null;
      Array argModesArray = rs.getArray("proargmodes");
      if (argModesArray != null) {
        argModes = (String[]) argModesArray.getArray();
      }

      int numArgs = argTypes.size();

      Long[] allArgTypes = null;
      Array allArgTypesArray = rs.getArray("proallargtypes");
      if (allArgTypesArray != null) {
        allArgTypes = (Long[]) allArgTypesArray.getArray();
        numArgs = allArgTypes.length;
      }

      // decide if we are returning a single column result.
      if ("b".equals(returnTypeType) || "d".equals(returnTypeType) || "e".equals(returnTypeType)
          || ("p".equals(returnTypeType) && argModesArray == null)) {
        byte[] @Nullable [] tuple = new byte[columns][];
        tuple[0] = catalogName;
        tuple[1] = schema;
        tuple[2] = functionName;
        tuple[3] = connection.encodeString("returnValue");
        tuple[4] = connection
            .encodeString(Integer.toString(DatabaseMetaData.functionReturn));
        tuple[5] = connection
            .encodeString(Integer.toString(connection.getTypeInfo().getSQLType(returnType)));
        tuple[6] = connection.encodeString(connection.getTypeInfo().getPGType(returnType));
        tuple[7] = null;
        tuple[8] = null;
        tuple[9] = null;
        tuple[10] = null;
        tuple[11] = connection
            .encodeString(Integer.toString(DatabaseMetaData.functionNullableUnknown));
        tuple[12] = null;
        tuple[14] = connection.encodeString(Integer.toString(0));
        tuple[15] = isnullableUnknown;
        tuple[16] = specificName;

        v.add(new Tuple(tuple));
      }

      // Add a row for each argument.
      for (int i = 0; i < numArgs; i++) {
        byte[] @Nullable [] tuple = new byte[columns][];
        tuple[0] = catalogName;
        tuple[1] = schema;
        tuple[2] = functionName;

        if (argNames != null) {
          tuple[3] = connection.encodeString(argNames[i]);
        } else {
          tuple[3] = connection.encodeString("$" + (i + 1));
        }

        int columnMode = DatabaseMetaData.functionColumnIn;
        if (argModes != null && argModes[i] != null) {
          if ("o".equals(argModes[i])) {
            columnMode = DatabaseMetaData.functionColumnOut;
          } else if ("b".equals(argModes[i])) {
            columnMode = DatabaseMetaData.functionColumnInOut;
          } else if ("t".equals(argModes[i])) {
            columnMode = DatabaseMetaData.functionReturn;
          }
        }

        tuple[4] = connection.encodeString(Integer.toString(columnMode));

        int argOid;
        if (allArgTypes != null) {
          argOid = allArgTypes[i].intValue();
        } else {
          argOid = argTypes.get(i).intValue();
        }

        tuple[5] =
            connection.encodeString(Integer.toString(connection.getTypeInfo().getSQLType(argOid)));
        tuple[6] = connection.encodeString(connection.getTypeInfo().getPGType(argOid));
        tuple[7] = null;
        tuple[8] = null;
        tuple[9] = null;
        tuple[10] = null;
        tuple[11] =
            connection.encodeString(Integer.toString(DatabaseMetaData.functionNullableUnknown));
        tuple[12] = null;
        tuple[14] = connection.encodeString(Integer.toString(i + 1));
        tuple[15] = isnullableUnknown;
        tuple[16] = specificName;

        v.add(new Tuple(tuple));
      }

      // if we are returning a multi-column result.
      if ("c".equals(returnTypeType) || ("p".equals(returnTypeType) && argModesArray != null)) {
        PreparedStatement columnstmt = connection.prepareStatement(
            "SELECT a.attname,a.atttypid FROM pg_catalog.pg_attribute a "
                + " WHERE a.attrelid = ?"
                + " AND NOT a.attisdropped AND a.attnum > 0 ORDER BY a.attnum ");
        columnstmt.setInt(1, returnTypeRelid);
        ResultSet columnrs = columnstmt.executeQuery();
        while (columnrs.next()) {
          int columnTypeOid = (int) columnrs.getLong("atttypid");
          byte[] @Nullable [] tuple = new byte[columns][];
          tuple[0] = catalogName;
          tuple[1] = schema;
          tuple[2] = functionName;
          tuple[3] = columnrs.getBytes("attname");
          tuple[4] = connection
              .encodeString(Integer.toString(DatabaseMetaData.functionColumnResult));
          tuple[5] = connection
              .encodeString(Integer.toString(connection.getTypeInfo().getSQLType(columnTypeOid)));
          tuple[6] = connection.encodeString(connection.getTypeInfo().getPGType(columnTypeOid));
          tuple[7] = null;
          tuple[8] = null;
          tuple[9] = null;
          tuple[10] = null;
          tuple[11] = connection
              .encodeString(Integer.toString(DatabaseMetaData.functionNullableUnknown));
          tuple[12] = null;
          tuple[14] = connection.encodeString(Integer.toString(0));
          tuple[15] = isnullableUnknown;
          tuple[16] = specificName;

          v.add(new Tuple(tuple));
        }
        columnrs.close();
        columnstmt.close();
      }
    }
    rs.close();
    stmt.close();

    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, v);
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

  private static Field col(String name, int oid) {
    return new Field(name, oid);
  }

  private static Field col(String name) {
    return new Field(name, Oid.VARCHAR);
  }

  private ResultSet emptyResult(Field... fields) throws SQLException {
    List<Tuple> tuples = new ArrayList<>();
    byte[] @Nullable [] tuple = new byte[fields.length][];
    for (int i = 0; i < fields.length; i++) {
      tuple[i] = null;
    }
    tuples.add(new Tuple(tuple));
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(fields, tuples);
  }
}
