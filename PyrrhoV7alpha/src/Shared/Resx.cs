using System;
using System.Text;
using System.Collections;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho
{
    /// <summary>
    /// Summary description for Resx.
    /// </summary>
    public class Resx
    {
#if SILVERLIGHT || WINDOWS_PHONE
        internal static Dictionary<string,string> dict = null;
#else
        public static Hashtable? dict = null;
#endif
        static bool SqlStateDone = false;
        static void InitSqlstate()
        {
            if (SqlStateDone || dict==null)
                return;
            dict.Add("02000", "Not found");
            dict.Add("08000", "Invalid connection name");
            dict.Add("08001", "SQL-client unable to establish SQL-connection");
            dict.Add("08004", "SQL-server rejected establishment of SQL-connection");
            dict.Add("08006", "Transaction failure");
            dict.Add("08007", "Transaction exception - transaction resolution unknown");
            dict.Add("08C00", "Client side threading violation for reader");
            dict.Add("08C01", "Client side threading violation for command");
            dict.Add("08C02", "Client side threading violation for transaction");
            dict.Add("08C03", "An explicit transaction is already active in this thread and connection");
            dict.Add("08C04", "A reader is already open in this thread and connection");
            dict.Add("08C05", "Conflict with an open reader in this thread and connection");
            dict.Add("08C06", "Cannot change connection properties during a transaction");
            dict.Add("0U000", "Attempt to assign to a non-updatable column");
            dict.Add("21000", "Cardinality violation");
            dict.Add("22000", "Data and incompatible type errors for {0}");
            dict.Add("22003", "Out of bounds value");
            dict.Add("22004", "Illegal null value");
            dict.Add("22005", "Error in assignment: expected {0} got {1}");
            dict.Add("22007", "DateTime format error: {0}");
            dict.Add("22008", "Datetime field overflow: {0}");
            dict.Add("2200G", "Most specific type mismatch: expected {0} got {1} at {2}");
            dict.Add("2200N", "Invalid XML content");
            dict.Add("22012", "Division by zero");
            dict.Add("22019", "Invalid escape character");
            dict.Add("2201B", "Invalid regular expression");
            dict.Add("2201C", "Null row not permitted in value");
            dict.Add("2201M", "Namespace {0} not defined");
            dict.Add("22025", "Invalid escape sequence");
            dict.Add("22041", "Invalid RDF format");
            dict.Add("22102", "Type mismatch on concatenate");
            dict.Add("22103", "Multiset element not found");
            dict.Add("22104", "Incompatible multisets for union");
            dict.Add("22105", "Incompatible multisets for intersection");
            dict.Add("22106", "Incompatible multisets for except");
            dict.Add("22107", "Exponent expected");
            dict.Add("22108", "Type error in aggregation operation");
            dict.Add("22109", "Too few arguments");
            dict.Add("22110", "Too many arguments");
            dict.Add("22111", "Circular dependency found");
            dict.Add("22201", "Unexpected type {0} for comparison with Decimal");
            dict.Add("22202", "Incomparable types");
            dict.Add("22203", "Loss of precision on conversion");
            dict.Add("22204", "RowSet expected");
            dict.Add("22205", "Null value found in table {0}");
            dict.Add("22206", "Null value not allowed in column {0}");
            dict.Add("22207", "Row has incorrect length");
            dict.Add("22208", "Mixing named and unnamed columns is not supported");
            dict.Add("22209", "AutoKey is not available for {0}");
            dict.Add("22210", "Illegal assignment of sensitive value");
            dict.Add("22211", "Domain {0} Check constraint fails");
            dict.Add("22212", "Column {0} Check constraint fails");
            dict.Add("22300", "Bad document format: {0}");
            dict.Add("22G0K", "Multi-sourced or multi-destined edge {0}");
            dict.Add("22G0L", "Incomplete edge {0}");
            dict.Add("22G0M", "Potentially infinite output");
            dict.Add("22G0N", "Repeating zeo-length output");
            dict.Add("23000", "Integrity constraint: {0}");
            dict.Add("23001", "RESTRICT: {0} referenced in {1} {2}");
            dict.Add("23002", "RESTRICT: Index {0} is not empty");
            dict.Add("23101", "Integrity constraint on referencing table {0} (delete)");
            dict.Add("23102", "Integrity constraint on referencing table {0} (update)");
            dict.Add("23103", "This record cannot be updated: {0}");
            dict.Add("24000", "Invalid cursor state");
            dict.Add("24101", "Cursor is not open");
            dict.Add("25000", "Invalid transaction state");
            dict.Add("25001", "A transaction is in progress");
            dict.Add("26000", "Invalid SQL statement name {0}");
            dict.Add("27000", "Invalid metadata {0}");
            dict.Add("28000", "Invalid authorisation specification: no {0} in database {1}");
            dict.Add("28101", "Unknown grantee kind");
            dict.Add("28102", "Unknown grantee {0}");
            dict.Add("28104", "Users can only be added to roles");
            dict.Add("28105", "Grant of select: entire row is nullable");
            dict.Add("28106", "Grant of insert: must include all notnull columns");
            dict.Add("28107", "Grant of insert cannot include generated columns");
            dict.Add("28108", "Grant of update: column {0} is not updatable");
            dict.Add("2D000", "Invalid transaction termination");
            dict.Add("2E104", "Database is read-only");
            dict.Add("2E105", "Invalid user for database {1}");
            dict.Add("2E111", "User {0} can access no columns of table {1}");
            dict.Add("2E201", "Transaction is not open");
            dict.Add("2E202", "A reader is already open");
            dict.Add("2E203", "Unexpected reply");
            dict.Add("2E204", "Bad obs type {0} (internal)");
            dict.Add("2E205", "Stream closed");
            dict.Add("2E206", "Internal error: {0}");
            dict.Add("2E208", "Badly formatted connection string {0}");
            dict.Add("2E209", "Unexpected element {0} in connection string");
            dict.Add("2E210", "LOCAL database server does not support distributed or partitioned operation");
            dict.Add("2E300", "The calling assembly does not have type {0}");
            dict.Add("2E301", "Type {0} does not have a default constructor");
            dict.Add("2E302", "Type {0} does not define public field {1}");
            dict.Add("2E303", "Types {0} and {1} do not match");
            dict.Add("2E304", "GET rurl should begin with /");
            dict.Add("2E305", "No Pyrrho service on rurl {0}");
            dict.Add("2E307", "Obtain an up-to-date schema for {0} from Role$Class");
            dict.Add("2F003", "Prohibited SQL-statement attempted");
            dict.Add("2H000", "Collation error: {0}");
            dict.Add("34000", "No such cursor: {0}");
            dict.Add("33000", "Invalid SQL-statement identifier");
            dict.Add("33001", "Error in prepared statement parameters");
            dict.Add("3D000", "Invalid catalog specification {0}");
            dict.Add("3D001", "Database {0} not open");
            dict.Add("3D005", "Requested operation not supported by this edition of Pyrrho");
            dict.Add("3D006", "Database {0} incorrectly terminated or damaged");
            dict.Add("3D007", "Database is not append storage"); // must match server version
            dict.Add("3D008", "Database is append storage"); // must match server version
            dict.Add("3D010", "Invalid Password");
            dict.Add("40000", "Transaction rollback");
            dict.Add("40001", "Transaction conflict {0} {1}");
            dict.Add("40003", "Transaction rollback – statement completion unknown");
            dict.Add("40005", "Transaction rollback - new key conflict with empty query");
            dict.Add("40006", "Transaction conflict: column {0}");
            dict.Add("40007", "Transaction conflict: {0}");
            dict.Add("40008", "Transaction conflict: table {0}");
            dict.Add("40009", "Transaction conflict: record {0} {1}");
            dict.Add("40010", "Transaction conflict: Object {0} has just been dropped");
            dict.Add("40011", "Transaction conflict: Supertype {0} has just been dropped");
            dict.Add("40012", "Transaction conflict: Table {0} has just been dropped");
            dict.Add("40013", "Transaction conflict: Column {0} has just been dropped");
            dict.Add("40014", "Transaction conflict: Record {0} has just been deleted");
            dict.Add("40015", "Transaction conflict: Type {0} has just been dropped");
            dict.Add("40016", "Transaction conflict: Domain {0} has just been dropped");
            dict.Add("40017", "Transaction conflict: Index {0} has just been dropped");
            dict.Add("40021", "Transaction conflict: Domain {0} has just been changed");
            dict.Add("40022", "Transaction conflict: Another domain {0} has just been defined");
            dict.Add("40023", "Transaction conflict: Period {0} has just been changed");
            dict.Add("40024", "Transaction conflict: Versioning has just been defined");
            dict.Add("40025", "Transaction conflict: Table {0} has just been altered");
            dict.Add("40026", "Transaction conflict: Conflicting record {0} has just been added");
            dict.Add("40027", "Transaction conflict: Record {0} has just been referenced");
            dict.Add("40029", "Transaction conflict: Record {0} has just been updated");
            dict.Add("40030", "Transaction conflict: A conflicting table {0} has just been defined");
            dict.Add("40031", "Transaction conflict: A conflicting view {0} has just been defined");
            dict.Add("40032", "Transaction conflict: A conflicting object {0} has just been defined");
            dict.Add("40033", "Transaction conflict: A conflicting trigger for {0} has just been defined");
            dict.Add("40034", "Transaction conflict: Table {0} has just been renamed");
            dict.Add("40035", "Transaction conflict: A conflicting role {0} has just been defined");
            dict.Add("40036", "Transaction conflict: A conflicting routine {0} has just been defined");
            dict.Add("40037", "Transaction conflict: An ordering now uses function {0}");
            dict.Add("40038", "Transaction conflict: Type {0} has just been renamed");
            dict.Add("40039", "Transaction conflict: A conflicting method {0} for {0} has just been defined");
            dict.Add("40040", "Transaction conflict: A conflicting period for {0} has just been defined");
            dict.Add("40041", "Transaction conflict: Conflicting metadata for {0} has just been defined");
            dict.Add("40042", "Transaction conflict: A conflicting index for {0} has just been defined");
            dict.Add("40043", "Transaction conflict: Columns of table {0} have just been changed");
            dict.Add("40044", "Transaction conflict: Column {0} has just been altered");
            dict.Add("40045", "Transaction conflict: A conflicting column {0} has just been defined");
            dict.Add("40046", "Transaction conflict: A conflicting check {0} has just been defined");
            dict.Add("40047", "Transaction conflict: Target object {0} has just been renamed");
            dict.Add("40048", "Transaction conflict: A conflicting ordering for {0} has just been defined");
            dict.Add("40049", "Transaction conflict: Ordering definition conflicts with drop of {0}");
            dict.Add("40050", "Transaction conflict: A conflicting namespace change has occurred");
            dict.Add("40051", "Transaction conflict: Conflict with grant/revoke on {0}");
            dict.Add("40052", "Transaction conflict: Conflicting routine modify for {0}");
            dict.Add("40053", "Transaction conflict: Domain {0} has just been used for insert");
            dict.Add("40054", "Transaction conflict: Domain {0} has just been used for update");
            dict.Add("40055", "Transaction conflict: An insert conflicts with drop of {0}");
            dict.Add("40056", "Transaction conflict: An update conflicts with drop of {0");
            dict.Add("40057", "Transaction conflict: A delete conflicts with drop of {0}");
            dict.Add("40058", "Transaction conflict: An index change conflicts with drop of {0}");
            dict.Add("40059", "Transaction conflict: A constraint change conflicts with drop of {0}");
            dict.Add("40060", "Transaction conflict: A method change conflicts with drop of type {0}");
            dict.Add("40068", "Transaction conflict: Domain {0} has just been altered,conflicts with drop");
            dict.Add("40069", "Transaction conflict: Method {0} has just been changed, conflicts with drop");
            dict.Add("40070", "Transaction conflict: A new ordering conflicts with drop of type {0}");
            dict.Add("40071", "Transaction conflict: A period definition conflicts with drop of {0}");
            dict.Add("40072", "Transaction conflict: A versioning change conflicts with drop of period {0}");
            dict.Add("40073", "Transaction conflict: A read conflicts with drop of {0}");
            dict.Add("40074", "Transaction conflict: A delete conflicts with update of {0}");
            dict.Add("40075", "Transaction conflict: A new reference conflicts with deletion of {0}");
            dict.Add("40076", "Transaction conflict: A conflicting domain or type {0} has just been defined");
            dict.Add("40077", "Transaction conflict: A conflicting change on {0} has just been done");
            dict.Add("40078", "Transaction conflict: Read conflict with alter of {0}");
            dict.Add("40079", "Transaction conflict: Insert conflict with alter of {0}");
            dict.Add("40080", "Transaction conflict: Update conflict with alter of {0}");
            dict.Add("40081", "Transaction conflict: Alter conflicts with drop of {0}");
            dict.Add("40082", "ETag validation failure");
            dict.Add("40083", "Secondary connection conflict on {0}");
            dict.Add("40084", "Remote object has just been changed");
            dict.Add("40085", "Transaction conflict: An update conflicts with delete of {0}");
            dict.Add("40086", "Transaction conflict: An update conflicts with update of {0}");
            dict.Add("42000", "Syntax error at {0}");
            dict.Add("42101", "Illegal character {0}");
            dict.Add("42102", "Name cannot be null");
            dict.Add("42103", "Key must have at least one column");
            dict.Add("42104", "Proposed name conflicts with existing database object (e.g. table already exists)");
            dict.Add("42105", "Access denied");
            dict.Add("42107", "Table {0} undefined");
            dict.Add("42108", "Procedure {0} not found");
            dict.Add("42109", "Assignment target {0} not found");
            dict.Add("42111", "The given key is not found in the referenced table");
            dict.Add("42112", "Column {0} not found");
            dict.Add("42113", "Multiset operand required, not {0}");
            dict.Add("42115", "Unexpected object type {0} {1} for GRANT");
            dict.Add("42116", "Role revoke has ADMIN option not GRANT");
            dict.Add("42117", "Privilege revoke has GRANT option not ADMIN");
            dict.Add("42118", "Unsupported CREATE {0}");
            dict.Add("42119", "Domain {0} not found in database {1}");
            dict.Add("4211A", "Unknown privilege {0}");
            dict.Add("42120", "Domain or type must be specified for base column {0}");
            dict.Add("42123", "NO ACTION is not supported");
            dict.Add("42124", "Colon expected {0}");
            dict.Add("42125", "Unknown Alter type {0}");
            dict.Add("42126", "Unknown SET operation");
            dict.Add("42127", "Table expected");
            dict.Add("42128", "Illegal aggregation operation");
            dict.Add("42129", "WHEN expected");
            dict.Add("42130", "Table is not empty");
            dict.Add("42131", "Invalid POSITION {0}");
            dict.Add("42132", "Method {0} not found in type {1}");
            dict.Add("42133", "Type {0} not found");
            dict.Add("42134", "FOR phrase is required");
            dict.Add("42135", "Object {0} not found");
            dict.Add("42138", "Field selector {0} not defined for {1}");
            dict.Add("42139", ":: on non-type");
            dict.Add("42140", ":: requires a static method");
            dict.Add("42142", "NEW requires a user-defined type constructor");
            dict.Add("42143", "{0} specified more than once");
            dict.Add("42146", "{0} specified on {1} trigger");
            dict.Add("42147", "Table {0} already has a primary key");
            dict.Add("42148", "FOR EACH ROW not specified");
            dict.Add("42149", "Cannot specify OLD/NEW TABLE for before trigger");
            dict.Add("42150", "Malformed SQL input (non-terminated string)");
            dict.Add("42151", "Bad join condition");
            dict.Add("42152", "Non-distributable where condition for update/delete");
            dict.Add("42153", "Table {0} already exists");
            dict.Add("42154", "Unimplemented or illegal function {0}");
            dict.Add("42156", "Column {0} is already in table {1}");
            dict.Add("42157", "END label {0} does not match start label {1}");
            dict.Add("42158", "{0} is not the primary key for {1}");
            dict.Add("42159", "{0} is not a foreign key for {1}");
            dict.Add("42160", "{0} has no unique constraint");
            dict.Add("42161", "{0} expected at {1}");
            dict.Add("42162", "Table period definition for {0} has not been defined");
            dict.Add("42163", "Generated column {0} cannot be used in a constraint");
            dict.Add("42164", "Table {0} has no primary key");
            dict.Add("42166", "Domain {0} already exists");
            dict.Add("42167", "A routine with name {0} and arity {1} already exists");
            dict.Add("42168", "AS GET needs a schema definition");
            dict.Add("42169", "Ambiguous column name {0} needs alias");
            dict.Add("42170", "Column {0} must be aggregated or grouped");
            dict.Add("42171", "A table cannot be placed in a column");
            dict.Add("42172", "Identifier {0} already declared in this block");
            dict.Add("42173", "Method {0} has not been defined");
            dict.Add("42174", "Unsupported rowset modification attempt");
            dict.Add("42175", "Alternative match expressions must bind the same identifiers");
            dict.Add("44000", "Check condition {0} fails");
            dict.Add("44001", "Domain check {0} fails for column {1} in table {2}");
            dict.Add("44002", "Table check {0} fails for table {1}");
            dict.Add("44003", "Column check {0} fails for column {1} in table {2}");
            dict.Add("44004", "Column {0} in table {1} contains nulls, not null cannot be set");
            dict.Add("44005", "Column {0} in table {1} contains values, generation rule cannot be set");
            SqlStateDone = true;
        }
        public static string Format(string sig, params object[] obs)
        {
            try
            {
                dict ??= new Hashtable();
                if (!SqlStateDone)
                    InitSqlstate();
                var fmt = (string?)dict[sig];
                if (fmt == null)
                    return "Signal " + sig;
                StringBuilder sb = new ();
                int j = 0;
                while (j < fmt.Length)
                {
                    char c = fmt[j++];
                    if (c != '{')
                        sb.Append(c);
                    else
                    {
                        int k = fmt[j++] - '0';
                        j++; // }
                        if (k < obs.Length)
                            sb.Append(obs[k].ToString());
                    }
                }
                return sb.ToString();
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
        }

    }
#if SILVERLIGHT
    internal class ErrorsDictionary
    {
        BTree<string,string> tabl = BTree<string,string>.Empty;
        internal void Add(string k, string v)
        {
            tabl +=(k, v);
        }
        internal string this[string k]
        {
            get { return tabl[k];  }
        }
    }
#endif
}

