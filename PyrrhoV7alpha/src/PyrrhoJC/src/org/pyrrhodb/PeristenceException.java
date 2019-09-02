// Pyrrho Database Engine by Malcolm Crowe at the University of Paisley
// (c) Malcolm Crowe, University of Paisley 2004-2007
//
// Patent Applied For:
// This software incorporates and is a sample implementation of JournalDB technology covered by 
// British Patent Application No 0620986.0 in the name of the University of Paisley
// entitled "Improvements in and Relating to Database Technology"
// 
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of Paisley

// OPEN SOURCE EDITIONS
package org.pyrrhodb;
import java.util.*;
// This class receives an Exception message from the server
// including an SQLSTATE and additional information
// and combines this into something readable.
// Localisation could be usefully performed on this class
public class PeristenceException extends Exception
{
        // A new Database Exception from the server
	public PeristenceException(String message) 
	{
		message = Format(message,null);
	}
        // A new Database Exception from the server
	public PeristenceException(String message, String ob)
	{
		message = Format(message, new String[] { ob });
	}
        // A new Database Exception from the server
	public PeristenceException(String message, String[] obs)
	{
		message = Format(message, obs);
	}
        // Format a database error message
	String Format(String sig, String[] obs)
	{
		init();
		StringBuffer sb = new StringBuffer();
		int j = 0;
		String fmt = (String)dict.get(sig);
		if (obs == null)
			return fmt;
		while (j < fmt.length())
		{
			char c = fmt.charAt(j++);
			if (c != '{')
				sb.append(c);
			else
			{
				int k = fmt.charAt(j++) - '0';
				if (k < obs.length)
					sb.append(obs[k]);
				j++; // '}'
			}
		}
		return sb.toString();
	}
	static boolean inited = false;
	static Hashtable dict = new Hashtable();
	void init()
	{
		if (inited)
			return;
		dict.put("00000","Transaction conflict");
		dict.put("01010","Warning: column cannot be mapped");
		dict.put("02000","Not found");
		dict.put("0N000","SQLXML mapping error");
		dict.put("0N001","SQLXML mapping error: unmappable XML name");
		dict.put("0N002","SQLXML mapping error: invalid XML character");
		dict.put("0N003","RDF/SPARQL error {0}");
		dict.put("21000","Cardinaility errors from specific codes below");
		dict.put("21101","Element requires a singleton multiset");
		dict.put("22000","Data and  incompatible type errors from specific codes below");
		dict.put("22003","Out of bounds value");
		dict.put("22004","Illegal null value");
		dict.put("22005","Wrong types: expected {0} got {1}");
		dict.put("22007","Expected keyword {0} at {1}");
		dict.put("22008","Minus sign expected (DateTime)");
		dict.put("22009","DateTime format error: {0}");
		dict.put("2200D","Index out of range");
		dict.put("2200G","Type mismatch: expected {0} got {1} at {2}");
		dict.put("2200J","Nonidentical notations with the same name");
		dict.put("2200K","Nonidentical unparsed entities with the same name");
		dict.put("2200N","Invalid XML content");
		dict.put("2200S","Invalid XML comment");
		dict.put("22010","Space expected (DateTime)");
		dict.put("22011","Colon expected (DateTime)");
		dict.put("22012","More than 6 decimal places of seconds precision");
		dict.put("22013","Digit expected (DateTime)");
		dict.put("22014","Year out of range");
		dict.put("22015","Month out of range");
		dict.put("22016","Day out of range");
		dict.put("22017","Hour out of range");
		dict.put("22018","Number in range 0..59 expected (DateTime)");
		dict.put("22019","Unexpected extract {0} (DateTime)");
		dict.put("22021","Illegal character for this char set");
		dict.put("22023","Too few arguments");
		dict.put("22024","Too many arguments");
		dict.put("22101","Bad row compare");
		dict.put("22102","Type mismatch on concatenate");
		dict.put("22103","Multiset element not found");
		dict.put("22104","Incompatible multisets for union");
		dict.put("22105","Incompatible multisets for intersection");
		dict.put("22106","Incompatible multisets for except");
		dict.put("22107","Exponent expected");
		dict.put("22108","Type error in aggregation operation");
		dict.put("22201","Unexpected type {0} for comparison with Decimal");
		dict.put("22202","Incomparable types");
		dict.put("22203","Loss of precision on conversion");
		dict.put("22204","Query expected");
		dict.put("22205","Null value found in table {0}");
		dict.put("22206","Null value not allowed in column {0}");
		dict.put("22207","Row has incorrect length");
		dict.put("23000","Integrity constraint: {0}");
		dict.put("23001","RESTRICT: {0} referenced in {1} (A referenced object cannot be deleted)");
		dict.put("23101","Integrity constraint on referencing table {0} (delete)");
		dict.put("23102","Integrity constraint on referencing table {0} (update)");
		dict.put("23103","This record cannot be updated: {0}");
		dict.put("23201","{0} cannot be updated");
		dict.put("23202","BEFORE data is not updatable");
		dict.put("24000","Cursor status error from specific codes below");
		dict.put("24101","Cursor is not open");
		dict.put("26000","Unexpected label {0}");
		dict.put("28000","No authority {0} in database {1}");
		dict.put("28101","Unknown grantee kind");
		dict.put("28102","Unknown grantee {0}");
		dict.put("2E000","Incorrect Pyrrho connection or security violation from specific codes below");
		dict.put("2E101","This free version is limited to 8MB data files");
		dict.put("2E103","This server disallows database creation by {0}");
		dict.put("2E104","Database is read-only");
		dict.put("2E105","Invalid user {0} for database {1}");
		dict.put("2E106","This operation requires a single-database session");
		dict.put("2E107","This external procedure already has an implementation");
		dict.put("2E108","Stop time was specified, so database is read-only");
		dict.put("2E109","Invalid authority {0} for database {1}");
		dict.put("2E110","Procedure {0} is not marked external");
		dict.put("2E111","User {0} can access no columns of table {1}");
		dict.put("2E201","Connection is not open");
		dict.put("2E202","A reader is already open");
		dict.put("2E203","Unexpected reply");
		dict.put("2E204","Bad data type {0} (internal)");
		dict.put("2E205","Stream closed");
		dict.put("2E206","Internal error: {0}");
		dict.put("2E207","Connection failed");
		dict.put("2E208","Badly formatted connection string {0}");
		dict.put("2E209","Unexpected element {0} in connection string");
		dict.put("2E210","Pyrrho DBMS service on {0} at port {1} not available (or not reachable)");
		dict.put("2E211","Feature {0} is not available in this edition of Pyrrho");
		dict.put("2E212","Database ? is loading. Please try later");
		dict.put("2H000","Collation error: {0}");
		dict.put("34000","No such cursor");
		dict.put("3D000","Could not use database {0} (database not found or damaged)");
		dict.put("3D001","Database {0} not open");
		dict.put("3D002","Cannot detach preloaded databases");
		dict.put("3D003","Remote database no longer accessible");
		dict.put("3D004","Exception reported by remote database: {0}");
		dict.put("3D005","Requested operation not supported by this edition of Pyrrho");
		dict.put("3D006","Database {0} incorrectly terminated or damaged");
		dict.put("3D007","Pathname not supported by this edition of Pyrrho");
		dict.put("3D008","Database file has been damaged");
		dict.put("3D009","Automatic database recovery from file damage failed: {0}");
		dict.put("3E001","Application should provide static method for {0}");
		dict.put("3E002","No constructor found with arity {0}");
		dict.put("3E003","Error during call of constructor for {0}: {1}");
		dict.put("3E004","Application should provide instance method for {0}");
		dict.put("3E005","Error during Callback of {0}: {1}");
		dict.put("3E006","Application should provide procedure {0}");
		dict.put("42000","Syntax error at {0}");
		dict.put("42101","Illegal character {0}");
		dict.put("42102","Name cannot be null");
		dict.put("42103","Key must have at least one column");
		dict.put("42104","Proposed name conflicts with existing database object (e.g. table already exists)");
		dict.put("42105","Access denied");
		dict.put("42106","Undefined variable {0}");
		dict.put("42107","Table {0} undefined");
		dict.put("42108","Procedure {0} not found");
		dict.put("42109","Table-valued function expected");
		dict.put("42110","Column list has incorrect length: expected {0}, got {1}");
		dict.put("42111","The given key is not found in the referenced table");
		dict.put("42112","Column {0} not found");
		dict.put("42113","Multiset operand required, not {0}");
		dict.put("42114","Object name expected for GRANT, got {0}");
		dict.put("42115","Unexpected object type {0} {1} for GRANT");
		dict.put("42116","Role revoke has ADMIN option not GRANT");
		dict.put("42117","Privilege revoke has GRANT option not ADMIN");
		dict.put("42118","Unsupported CREATE {0}");
		dict.put("42119","Domain {0} not found in database {1}");
		dict.put("4211A","Unknown privilege {0}");
		dict.put("42120","Domain or type must be specified for base column {0}");
		dict.put("42121","Cannot specify collate if domain name given");
		dict.put("42123","NO ACTION is not supported");
		dict.put("42124","Colon expected {0}");
		dict.put("42125","Unknown Alter type {0}");
		dict.put("42126","Unknown SET operation");
		dict.put("42127","Table expected");
		dict.put("42128","Illegal aggregation operation");
		dict.put("42129","WHEN expected");
		dict.put("42130","Ambiguous table or column reference {0}");
		dict.put("42131","Invalid POSITION {0}");
		dict.put("42132","Method {0} not found in type {1}");
		dict.put("42133","Type {0} not found");
		dict.put("42134","FOR phrase is required");
		dict.put("42135","Object {0} not found");
		dict.put("42136","Ambiguous field selector");
		dict.put("42137","Field selector {0} on non-structured type");
		dict.put("42138","Field selector {0} not defined for {1}");
		dict.put("42139",":: on non-type");
		dict.put("42140",":: requires a static method");
		dict.put("42141","{0} requires an instance method");
		dict.put("42142","NEW requires a user-defined type constructor");
		dict.put("42143","{0} specified more than once");
		dict.put("42144","Method selector on non-structured type {0}");
		dict.put("42145","Cannot supply value for generated column {0}");
		dict.put("42146","{0} specified on {1} trigger");
		dict.put("42147","Table {0} already has a primary key");
		dict.put("42148","FOR EACH ROW not specified");
		dict.put("42149","Cannot specify OLD/NEW TABLE for before trigger");
		dict.put("42150","Malformed SQL input (non-terminated string)");
		dict.put("42151","Bad join condition");
		dict.put("42152","Column {0} must be aggregated or grouped");
		dict.put("42153","Table {0} already exists");
		dict.put("42154","Unimplemented or illegal function {0}");
		dict.put("42155","Nested types are not supported");
		dict.put("42156","Column {0} is already in table {1}");
		dict.put("42157","END label {0} does not match start label {1}");
		dict.put("42158","{0} is not the primary key for {1}");
		dict.put("42159","{0} is not a foreign key for {1}");
		dict.put("42160","{0} has no unique constraint");
		dict.put("42161","{0} expected at {1}");
		dict.put("42162","RDF triple expected");
		dict.put("44000","Check condition {0} fails");
		dict.put("44001","Domain check fails for {0} in table {1}");
		dict.put("44002","Table check {0} fails");
		inited = true;
	}
}
