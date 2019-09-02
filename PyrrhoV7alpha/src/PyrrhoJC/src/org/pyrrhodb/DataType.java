/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pyrrhodb;
import java.util.ArrayList;
/**
 *
 * @author 66668214
 */
public class DataType {
    private static short anonType = 16;
    String name;
    short basicType;
    boolean nullable;
    ArrayList<Column> columns = null;
    // Pyrrho predefined constants: do not change these
    public static short NULL = 0;
    public static short INTEGER = 1;
    public static short NUMERIC = 2;
    public static short STRING = 3;
    public static short TIMESTAMP = 4;
    public static short BLOB = 5;
    public static short ROW = 6;
    public static short ARRAY = 7;
    public static short REAL = 8;
    public static short BOOLEAN = 9;
    public static short INTERVAL = 10;
    public static short TIMESPAN = 11;
    public static short UDT = 12;
    public static short DATE = 13;
    public static short PASSWORD = 14;
    public static short MULTISET = 15; 
    DataType(String nm,short t,boolean n)
    {
        name = nm; basicType=t; nullable=n;
        if (t==UDT)
            basicType = ++anonType;
    }
    public static short fromSqlType(int sqlType) throws DatabaseException
    {
        switch(sqlType)
        {
            case 2003: return ARRAY;
            case -5: return INTEGER; // bigint
            case -2: return BLOB; // binary
            case -7: return BOOLEAN; // bit
            case 2004: return BLOB;
            case 16: return BOOLEAN;
            case 1: return STRING; // char
            case 2005: return STRING; // clob
            // datalink unsupported
            case 91: return DATE; 
            case 3: return NUMERIC; // decimal
            // distinct is a constraint not a type
            case 8: return REAL; // double
            case 6: return REAL; //float
            case 4: return INTEGER; 
            case 2000: return UDT; // java_object
            case -16: return STRING; // longnvarchar
            case -4: return BLOB; // longvarbinary
            case -1: return STRING; // longvarchar
            case -15: return STRING; // nchar
            case 2011: return STRING; // nclob
            case 0: return NULL;
            case 2: return NUMERIC; 
            case -9: return STRING; // nvarchar
            case 1111: return UDT; // other
            case 7: return REAL; 
            // ref unsupported
            case -8: return STRING; // rowid
            case 5: return INTEGER; // smallint
            case 2009: return STRING; // sqlxml
            case 2002: return ROW; // struct
            case 92: return TIMESPAN; // time
            case 93: return TIMESTAMP;
            case -6: return INTEGER; // tinyint
            case -3: return BLOB; // varbinary
            case 12: return STRING; // varchar
            default:
                throw new DatabaseException("22000");
        }
    }
}
