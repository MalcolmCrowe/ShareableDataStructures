/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pyrrhodb;

/**
 *
 * @author Malcolm
 */
public class DatabaseMetaData {
    Connection conn;
    DatabaseMetaData(Connection c)
    {
        conn = c;
    }
    // Many of these constants are no use for Pyrrho
    // as it implements database features in a different way
    public static final short attributeNoNulls=0;
    public static final short attributeNullable=1;
    public static final short attributeNullableUnknown=2;
    public static final int bestRowNotPseudo= -1;
    public static final int bestRowSession= -1;
    public static final int bestRowTemporary= -1;
    public static final int bestRowTransaction= -1;
    public static final int bestRowUnknown= -1;
    public static final int columnNoNulls=1;
    public static final int columnNullable=0;
    public static final int columnNullableUnknown= -1;
    public static final int functionColumnIn=Parameter.IN;
    public static final int functionColumnInOut=Parameter.INOUT;
    public static final int functionColumnOut=Parameter.OUT;   
    public static final int functionColumnResult=Parameter.RESULT;
    public static final int functionColumnUnknown = 0; // illegal
    public static final int functionResultUnknown = DataType.NULL; // illegal
    public static final int functionReturn = Parameter.RESULT;
    public static final int functionReturnsTable = -1;
    public static final int importedKeyCascade = -1;
    public static final int importedKeyInitiallyDeferred = -1; // illegal
    public static final int importedKeyInitiallyImmediate = 1;
    public static final int importedKeyNoAction = -1; // illegal
    public static final int importedKeySetDefault = -1; // illegal
    public static final int procedureColumnIn = Parameter.IN;
    public static final int procedureColumnInOut = Parameter.INOUT;
    public static final int ProcedureColumnOut = Parameter.OUT;
    public static final int ProcedureColumnResult = Parameter.RESULT;
    public static final int ProcedureColumnReturn = -1;
    public static final int ProcedureColumnUnkown = -1; // illegal
    public static final int ProcedureNoNulls = 0;
    public static final int ProcedureNoResult = 0;
    public static final int ProcedureNullable = 0;
    public static final int ProcedureNullableUnknown = -1; // illegal
    public static final int ProcedureReturnsResult = 0;
    public static final int sqlStateSQL = 0;
    public static final int sqlStateSQL99 = 0;
    public static final int SqlStateXOpen = 0;
    public static final int tableIndexClustered = 0;
    public static final int tableIndexHashed = 0;
    public static final int tableIndexOther = 1;
    public static final int tableIndexStatistic = 0;
    public static final int typeNoNulls = -1;
    public static final int typeNullable = 1;
    public static final int typeNullableUnknown = -1;
    public static final int typePredBasic = 0;
    public static final int typePredChar = 0;
    public static final int TypePredNone = 0;
    public static final int TypeSearchable = 1;
    public static final int versionColumnNotPseudo = 0;
    public static final int versionColumnPseudo = 1;
    public static final int versionColumnUnknown = -1;
    short nullability(Column c)
    {
        if (c.generated || c.notNull)
            return 0;
        return 1;
    }
    short parameterFlags(Parameter p)
    {
        return p.mode;
    }
    public ResultSet getTables(String catalog,String schemaPattern,
            String tableNamePattern, String[] types) throws Exception
    {
        Statement s = conn.createStatement();
        String cm = "select \"Name\" from \"Role$Table\" ";
        if (tableNamePattern.length()>0)
            cm = cm+"where \\\"Name\\\" like "+tableNamePattern;
        return s.executeQuery(cm);
    }
    public String getDatabaseProductName()
    {
        return "Pyrrho DBMS";
    }
}
