/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 *
 * @author Malcolm
 */
public class Types {

    public static final int Serialisable = 0,
            STimestamp = 1, // not used
            SInteger = 2,
            SNumeric = 3,
            SString = 4,
            SDate = 5,
            STimeSpan = 6,
            SBoolean = 7,
            SRow = 8,
            STable = 9,
            SColumn = 10,
            SRecord = 11,
            SUpdate = 12,
            SDelete = 13,
            SAlter = 14,
            SDrop = 15,
            SView = 16,
            SIndex = 17,
            SSearch = 18,
            SBegin = 19,
            SRollback = 20,
            SCommit = 21,
            SCreateTable = 22,
            SCreateIndex = 23,
            SUpdateSearch = 24,
            SDeleteSearch = 25,
            SAlterStatement = 26,
            SDropStatement = 27,
            SInsert = 28,
            SSelect = 29,
            EoF = 30,
            Get = 31,
            Insert = 32,
            Read = 33,
            Done = 34,
            Exception = 35,
            SExpression = 36,
            SFunction = 37,
            SValues = 38,
            SOrder = 39,
            SBigInt = 40,
            SInPredicate = 41,
            DescribedGet = 42,
            SGroupQuery = 43,
            STableExp = 44,
            SAliasedTable = 45,
            SJoin = 46;
    static String[] types = new String[]{
        "", "?", "Integer", "Numeric", "String", "Date", "TimeSpan",//0-6
        "Boolean", "Row", "Table", "Column", "Record", "Update", "Delete",//7-13
        "Alter", "Drop", "View", "Index", "Search", "Begin", "Rollback", "Commit",//14-21
        "CreateTable", "CreateIndex", "UpdateSearch", "DeleteSearch",//22-25
        "AlterStatement", "DropStatement", "SInsert", "SSelect",//26-29
        "EoF","Get","Insert","Read","Done","Exception","SExpression",//30-36
        "SFunction","SValues","SOrder","SBigInt","SInPredicate",//37-41
        "DescribedGet","SGroupQuery","STableExp","SAliasedTable"//42-45
    };

    public static String toString(int t) {
        return types[t];
    }
}
