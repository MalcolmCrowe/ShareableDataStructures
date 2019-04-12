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
            SCreateColumn = 23,
            SUpdateSearch = 24,
            SDeleteSearch = 25,
            SAlterStatement = 26,
            SInsert = 27,
            SSelect = 28,
            EoF = 39,
            Get = 30,
            Insert = 31,
            Read = 32,
            Done = 33,
            Exception = 34,
            SExpression = 35,
            SFunction = 36,
            SValues = 37,
            SOrder = 38,
            SBigInt = 39,
            SInPredicate = 40,
            DescribedGet = 41,
            SGroupQuery = 42,
            STableExp = 43,
            SAlias = 44,
            SSelector = 45,
            SArg = 46,
            SRole = 47,
            SUser = 48,
            SName = 49,
            SNames = 50,
            SQuery = 51, // only used for "STATIC"
            SSysTable = 52;
    static String[] types = new String[]{
        "", "?", "Integer", "Numeric", "String", "Date", "TimeSpan",//0-6
        "Boolean", "Row", "Table", "Column", "Record", "Update", "Delete",//7-13
        "Alter", "Drop", "View", "Index", "Search", "Begin", "Rollback", "Commit",//14-21
        "CreateTable", "CreateIndex", "UpdateSearch", "DeleteSearch",//22-25
        "AlterStatement", "SInsert", "SSelect",//26-28
        "EoF","Get","Insert","Read","Done","Exception","SExpression",//29-35
        "SFunction","SValues","SOrder","SBigInt","SInPredicate",//36-40
        "DescribedGet","SGroupQuery","STableExp","SAlias",//41-44
        "SSelector","SArg","SRole","SUser","SName","SNames",//45-50
        "SQuery","SSysTable" //51-52
    };

    public static String toString(int t) {
        return types[t];
    }
}
