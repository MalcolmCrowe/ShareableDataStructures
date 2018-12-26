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
            STimestamp = 1,
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
            SSelect = 29;
    static String[] types = new String[]{
        "", "Timestamp", "Integer", "Numeric", "String", "Date", "TimeSpan",
        "Boolean", "Row", "Table", "Column", "Record", "Update", "Delete",
        "Alter", "Drop", "View", "Index", "Begin", "Rollback", "Commit",
        "CreateTable", "CreateIndex", "UpdateSearch", "DeleteSearch",
        "AlterStatement", "DropStatement", "Insert", "Select"
    };

    public static String ToString(int t) {
        return types[t];
    }
}
