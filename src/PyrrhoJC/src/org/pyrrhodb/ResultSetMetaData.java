/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pyrrhodb;
import java.util.*;
/**
 *
 * @author Malcolm
 */
public class ResultSetMetaData {
    ResultSetMetaData(List<Column> c, int[] k)
    {
        columns = c;
        key = k;
    }
    public List<Column> columns;
    public int[] key;
    public int getColumnCount()
    {
        return columns.size();
    }
    public String getColumnName(int i)
    {
        return columns.get(i-1).name;
    }
    public String getColumnLabel(int i)
    {
        return columns.get(i-1).name;       
    }
    public String getColumnType(int i)
    {
        return columns.get(i-1).dataTypeName;     
    }
    public int getColumnDisplaySize(int i)
    {
        return columns.get(i-1).displaySize;
    }
}
