/*
 * ColumnInfo.java
 *
 * Created on 25 November 2006, 20:33
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

import java.lang.reflect.Method;

/**
 *
 * @author Malcolm
 */
public class ColumnInfo {
    String column;
    Method meth;
    /** Creates a new instance of ColumnInfo */
    public ColumnInfo(Method m) {
        Column c = (Column)m.getAnnotation(Column.class);
        column = c.name();
        meth = m;
        if (column.equals(""))
            column = m.getName();
        int ix = column.lastIndexOf('.');
        if (ix>=0)
            column = column.substring(ix+1);
        if (column.startsWith("get"))
            column = column.substring(3);
    }
}
