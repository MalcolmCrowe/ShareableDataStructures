/*
 * PyrrhoRow.java
 *
 * Created on 07 October 2006, 13:03
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

/**
 *
 * @author Malcolm
 */
public class PyrrhoRow {
    public ResultSetMetaData schema;
    public Versioned check = new Versioned();
    public CellValue[] row;
    /** Creates a new instance of PyrrhoRow */
    public PyrrhoRow(ResultSetMetaData md,Versioned r) {
        schema = md;
        check = r;
        row = new CellValue[schema.columns.size()];
    }
    
}
