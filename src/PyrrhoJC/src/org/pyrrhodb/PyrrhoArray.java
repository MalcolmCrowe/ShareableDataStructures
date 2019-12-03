/*
 * PyrrhoArray.java
 *
 * Created on 25 November 2006, 23:22
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

/**
 *
 * @author Malcolm
 */
public class PyrrhoArray {
    String kind;
    Column col;
    CellValue[] data;
    /** Creates a new instance of PyrrhoArray */
    PyrrhoArray(String k,Column c,int n) {
        kind = k;
        col = c;
        data = new CellValue[n];
    }
    
}
