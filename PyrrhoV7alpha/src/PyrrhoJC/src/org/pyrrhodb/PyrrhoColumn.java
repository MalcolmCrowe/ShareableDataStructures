/*
 * PyrrhoColumn.java
 *
 * Created on 10 December 2006, 16:23
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

/**
 *
 * @author Malcolm
 */
public class PyrrhoColumn {
    int type;
    String name;
    /** Creates a new instance of PyrrhoColumn */
    public PyrrhoColumn(String n,int t) {
        name = n; type = t;
    }
    
}
