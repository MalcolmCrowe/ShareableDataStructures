/*
 * PyrrhoEntity.java
 *
 * Created on 23 November 2006, 21:14
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
public class EntityType {
    String table;
    ColumnInfo key;
    ColumnInfo[] cols;
    /** Creates a new instance of PyrrhoEntity */
    public EntityType(String t,ColumnInfo k,ColumnInfo[] c) {
        table=t; key = k; cols = c;
    }
    
}
