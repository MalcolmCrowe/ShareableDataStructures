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
public class PyrrhoEntityType {
    PyrrhoEntityManager em;
    String table;
    Method[] pkey;
    /** Creates a new instance of PyrrhoEntity */
    public PyrrhoEntityType(PyrrhoEntityManager e,String t,Method[] k) {
        em=e; table=t; pkey=k;
    }
    
}
