/*
 * PyrrhoEntity.java
 *
 * Created on 24 November 2006, 19:11
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

/**
 *
 * @author Malcolm
 */
public class PyrrhoEntity {
    PyrrhoEntityManager em;
    PyrrhoEntityState state;
    Object cache;
    /** Creates a new instance of PyrrhoEntity */
    public PyrrhoEntity(PyrrhoEntityManager e,PyrrhoEntityState s,Object o) {
        em = e; state = s; cache = o;
    }
    
}
