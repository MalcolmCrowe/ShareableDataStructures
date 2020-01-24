/*
 * EntityFinder.java
 *
 * Created on 25 November 2006, 18:35
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

/**
 *
 * @author Malcolm
 */
public class EntityFinder {
    
    Class type;
    Object key;
    /** Creates a new instance of EntityFinder */
    public EntityFinder(Class t,Object k) {
        type = t; key = k;
    }
    public EntityFinder() {}
    public boolean equals(Object obj) {
        EntityFinder that = (EntityFinder)obj;
        return type==that.type && key.equals(that.key);
    }
}
