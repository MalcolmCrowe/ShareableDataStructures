/*
 * EntityInstance.java
 *
 * Created on 03 January 2007, 14:53
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

/**
 *
 * @author Malcolm
 */
public class EntityInstance {
    EntityType et;
    int dbx;
    long pos;
    Object ent;
    /** Creates a new instance of EntityInstance */
    public EntityInstance(EntityType e,int d,long p) {
        et = e; dbx = d; pos = p;
    }
    
}
