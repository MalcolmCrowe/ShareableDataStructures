/*
 * EntityInstances.java
 *
 * Created on 05 January 2007, 13:48
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

/**
 *
 * @author Malcolm
 */
public class EntityInstances {
    EntityType et;
    EntityInstance[] eis;
    /** Creates a new instance of EntityInstance */
    public EntityInstances(EntityType e,int n) {
        et = e; eis = new EntityInstance[n];
    }
}
