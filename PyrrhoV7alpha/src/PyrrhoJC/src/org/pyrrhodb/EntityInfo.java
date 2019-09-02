/*
 * EntityInfo.java
 *
 * Created on 24 November 2006, 19:11
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

import java.lang.reflect.*;

/**
 *
 * @author Malcolm
 */
public class EntityInfo {
    EntityState state;
    Object entity;
    private Object[] cache = null;
    /**
     * Creates a new instance of EntityInfo
     */
    public EntityInfo(EntityState s,Object e) {
        state = s; entity = e;
    }
    void revert(EntityType et) throws IllegalAccessException,InvocationTargetException {
        for (int j=0;j<et.cols.length; j++)
            et.cols[j].meth.invoke(entity,cache[j]);
    }
    void setCache(EntityType et,Object obj) throws IllegalAccessException,
        InvocationTargetException {
        if (cache==null)
            cache = new Object[et.cols.length];
        for (int j=0;j<et.cols.length;j++)
            cache[j]=et.cols[j].meth.invoke(obj);
            
    }
}
