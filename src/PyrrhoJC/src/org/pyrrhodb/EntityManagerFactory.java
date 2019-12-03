/*
 * EntityManagerFactory.java
 *
 * Created on 23 November 2006, 21:01
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

import java.util.Map;

/**
 *
 * @author Malcolm
 */
public class EntityManagerFactory {
    String name;
    Map properties;
    EntityManagerFactory(String n,Map map)
    {
        name = n; properties = map;
    }
    
    public EntityManager createEntityManager() {
        return null;
    }

    public EntityManager createEntityManager(Map map) {
        return null;
    }

    public void close() {
    }

    public boolean isOpen() {
        return false;
    }
    
}
