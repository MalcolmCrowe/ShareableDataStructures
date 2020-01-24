/*
 * PyrrhoEntityManagerFactory.java
 *
 * Created on 23 November 2006, 21:01
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

import java.util.Map;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

/**
 *
 * @author Malcolm
 */
public class PyrrhoEntityManagerFactory implements EntityManagerFactory {
    String name;
    Map properties;
    PyrrhoEntityManagerFactory(String n,Map map)
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
