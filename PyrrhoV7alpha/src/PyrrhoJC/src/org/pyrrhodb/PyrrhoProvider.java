/*
 * PyrrhoProvider.java
 *
 * Created on 23 November 2006, 20:15
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

import java.util.Map;
import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceProvider;
import javax.persistence.spi.PersistenceUnitInfo;

/**
 *
 * @author Malcolm
 */
public class PyrrhoProvider implements PersistenceProvider {
    
    /** Creates a new instance of PyrrhoProvider */
    public PyrrhoProvider() {
    }

    public EntityManagerFactory createEntityManagerFactory(String emName, Map map) {
        return new PyrrhoEntityManagerFactory(emName,map);
    }

    public EntityManagerFactory createContainerEntityManagerFactory(PersistenceUnitInfo info, Map map) {
         return null;
    }
    
}
