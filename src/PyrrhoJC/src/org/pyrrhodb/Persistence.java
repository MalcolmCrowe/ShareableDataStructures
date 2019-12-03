/*
 * Persistence.java
 *
 * Created on 26 December 2006, 10:57
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;
import java.util.*;
/**
 *
 * @author Malcolm
 */
public class Persistence {
    
    /** Creates a new instance of Persistence */
    public Persistence() {
    }
    protected static final Set<PersistenceProvider> providers = new HashSet<PersistenceProvider>();
    public static String PERSISTENCE_PROVIDER = "Pyrrho DBMS";
    static PersistenceProvider pyrrho = new PersistenceProvider();
    public static EntityManagerFactory createEntityManagerFactory(String name)
    {
        return pyrrho.createEntityManagerFactory(name,new HashMap());
    }
    public static EntityManagerFactory createEntityManagerFactory(String name,Map properties)
    {
        return pyrrho.createEntityManagerFactory(name,properties);
    }
}
