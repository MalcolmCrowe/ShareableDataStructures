/*
 * PersistenceProvider.java
 *
 * Created on 23 November 2006, 20:15
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

import java.util.Map;
import java.lang.reflect.*;
/**
 *
 * @author Malcolm
 */
public class PersistenceProvider {
    
    /**
     * Creates a new instance of PersistenceProvider
     */
    public PersistenceProvider() {
    }

    public EntityManagerFactory createEntityManagerFactory(String emName, Map map) {
        return new EntityManagerFactory(emName,map);
    }

    public EntityManagerFactory createContainerEntityManagerFactory(PersistenceUnitInfo info, Map map) {
         return null;
    }
    
    public static void Context(Object ob)
    {
         try{
            for(Field f : ob.getClass().getDeclaredFields())
            {
                PersistenceContext ctx = (PersistenceContext)f.getAnnotation(PersistenceContext.class);
                if (ctx!=null)
                    f.set(ob,new EntityManager(ctx));
            }
         } catch(Exception e)
         {
             e.printStackTrace();
         }
    }
}
