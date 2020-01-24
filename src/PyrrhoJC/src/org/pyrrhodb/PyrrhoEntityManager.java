/*
 * PyrrhoEntityManager.java
 *
 * Created on 23 November 2006, 21:06
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

import java.lang.reflect.Type;
import java.util.Hashtable;
import java.util.Map;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Query;

/**
 *
 * @author Malcolm
 */
public class PyrrhoEntityManager implements EntityManager{
    
    Map connectionProperties;
    Map<Object,PyrrhoEntity> entities;
    Map<Type, PyrrhoEntityType> types;
    boolean transaction = false;
    /** Creates a new instance of PyrrhoEntityManager */
    PyrrhoEntityManager(Map cdata) {
        connectionProperties = cdata;
        entities = new Hashtable<Object,PyrrhoEntity>();
        types = new Hashtable<Type,PyrrhoEntityType>();
    }

    public void persist(Object entity) {
    }

    public <T> T merge(T entity) {
        return null;
    }

    public void remove(Object entity) {
    }

    public <T> T find(Class<T> cl, Object obj) {
        return null;
    }

    public <T> T getReference(Class<T> cl , Object obj) {
        return null;
    }

    public void flush() {
    }

    public void setFlushMode(FlushModeType flushMode) {
    }

    public FlushModeType getFlushMode() {
        return null;
    }

    public void lock(Object entity, LockModeType lockMode) {
    }

    public void refresh(Object entity) {
    }

    public void clear() {
        entities.clear();
    }

    public boolean contains(Object entity) {
        return entities.containsKey(entity);
    }

    public Query createQuery(String qlString) {
         return new PyrrhoQuery(this,"",true,qlString,Object.class);
    }

    public Query createNamedQuery(String name) {
        return new PyrrhoQuery(this,name,false,"",Object.class);
    }

    public Query createNativeQuery(String sqlString) {
        return new PyrrhoQuery(this,"",false,sqlString,Object.class);
    }

    public Query createNativeQuery(String sqlString, Class resultClass) {
        return new PyrrhoQuery(this,"",false,sqlString,resultClass);
    }

    public Query createNativeQuery(String sqlString, String resultSetMapping) {
        return null;
    }

    public void joinTransaction() {
        transaction = true;
    }

    public Object getDelegate() {
        return null;
    }

    public void close() {
    }

    public boolean isOpen() {
        return false;
    }

    public EntityTransaction getTransaction() {
        return null;
    }
    
}
