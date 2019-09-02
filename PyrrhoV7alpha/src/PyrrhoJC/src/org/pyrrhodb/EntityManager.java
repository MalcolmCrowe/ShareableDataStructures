/*
 * EntityManager.java
 *
 * Created on 23 November 2006, 21:06
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

import java.io.IOException;
import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;

/**
 *
 * @author Malcolm
 */
public class EntityManager {
    
    Map<String,String> properties = new Hashtable<String,String>();
    Map<Object, EntityInfo> entities = new Hashtable<Object,EntityInfo>();
    Map<EntityFinder,EntityInfo> cache = new Hashtable<EntityFinder,EntityInfo>();
    Map<Type, EntityType> types = new Hashtable<Type,EntityType>();
    EntityTransaction transaction = null;
    Connection connection = null;
    boolean closed = false;
    /**
     * Creates a new instance of EntityManager
     */
    EntityManager(Map cdata) {
        for (Object k : cdata.keySet())
            properties.put(k.toString(),cdata.get(k).toString());
        entities = new Hashtable<Object,EntityInfo>();
        types = new Hashtable<Type,EntityType>();
    }
    
    public EntityManager(PersistenceContext ctx) {
            for (PersistenceProperty p:ctx.properties())
                properties.put(p.name(),p.value());
    }
    
    void Connect() throws IllegalStateException,IOException {
        if (closed)
            throw new IllegalStateException();
        if (connection!=null)
            return;
        connection = new Connection(properties);
    }
    
    public void persist(Object entity) throws EntityExistsException,
            IllegalStateException,IllegalArgumentException,
            IllegalAccessException, InvocationTargetException {
        if (closed)
            throw new IllegalStateException();
        if (entities.containsKey(entity))
            throw new EntityExistsException();
        EntityType tp = ETfor(entity.getClass());
        EntityInfo ei = new EntityInfo(EntityState.NEW,entity);
        EntityFinder ef = EFfor(entity,tp);
        ei.setCache(tp,entity);
        if (cache.containsKey(ef))
            throw new EntityExistsException();
        entities.put(entity,ei);
        cache.put(ef,ei);
    }
    
    public <T> T merge(T entity) throws IllegalStateException,
            IllegalArgumentException,EntityExistsException,
            IllegalAccessException, InvocationTargetException
            {
        if (closed)
            throw new IllegalStateException();
        EntityInfo ei = entities.get(entity);
        if (ei==null) {
            persist(entity);
            return entity;
        }
        EntityType et = ETfor(entity.getClass());
        EntityFinder ef = EFfor(entity,et);
        T next = (T)cache.get(ef).entity;
        if (next!=null && next!=entity) {
            EntityInfo nei = entities.get(next);
            nei.setCache(et,entity);
            return next;
        }
        ei.setCache(et,entity);
        return entity;
    }
    
    public void remove(Object entity) throws IllegalStateException {
        if (closed)
            throw new IllegalStateException();
        entities.remove(entity);
    }
    
    public <T> T find(Class<T> cl, Object obj) throws IllegalStateException, IllegalArgumentException,IOException  {
        Connect();
        EntityType tp = ETfor(cl);
        if (tp==null)
            throw new IllegalArgumentException();
        String a = "a";
        if (a.equals(tp.table))
            a = "b";
        return (T)connection.getResults("select "+a+" from "+tp.table+
                " "+a+" where "+tp.key.column+"="+formatColumn(tp.key,obj))[0];
    }
    
    EntityFinder EFfor(Object en,EntityType tp) throws IllegalArgumentException,
            IllegalAccessException, InvocationTargetException {
        EntityFinder ef = new EntityFinder();
        ef.type = en.getClass();
        if (tp==null)
            throw new IllegalArgumentException();
        ef.key = tp.key.meth.invoke(en);
        return ef;
    }
    
    EntityType ETfor(Class cl) {
        try {
            EntityType tp = types.get(cl);
            if (tp==null) {
                Entity e = (Entity)cl.getAnnotation(Entity.class);
                String tn = e.name();
                if (tn.equals(""))
                    tn = cl.getName();
                int ix = tn.lastIndexOf('.');
                if (ix>=0)
                    tn = tn.substring(ix+1);
                Method k = null;
                ArrayList al = new ArrayList();
                for (Method m: cl.getDeclaredMethods()) {
                    if (m.isAnnotationPresent(Id.class))
                        k = m;
                    if (m.isAnnotationPresent(Column.class))
                        al.add(new ColumnInfo(m));
                }
                ColumnInfo[] cols = new ColumnInfo[al.size()];
                for (int j=0;j<al.size();j++)
                    cols[j] = (ColumnInfo)al.get(j);
                tp = new EntityType(tn,new ColumnInfo(k),cols);
                types.put(cl,tp);
            }
            return tp;
        } catch(Exception e) {}
        return null;
    }
    
    String formatColumn(ColumnInfo c,Object v){
        Class r = c.meth.getReturnType();
        if (r==String.class)
            return "'"+v.toString()+"'";
        return v.toString();
    }
    
    public <T> T getReference(Class<T> cl , Object obj) throws
            IllegalStateException, IllegalArgumentException,
            InstantiationException, IllegalAccessException,
            InvocationTargetException  
    {
        if (closed)
            throw new IllegalStateException();
        EntityType tp = ETfor(cl);
        if (tp==null)
            throw new IllegalArgumentException();
        EntityFinder ef = new EntityFinder(cl,obj);
        T entity = (T)cache.get(ef);
        if (entity==null)
        {
            entity = cl.newInstance();
            tp.key.meth.invoke(entity,obj);
            EntityInfo ei = new EntityInfo(EntityState.REFERENCE,entity);
            ei.setCache(tp,entity);
            cache.put(ef,ei);
            entities.put(entity,ei);
        }
        return entity;
    }
    
    public void flush() {
        for (EntityFinder ef:cache.keySet());
    }
    
    public void setFlushMode(FlushModeType flushMode) {
    }
    
    public FlushModeType getFlushMode() {
        return FlushModeType.AUTO;
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
        return new Query(this,"",true,qlString,Object.class);
    }
    
    public Query createNamedQuery(String name) {
        return new Query(this,name,false,"",Object.class);
    }
    
    public Query createNativeQuery(String sqlString) {
        return new Query(this,"",false,sqlString,Object.class);
    }
    
    public Query createNativeQuery(String sqlString, Class resultClass) {
        return new Query(this,"",false,sqlString,resultClass);
    }
    
    public Query createNativeQuery(String sqlString, String resultSetMapping) {
        return null;
    }
    
    public void joinTransaction() {
        transaction = new EntityTransaction(this);
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
