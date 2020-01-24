/*
 * Query.java
 *
 * Created on 23 November 2006, 21:43
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Malcolm
 */
public class Query  {
    EntityManager em;
    String name;
    String ql;
    boolean useJpql;
    Class result;
    /**
     * Creates a new instance of Query
     */
    public Query(EntityManager e,String n,boolean u, String s,Class r) {
        em = e; name = n; useJpql = u; ql = s; result =r;
    }

    public List getResultList() {
        return null;
    }

    public Object getSingleResult() {
        return null;
    }

    public int executeUpdate() {
        return -1;
    }

    public Query setMaxResults(int maxResult) {
        return null;
    }

    public Query setFirstResult(int startPosition) {
        return null;
    }

    public Query setHint(String hintName, Object value) {
        return null;
    }

    public Query setParameter(String name, Object value) {
        return null;
    }

    public Query setParameter(String name, Date value, TemporalType temporalType) {
        return null;
    }

    public Query setParameter(String name, Calendar value, TemporalType temporalType) {
        return null;
    }

    public Query setParameter(int position, Object value) {
        return null;
    }

    public Query setParameter(int position, Date value, TemporalType temporalType) {
        return null;
    }

    public Query setParameter(int position, Calendar value, TemporalType temporalType) {
        return null;
    }

    public Query setFlushMode(FlushModeType flushMode) {
        return null;
    }
    
}
