/*
 * EntityTransaction.java
 *
 * Created on 25 November 2006, 14:07
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

/**
 *
 * @author Malcolm
 */
public class EntityTransaction {
    private EntityManager em;
    private enum Status { NEW, BEGUN, COMPLETE };
    private Status status = Status.NEW;
    private boolean rollbackOnly = false;
    
    /** Creates a new instance of EntityTransaction */
    EntityTransaction(EntityManager e) {
        em = e;
    }
    public void begin() 
    {
        status = Status.BEGUN;
    }
    public void commit()
    {
        em.flush();
        status = Status.COMPLETE;
    }
    public void rollback()
    {
         em.clear();
         status = Status.COMPLETE;
    }
    public boolean getRollbackOnly()
    {
        return rollbackOnly;
    }
    public void setRollbackOnly()
    {
        rollbackOnly = true;
    }
    public boolean isActive()
    { 
        return status==Status.BEGUN; 
    }
}
