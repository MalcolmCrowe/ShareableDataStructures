/*
 * NoResultException.java
 *
 * Created on 04 January 2007, 20:25
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;
import java.lang.Exception;

/**
 *
 * from Java EE 5 specification
 */
public class NoResultException extends Exception 
{
    /** Creates a new instance of NoResultException */
    public NoResultException() {    }
    
    public NoResultException(String msg) 
    {
        super(msg);
    }
}
