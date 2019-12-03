/*
 * EntityExistsException.java
 *
 * Created on 25 November 2006, 21:13
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

/**
 *
 * @author Malcolm
 */
public class EntityExistsException extends java.lang.Exception {
    
    /**
     * Creates a new instance of <code>EntityExistsException</code> without detail message.
     */
    public EntityExistsException() {
    }
    
    
    /**
     * Constructs an instance of <code>EntityExistsException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public EntityExistsException(String msg) {
        super(msg);
    }
}
