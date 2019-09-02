/*
 * EntityNotFoundException.java
 *
 * Created on 04 January 2007, 20:27
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

/**
 *
 * @author Malcolm
 */
public class EntityNotFoundException extends java.lang.Exception {
    
    /**
     * Creates a new instance of <code>EntityNotFoundException</code> without detail message.
     */
    public EntityNotFoundException() {
    }
    
    
    /**
     * Constructs an instance of <code>EntityNotFoundException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public EntityNotFoundException(String msg) {
        super(msg);
    }
}
