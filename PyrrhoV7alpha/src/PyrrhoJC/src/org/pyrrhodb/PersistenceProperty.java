/*
 * PersistenceProperty.java
 *
 * Created on 25 November 2006, 17:14
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

/**
 *
 * @author Malcolm
 */
public @interface PersistenceProperty {
    public String name();
    public String value();
}
