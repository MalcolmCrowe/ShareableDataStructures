/*
 * PrimaryKeyJoinColumns.java
 *
 * Created on 29 December 2006, 16:48
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;
import java.lang.annotation.*;
/**
 *
 * @author Malcolm
 */
@Target({ElementType.TYPE,ElementType.METHOD,ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface PrimaryKeyJoinColumns {
    public PrimaryKeyJoinColumn[] value();
}