/*
 * PersistenceUnit.java
 *
 * Created on 26 December 2006, 13:49
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
public @interface PersistenceUnit {
    public String name() default "";
    public String unitName() default "";
    public PersistenceContextType type() default PersistenceContextType.TRANSACTION;
    public PersistenceProperty[] properties() default {};
}
