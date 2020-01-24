/*
 * Table.java
 *
 * Created on 29 December 2006, 16:54
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
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Table {
    public String name() default "";
    public String catalog() default "";
    public String schema() default "";
    public UniqueConstraint[] uniqueConstraints() default {};
}
