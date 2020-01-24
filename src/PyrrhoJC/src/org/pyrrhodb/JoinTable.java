/*
 * JoinTable.java
 *
 * Created on 29 December 2006, 17:55
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
@Target({ElementType.METHOD,ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface JoinTable {
    public String name() default "";
    public String catalog() default "";
    public String schema() default "";
    public UniqueConstraint[] uniqueConstraints() default {};
    public JoinColumn[] inverseJoinColumns() default{};
    public JoinColumn[] joinColumns() default {};
}
