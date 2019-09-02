/*
 * SecondaryTable.java
 *
 * Created on 29 December 2006, 17:01
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
public @interface SecondaryTable {
    public String name();
    public String catalog() default "";
    public String schema() default "";
    public PrimaryKeyJoinColumn[] pkJoinColumns() default {};
    public UniqueConstraint[] uniqueConstraints() default {};
}
