/*
 * JoinColumn.java
 *
 * Created on 29 December 2006, 17:48
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
public @interface JoinColumn {
    public String name() default "";
    public String referencedColumnName() default "";
    public boolean unique() default false;
    public boolean nullable() default true;
    public boolean insertable() default true;
    public boolean updatable() default true;
    public String columnDefinition() default "";
    public String table() default "";
}
