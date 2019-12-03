/*
 * ManyToMany.java
 *
 * Created on 29 December 2006, 17:36
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
public @interface ManyToMany {
    public CascadeType[] cascade() default{};
    public Class targetEntity() default void.class;
    public FetchType fetch() default FetchType.EAGER;
    public String mappedBy() default "";
    public boolean optional() default true;
}
