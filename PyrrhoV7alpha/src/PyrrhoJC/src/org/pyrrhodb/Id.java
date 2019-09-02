/*
 * Id.java
 *
 * Created on 25 November 2006, 13:51
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
@Target({ElementType.FIELD,ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Id {
}
