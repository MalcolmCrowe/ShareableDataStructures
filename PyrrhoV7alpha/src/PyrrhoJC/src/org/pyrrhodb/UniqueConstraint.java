/*
 * UniqueConstraint.java
 *
 * Created on 29 December 2006, 16:57
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
@Retention(RetentionPolicy.RUNTIME)
public @interface UniqueConstraint {
    public String[] columnNames();
}
