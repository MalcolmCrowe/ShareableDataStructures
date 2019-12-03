/*
 * FieldInfo.java
 *
 * Created on 25 November 2006, 20:33
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

import java.lang.reflect.*;
import java.lang.annotation.*;
/**
 *
 * @author Malcolm
 */
public class FieldInfo {
    String name;
    Method getMeth,setMeth;
    Field field;
    Class type;
    public FieldInfo(Method gm,Method sm) {
        getMeth = gm; setMeth = sm; field = null;
        name = gm.getName();
        type = gm.getReturnType();
        int ix = name.lastIndexOf('.');
        if (ix>=0)
            name = name.substring(ix+1);
        if (name.startsWith("get"))
            name = name.substring(3,1).toLowerCase()+name.substring(4);
    }
     public FieldInfo(Field f) {
        getMeth = null; setMeth = null; field = f;
        name = f.getName();
        type = f.getType();
        int ix = name.lastIndexOf('.');
        if (ix>=0)
            name = name.substring(ix+1);
    }
     public Object get(Object en) throws IllegalAccessException, InvocationTargetException
     {
         if (field!=null)
             return field.get(en);
         return getMeth.invoke(en);
     }
     public void set(Object en,Object val) throws IllegalAccessException, InvocationTargetException
     {
         if (field!=null)
             field.set(en,val);
         else
             setMeth.invoke(en,val);
     }
}
