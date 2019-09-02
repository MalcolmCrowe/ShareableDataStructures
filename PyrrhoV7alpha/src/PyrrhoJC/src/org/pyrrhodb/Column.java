/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pyrrhodb;

/**
 *
 * @author Malcolm
 */
public class Column {
    Column(Connection c,String n,String d,int f)
    {
        name = n;
        dataTypeName = d;
        dataType = (short)(f &0xf);
        notNull = (f&0x100)!=0;
        generated = (f&0x200)!=0;
        keyPos = ((f>>4)&0xf)-1;
        if (!c.dataTypes.containsKey(d))
            c.dataTypes.put(n,new DataType(d,dataType,!notNull));
    }
    public String name;
    public String dataTypeName;
    int displaySize;
    boolean notNull;
    boolean generated;
    short dataType;
    int keyPos;
}
