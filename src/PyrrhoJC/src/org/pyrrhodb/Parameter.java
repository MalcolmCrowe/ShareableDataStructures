/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pyrrhodb;

/**
 *
 * @author 66668214
 */
public class Parameter {
    public String name;
    public DataType dataType;
    public short mode;
    public static short IN=0;
    public static short OUT=1;
    public static short INOUT=2;
    public static short RESULT=3;
    Parameter(String n,DataType t,String m)
    {
        name = n; dataType = t;
        if (m.equals("IN"))
            mode = 0;
        else if (m.equals("OUT"))
            mode = 1;
        else if (m.equals("INOUT"))
            mode = 2;
        else if (m.equals("RESULT"))
            mode = 3;
    }
}
