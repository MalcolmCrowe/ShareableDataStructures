/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

import java.io.*;

/**
 *
 * @author Malcolm
 */

public class Serialisable implements Comparable {

    public final int type;
    public final static Serialisable Null = new Serialisable(0);
    public final static Serialisable Exception = new Serialisable(Types.Exception);
    protected Serialisable(int t) {
        type = t;
    }

    public Serialisable(int t, Reader f) {
        type = t;
    }
    public String Alias(int n)
    {
        return "col" + n;
    }
    public boolean isValue() { return true;}
    public static Serialisable Get(Reader f) {
        return Null;
    }
    
    public void Put(StreamBase f) 
    {
        f.WriteByte((byte)type);
    }

    public boolean Conflicts(Serialisable that) {
        return false;
    }
    
    public void Append(SDatabase db,StringBuilder sb) 
    {
        sb.append(toString());
    }

    public String toString() {
        return "null";
    }
    public static String DataTypeName(int t)
    {
        switch(t)
        {
            case Types.SInteger: return "integer";
            case Types.SString: return "string";
            case Types.SNumeric: return "numeric";
            case Types.SBoolean: return "boolean";
            case Types.SDate: return "date";
            case Types.STimeSpan: return "timespan";
            case Types.STimestamp: return "timestamp";
        }
        return "Unknown";
    }
    @Override
    public int compareTo(Object o) {
        return (o==Null)?0:-1;
    }
    public Serialisable Lookup(Context cx)
    {
        return this;
    }
    public boolean Check(SDict<Long,Boolean> rdC)
    {
        return false;
    }
    public Serialisable StartCounter(Serialisable v)
    {
        return v;
    }
    public Serialisable AddIn(Serialisable a,Serialisable v)
    {
        return a;
    }
    public SDict<Long,SFunction> Aggregates(SDict<Long,SFunction> a,Context cx)
    {
        return a;
    }
}
