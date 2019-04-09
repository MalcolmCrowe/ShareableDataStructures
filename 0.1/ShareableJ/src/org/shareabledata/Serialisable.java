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
    public boolean isValue() { return true;}
    public static Serialisable Get(Reader f) throws Exception
    {
        return Null;
    }
    /// Prepare is used by the server to make a transformed version of the Serialisable that
    /// replaces all client-side uids with server-side uids apart from aliases.
    /// This speeds things up in stored procs etc.
    /// With single-statement execution it is called before Obey.   
    public Serialisable Prepare(STransaction db,SDict<Long,Long> pt) throws Exception
    {
        return this;
    }
    /// The UseAliases machinery is used by the server when a viewdefinition is the target of a query
    public Serialisable UseAliases(SDatabase db,SDict<Long,Long> ta)
    {
        return this;
    }
    /// The UpdateAliases machinery is used by the client at the end of parsing a SelectStatement
    public Serialisable UpdateAliases(SDict<Long,String> uids)
    {
        return this;
    }
    /// Obey is used in the server to make changes to the transaction
    public STransaction Obey(STransaction tr,Context cx) throws Exception
    {
        return tr;
    }
    /// Aggregates is used by the server in query processing
    public SDict<Long,Serialisable> Aggregates(SDict<Long,Serialisable> ags)
    {
        return ags;
    }
    /// Put is used in serialisation by client and server
    public void Put(StreamBase f) 
    {
        f.WriteByte((byte)type);
    }
    /// Conflicts is used by the server in commit validation 
    public boolean Conflicts(SDatabase db,STransaction tr,Serialisable that) {
        return false;
    }
    /// Append is used by the server when preparing results to send to the client
    public void Append(SDatabase db,StringBuilder sb) 
    {
        Append(sb);
    }
    /// Append is used to create readable versions of database objects    
    public void Append(StringBuilder sb) 
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
    //        case Types.STimestamp: return "timestamp";
        }
        return "Unknown data type";
    }
    public Serialisable Coerce(int t) throws Exception
    {
        if (type==t)
            return this;
        switch(t)
        {
                   case Types.SInteger:
                    switch(type)
                    {
                        case Types.SBigInt:
                            return this;
                        default: throw new Exception("Expected integer got " 
                                + Types.types[type]);
                    }
                case Types.SNumeric:
                    switch (type)
                    {
                        case Types.SInteger:
                            var n = (SInteger)this;
                            Bigint b;
                            if (n.big !=null)
                                b = n.big;
                            else
                                b = new Bigint(n.value);
                            return new SNumeric(new Numeric(b, 0, 12));
                        default:
                            throw new Exception("Expected numeric got " 
                                 + Types.types[type]);
                    }
            }
            throw new Exception("Expected " + Types.types[t] + " got " 
                        + Types.types[type]);
    }
    @Override
    public int compareTo(Object o) {
        return (o==Null)?0:-1;
    }
    public Serialisable Lookup(STransaction tr,Context cx)
    {
        return this;
    }
    public Serialisable Fix(AStream f)
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
}
