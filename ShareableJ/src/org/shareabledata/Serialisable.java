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
    
    protected Serialisable(int t) {
        type = t;
    }

    public Serialisable(int t, Reader f) {
        type = t;
    }

    public static Serialisable Get(Reader f) throws Exception {
        return Null;
    }
    
    public void Put(StreamBase f) throws Exception
    {
        f.WriteByte((byte)type);
    }

    public boolean Conflicts(Serialisable that) {
        return false;
    }
    
    public void Append(StringBuilder sb)
    {
        sb.append(toString());
    }

    public String ToString() {
        return "Null";
    }

    @Override
    public int compareTo(Object o) {
        return (o==Null)?0:-1;
    }
}
