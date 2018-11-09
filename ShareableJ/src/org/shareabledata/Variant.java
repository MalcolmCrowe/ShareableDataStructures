/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 *
 * @author Malcolm
 */
public class Variant implements Comparable {
    public final Variants variant;
    public final Object ob;
    public Variant(Variants t,Object v)
    {
        variant = t;
        ob = v;
    }
    public Variant(Object v) throws Exception
    {
        if (v instanceof Variant)
            throw new Exception("Internal error");
        variant = Variants.Single;
        ob = v;
    }
    @Override
    public int compareTo(Object obj)
    {
        return ((Comparable)ob).compareTo(((Variant)obj).ob);
    }
}
