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
    public Variant(Object v,boolean asc)
    {
        if (v instanceof Variant)
            throw new Error("Internal error");
        variant = asc?Variants.Ascending:Variants.Descending;
        ob = v;
    }
    @Override
    public int compareTo(Object obj)
    {
        var c = ((Comparable)ob).compareTo(((Variant)obj).ob);
        return (variant == Variants.Descending) ? -c : c;
    }
}
