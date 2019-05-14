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
public class SRObject {
    public final SDict<Long,Integer> props;
    public final SDict<String,Integer> defs;
    public final SDict<Integer,SSlot<Long,String>> obs;
    public static final SRObject Empty =
            new SRObject(null,null,null);
    public SRObject(SDict<Long,Integer> p,SDict<String,Integer> d,
            SDict<Integer,SSlot<Long,String>> o)
    {
        props = p; defs = d; obs = o;
    }
    public SRObject Remove(int k)
    {
        if (k<0 || obs==null || k>=obs.Length)
            return this;
        SDict<Long,Integer> pr = null;
        SDict<String,Integer> df = null;
        SDict<Integer,SSlot<Long,String>> os = null;
        var m = 0;
        if (obs!=null)
        for (var b=obs.First();b!=null;b=b.Next())
        {
            var i = b.getValue().key;
            var p = b.getValue().val.key;
            var n = b.getValue().val.val;
            if (i==k)
                continue;
            pr = (pr==null)?new SDict(p,m):pr.Add(p,m);
            df = (df==null)?new SDict(n,m):df.Add(n,m);
            os = (os==null)?new SDict(m,new SSlot(p,n)):os.Add(m,new SSlot(p,n));
            m++;
        }
        return new SRObject(pr,df,os);
    }
    public SRObject Add(int i,long p,String n)
    {
        var k = (props!=null && props.defines(p))?props.get(p):-1;
        if (i<0)
            i = (k<0)?((props!=null)?props.Length:0):k;
        SDict<Long,Integer> pr = null;
        SDict<String,Integer> df = null;
        SDict<Integer,SSlot<Long,String>> os = null;
        var m = 0;
        if (obs!=null)
        for (var b=obs.First();b!=null;b=b.Next())
        {    
            var ii = b.getValue().key;
            var pp = b.getValue().val.key;
            var nn = b.getValue().val.val; 
            if (ii==k)
                continue;
            pr = (pr==null)?new SDict(pp,m):pr.Add(pp,m);
            df = (df==null)?new SDict(nn,m):df.Add(nn,m);
            os = (os==null)?new SDict(m,new SSlot(pp,nn))
                    :os.Add(m,new SSlot(pp,nn));
            m++;
        }
        pr = (pr==null)?new SDict(p,i):pr.Add(p,i);
        df = (df==null)?new SDict(n,i):df.Add(n,i);
        os = (os==null)?new SDict(i,new SSlot(p,n)):os.Add(i, new SSlot(p,n));
        return new SRObject(pr,df,os);
    }
}
