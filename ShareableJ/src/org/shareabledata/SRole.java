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
public class SRole extends SDbObject implements ILookup<Long,String>
{
    public final String name;
    public final SDict<Long, String> uids;
    public final SDict<Long, SRObject> subs;
    public final SDict<String, Long> globalNames;
    public static final SRole Public = new SRole("PUBLIC", -1);
    public String get(Long u)
    {
        return uids.get(u);
    }
    public boolean defines(Long u)
    {
        return uids.Contains(u);
    }
    public SRole(String n,long u)
    {
        super(Types.SRole,u);
        name = n; uids = null;
        subs = null;
        globalNames = null;
    }
    public SRole(SRole sr,SDict<Long,String>u)
    {
        super(Types.SRole,sr.uid);
        name = sr.name;
        uids = u;
        subs = sr.subs;
        globalNames = sr.globalNames;
    }
    SRole(SRole sr,long u,String n)
    {
        super(Types.SRole,sr.uid);
        name = sr.name;
        uids = (sr.uids==null)?new SDict(u,n):sr.uids.Add(u,n);
        subs = sr.subs;
        globalNames = sr.globalNames;
    }
    SRole(SRole sr,String n,long u)
    {
        super(Types.SRole,sr.uid);
        name = sr.name;
        uids = (sr.uids==null)?new SDict(u,n):sr.uids.Add(u,n);
        subs = sr.subs;
        globalNames = (sr.globalNames==null)?new SDict(n,u):
                sr.globalNames.Add(n,u);
    }
    SRole(SRole sr, String s)
    {
        super(Types.SRole,sr.uid);
        name = sr.name;
        uids = sr.uids;
        subs = sr.subs;
        globalNames = sr.globalNames.Remove(s);
    }
    SRole(SRole sr,long t,int s,long c,String n)
    {
        super(Types.SRole,sr.uid);
        name = sr.name;
        uids = sr.uids;
        var so = (sr.subs!=null && sr.subs.defines(t))?sr.subs.get(t)
                : SRObject.Empty;
        so = so.Add(s, c, n);
        subs = (sr.subs==null)?new SDict(t,so):sr.subs.Add(t, so);
        globalNames = sr.globalNames;
    }
    SRole(SRole sr,long p,int k)
    {
        super(Types.SRole,sr.uid);
        name = sr.name;
        uids = sr.uids;
        var so = sr.subs.defines(p)?sr.subs.get(p):SRObject.Empty;
        so = so.Remove(k);
        subs = (sr.subs==null)?new SDict(p,so):sr.subs.Add(p,so);
        globalNames = sr.globalNames;
    }
    public SRole Add(String n,long u)
    {
        return new SRole(this,n,u);
    }
    public SRole Add(long u, String n)
    {
        return new SRole(this, u, n);
    }
    public SRole Add(long t,int s,long c,String n)
    {
        return new SRole(this, t,s,c,n);
    }
    public SRole Remove(String s)
    {
        return new SRole(this, s);
    }
    public SRole Remove(long p,int sq)
    {
        return new SRole(this,p,sq);
    }
    public boolean defines(long s)
    {
        return uids.Contains(s);
    }
}    
