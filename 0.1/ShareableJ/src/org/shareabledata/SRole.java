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
        public final SDict<Long, SDict<Long, String>> props;
        public final SDict<Long, SDict<String, Long>> defs;
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
            props = null;
            defs = null;
            globalNames = null;
        }
        public SRole(SRole sr,SDict<Long,String>u)
        {
            super(Types.SRole,sr.uid);
            name = sr.name;
            uids = u;
            props = sr.props;
            defs = sr.defs;
            globalNames = sr.globalNames;
        }
        SRole(SRole sr,long u,String n)
        {
            super(Types.SRole,sr.uid);
            name = sr.name;
            uids = (sr.uids==null)?new SDict(u,n):sr.uids.Add(u,n);
            props = sr.props;
            defs = sr.defs;
            globalNames = sr.globalNames;
        }
        SRole(SRole sr,String n,long u)
        {
            super(Types.SRole,sr.uid);
            name = sr.name;
            uids = (sr.uids==null)?new SDict(u,n):sr.uids.Add(u,n);
            props = sr.props;
            defs = sr.defs;
            globalNames = (sr.globalNames==null)?new SDict(n,u):
                    sr.globalNames.Add(n,u);
        }
        SRole(SRole sr, String s)
        {
            super(Types.SRole,sr.uid);
            name = sr.name;
            uids = sr.uids;
            props = sr.props;
            defs = sr.defs;
            globalNames = sr.globalNames.Remove(s);
        }
        SRole(SRole sr,long t,long c,String n)
        {
            super(Types.SRole,sr.uid);
            name = sr.name;
            uids = sr.uids;
            var p = sr.props;
            var d = sr.defs;
            if (p==null || !p.Contains(t))
            {
                props = (p==null)?new SDict(t,new SDict(c,n)):
                        p.Add(t,new SDict(c,n));
                defs = (d==null)?new SDict(t,new SDict(n,c)):
                        d.Add(t,new SDict(n,c));
            } else
            {
                props = p.Add(t, p.get(t).Add(c,n));
                defs = d.Add(t, d.get(t).Add(n, c));
            }
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
        public SRole Add(long t,long c,String n)
        {
            return new SRole(this, t,c,n);
        }
        public SRole Remove(String s)
        {
            return new SRole(this, s);
        }
        public boolean defines(long s)
        {
            return uids.Contains(s);
        }
    }    
