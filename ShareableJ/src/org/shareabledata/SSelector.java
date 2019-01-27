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
    public abstract class SSelector extends SDbObject
    {
        public final String name;
        public SSelector(int t, String n, long u)
        {
            super(t,u);
            name = n;
        }
        public SSelector(int t, String n, STransaction tr)
        {
            super(t, tr);
            name = n;
        }
        public SSelector(SSelector s, String n)
        {
            super(s);
            name = n;
        }
        protected SSelector(int t, Reader f)
        {
            super(t,f);
            name = f.GetString();
        }
        protected SSelector(SSelector s,AStream f) 
        {
            super(s,f);
            name = s.name;
            f.PutString(name);
        }
        @Override
        public void Put(StreamBase f)
        {
            super.Put(f);
            f.PutString(name);
        }
        @Override
        public Serialisable Lookup(ILookup<String,Serialisable>nms)
        {
            return (nms!=null && nms.defines(name))?nms.get(name):this;
        }
        @Override
        public String Alias(int n)
        {
            return name;
        }
    }
