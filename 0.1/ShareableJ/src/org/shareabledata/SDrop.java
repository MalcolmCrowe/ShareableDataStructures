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
public class SDrop extends SDbObject {
        public final long drpos;
        public final long parent;
        public final String detail;
        public SDrop(STransaction tr,long d,long p,String s)
        {
            super(Types.SDrop,tr);
            drpos = d; parent = p;
            detail = s;
        }
        public SDrop(long d,long p,String s)
        {
            super(Types.SDrop);
            drpos = d; parent = p;
            detail = s;
        }
        SDrop(ReaderBase f) throws Exception
        {
            super(Types.SDrop,f);
            drpos = f.GetLong();
            parent = f.GetLong();
            detail = f.GetString();
        }
        public SDrop(SDrop d,Writer f) throws Exception
        {
            super(d,f);
            drpos = f.Fix(d.drpos);
            parent = f.Fix(d.parent);
            detail = d.detail;
            f.PutLong(drpos);
            f.PutLong(parent);
            f.PutString(detail);
        }
        public static SDrop Get(ReaderBase f) throws Exception
        {
            return new SDrop(f);
        }
        @Override
        public void Put(WriterBase f) throws Exception
        {
            super.Put(f);
            f.PutLong(drpos);
            f.PutLong(parent);
            f.PutString(detail);
        }
        @Override
        public Serialisable Prepare(STransaction tr, SDict<Long, Long> pt)
                throws Exception
        {
            var pr = parent;
            var dp = drpos;
            var st = detail;
            var tn = tr.role.uids.get((pr == -1) ? dp : pr);
            var tb = (STable)tr.objects.get(tr.role.globalNames.get(tn));
            if (tb==null)
                throw new Exception("Table " + tn + " not found");
            if (pr == -1)
            {
                dp = tb.uid;
                for (var b = tr.objects.First(); b != null; b = b.Next())
                    if (b.getValue().val instanceof SIndex)
                    {
                        var x = (SIndex)b.getValue().val;
                        if (x.references == tb.uid)
                            throw new Exception("Restricted by reference");
                    }
            }
            else
            {
                pr = tb.uid;
                var ss = tr.role.subs.get(tb.uid);
                var cn = tr.role.uids.get(dp);
                if (!ss.defs.defines(cn))
                    throw new Exception("Column " + cn + " not found");
                dp = ss.obs.get(ss.defs.get(cn)).key;
                if (st.length() != 0) 
                {
                    var sc = tr.objects.get(dp);
                    if (sc instanceof SColumn && !((SColumn)sc).constraints.defines(st))
                    throw new Exception("Column " + cn + " lacks constraint " + st);
                }
                if (tb.indexes!=null)
                for (var b = tb.indexes.First(); b != null; b = b.Next())
                {
                    var x = (SIndex)tr.objects.get(b.getValue().key);
                    for (var c = x.cols.First(); c != null; c = c.Next())
                        if (c.getValue() == dp)
                            throw new Exception("Restrict: column " + cn + " is an index key");
                }
            } 
            return new SDrop(tr, dp, pr, st);
        }
        @Override
        public STransaction Obey(STransaction tr, Context cx)
                throws Exception
        {
            return (STransaction)tr.Install(this, tr.curpos);
        }

        public boolean Conflicts(Serialisable that)
        {
            switch(that.type)
            {
                case Types.SDrop:
                    {
                        var d = (SDrop)that;
                        return (d.drpos == drpos && d.parent==parent) || d.drpos==parent || d.parent==drpos;
                    }
                case Types.SColumn:
                    {
                        var c = (SColumn)that;
                        return c.table == drpos || c.uid == drpos;
                    }
                case Types.SAlter:
                    {
                        var a = (SAlter)that;
                        return a.defpos == drpos || a.col == drpos;
                    }
            }
            return false;
        }
        @Override
        public String toString()
        {
            var sb = new StringBuilder();
            sb.append("Drop ");
            sb.append("" + drpos);
            sb.append((parent!=0)?"":(" of "+parent));
            sb.append(" ");
            sb.append(detail);
            return sb.toString();
        }
        @Override
        public long getAffects() { return drpos; }
}
