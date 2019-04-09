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
public class SColumn extends SSelector {
        public final int dataType;
        public final long table;
        public final SDict<String,SFunction> constraints;
        /// <summary>
        /// For system column
        /// </summary>
        /// <param name="n"></param>
        /// <param name="t"></param>
        /// <param name="u"> will be negative</param>
        public SColumn(long u,long tbl,int t)
        {
            super(Types.SColumn,u);
            dataType = t; table = tbl;
            constraints = null;
        }
        public SColumn(STransaction tr,long tbl,int t,SDict<String,SFunction>cs)
        {
            super(tr,Types.SColumn);
            dataType = t; table = tbl;
            constraints = cs;
        }
        public SColumn(long tbl,int t,long u,SDict<String,SFunction>cs)
        {
            super(Types.SColumn,u);
            dataType = t; table = tbl;
            constraints = cs;
        }
        public SColumn(SColumn c,int d,SDict<String,SFunction>cs)
        {
            super(c);
            dataType = d; table = c.table;
            constraints = cs;
        }
        SColumn(Reader f) throws Exception
        {
            super(Types.SColumn,f);
            var db = f.db;
            var ro = db.role;
            var cn = (f instanceof SocketReader)?ro.uids.get(uid):f.GetString(); 
            var oc = f.context;
            f.context = this; 
            dataType = f.ReadByte();
            var ut = f.GetLong();
            var tn = ro.uids.get(ut);
            table = ro.globalNames.get(tn);
            var n = f.GetInt();
            SDict<String,SFunction> c = null;
            for (var i = 0; i < n; i++)
            {
                cn = f.GetString();
                var vf = f._Get();
                if (vf==null || vf.type!=Types.SFunction)
                    throw new Exception("Constraint expected");
                var fn = (SFunction)vf;
                c = (c==null)?new SDict(cn,fn):c.Add(cn,fn);
            }
            constraints = c;
            f.context = oc;
            if (!(f instanceof SocketReader) && 
                    !f.db.role.uids.Contains(uid))
                f.db = f.db.Install(this, cn, f.getPosition());
        }
        public SColumn(SColumn c,String nm,AStream f) 
        {
            super(c,f);
            f.PutString(nm);
            dataType = c.dataType;
            table = f.Fix(c.table);
            f.WriteByte((byte)dataType);
            f.PutLong(table);
            f.PutInt((c.constraints==null)?0:c.constraints.Length);
            if (c.constraints!=null)
            for (var b = c.constraints.First(); b != null; b = b.Next())
            {
                f.PutString(b.getValue().key);
                b.getValue().val.Fix(f).Put(f);
            }
            constraints = c.constraints;
        }
        public static SColumn Get(Reader f) throws Exception
        {
            return new SColumn(f);
        }
        public void PutColDef(StreamBase f)
        {
            super.Put(f);
            f.WriteByte((byte)dataType);
            f.PutLong(table);
            f.PutInt((constraints==null)?0:constraints.Length);
            if (constraints!=null)
            for (var b = constraints.First(); b != null; b = b.Next())
            {
                f.PutString(b.getValue().key);
                var c = b.getValue().val;
                if (f instanceof AStream)
                    c = (SFunction)c.Fix((AStream)f);
                c.Put(f);
            }
        }
        @Override
        public void Put(StreamBase f)
        {
            f.WriteByte((byte)Types.SName);
            f.PutLong(uid);
        }
        @Override
        public boolean Conflicts(SDatabase db,STransaction tr,Serialisable that)
        {
            switch (that.type)
            {
                case Types.SColumn:
                    {
                        SColumn c = (SColumn)that;
                        return c.table == table && 
                                db.role.uids.defines(c.uid) &&
                                db.role.uids.get(c.uid).compareTo(tr.Name(uid)) == 0;
                    }
                    case Types.SDrop:
                    {
                        var d = (SDrop)that;
                        return d.drpos == table;
                    }
            }
            return false;
        }
        @Override
        public Serialisable UseAliases(SDatabase db,SDict<Long, Long> ta)
        {
            if (ta.Contains(uid))
                return new SExpression(db.objects.get(ta.get(table)), 
                        SExpression.Op.Dot, this);
            return super.UseAliases(db,ta);
        }
        @Override
        public Serialisable Prepare(STransaction tr, SDict<Long,Long> pt) throws Exception
        {
            SDict<String, SFunction> cs = null;
            if (constraints!=null)
            for (var b = constraints.First(); b != null; b = b.Next())
            {
                var k = b.getValue().key;
                var v = (SFunction)b.getValue().val.Prepare(tr, pt);
                cs =(cs==null)?new SDict(k,v):cs.Add(k, v);
            }
            return new SColumn(this, dataType, cs);
        }
        /// <summary>
        /// Fix a Column reference
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        @Override
        public Serialisable Fix(AStream f)
        {
            return (f.uids.Contains(uid)) ? 
                    new SColumn(f.uids.get(uid), f.uids.get(table), dataType) : 
                    this;
        }
        @Override
        public Serialisable Lookup(STransaction tr,Context cx)
        {
            var r = cx.defines(uid) ? cx.get(uid) : Null;
            if (r == Null && !(cx.refs instanceof RowBookmark))
                return this;
            return r;
        }
        public Serialisable Check(STransaction tr,Serialisable v,Context cx)
                throws Exception
        {
            v = v.Coerce(dataType);
            if (constraints!=null)
            for (var b=constraints.First();b!=null;b=b.Next())
                switch (b.getValue().key)
                {
                    case "NOTNULL":
                        if (v.type==Types.Serialisable)
                            throw new Exception("Illegal null value");
                        break;
                    case "GENERATED":
                        if (v.type!=Types.Serialisable)
                            throw new Exception("Illegal value for generated column");
                        return b.getValue().val.arg.Lookup(tr,cx);
                    case "DEFAULT":
                        if (v.type == Types.Serialisable)
                            return b.getValue().val.arg;
                        break;
                    default:
                        cx = Context.New(new SDict(SArg.Value.target.uid, v), cx);
                        if (b.getValue().val.arg.Lookup(tr,cx) != SBoolean.True)
                            throw new Exception("Column constraint " + 
                                    b.getValue().key + " fails");
                        break;
                }
            return v;
        }
        public SDict<Long, Serialisable> Aggregates(SDict<Long, Serialisable> ags)
        {
            if (constraints!=null)
            for (var b = constraints.First(); b != null; b = b.Next())
            {
                var k = b.getValue().val.fid;
                var v = b.getValue().val;
                ags =(ags==null)?new SDict(k,v):ags.Add(k,v);
            }
            return super.Aggregates(ags);
        }
        @Override
        public boolean isValue()
        {
            return false;
        }
        @Override
        public String toString()
        {
            var sb = new StringBuilder(_Uid(table));
            sb.append("[");
            sb.append(Uid());
            sb.append("] ");
            sb.append(Types.types[dataType]);
            if (constraints!=null)
            for (var b = constraints.First(); b != null; b = b.Next())
            {
                sb.append(' ');
                if (b.getValue().val.func == SFunction.Func.Constraint)
                {
                    sb.append(b.getValue().key);
                    sb.append('=');
                }
                b.getValue().val.Append(sb);
            }
            return sb.toString();
        }
}
