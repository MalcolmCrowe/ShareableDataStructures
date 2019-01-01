using System;
using System.Text;
#nullable enable
namespace Shareable
{
    public class SQuery : SDbObject
    {
        public readonly SDict<int,Serialisable> cpos;
        public readonly SDict<string, Serialisable> names;
        public SQuery(Types t, long u) : base(t, u)
        {
            cpos = SDict<int,Serialisable>.Empty;
            names = SDict<string, Serialisable>.Empty;
        }
        public SQuery(Types t, STransaction tr) : base(t, tr)
        {
            cpos = SDict<int,Serialisable>.Empty;
            names = SDict<string, Serialisable>.Empty;
        }
        public SQuery(SQuery q) : base(q)
        {
            cpos = q.cpos;
            names = q.names;
        }
        public SQuery(Types t,SQuery q) : base(t)
        {
            cpos = q.cpos;
            names = q.names;
        }
        public SQuery(SQuery q,SDict<int,Serialisable>cp,SDict<string,Serialisable>cn) :base(q)
        {
            cpos = cp;
            names = cn;
        }
        /// <summary>
        /// a and c must have same length: c might have complex values
        /// </summary>
        /// <param name="t"></param>
        /// <param name="a">aliases</param>
        /// <param name="c">column expressions</param>
        /// <param name="source"></param>
        public SQuery(Types t,SDict<int,string> a,SDict<int,Serialisable>c,
            SDict<string,Serialisable> source) : base(t)
        {
            var cp = SDict<int, Serialisable>.Empty;
            var cn = SDict<string, Serialisable>.Empty;
            var ab = a.First();
            for (var cb = c.First();ab!=null && cb!=null;ab=ab.Next(),cb=cb.Next())
            {
                var s = cb.Value.val.Lookup(source)??cb.Value.val;
                cp = cp.Add(cb.Value.key, s);
                cn = cn.Add(ab.Value.val, s);
            }
            cpos = cp;
            names = cn;
        }
        protected SQuery(Types t, Reader f) : base(t, f)
        {
            cpos = SDict<int,Serialisable>.Empty;
            names = SDict<string, Serialisable>.Empty;
        }
        protected SQuery(SQuery q, AStream f) : base(q, f)
        {
            cpos = q.cpos;
            names = q.names;
        }
        /// <summary>
        /// Construct the Rowset for the given SDatabase (may have changed since SQuery was built)
        /// </summary>
        /// <param name="db">The current state of the database or transaction</param>
        /// <returns></returns>
        public virtual RowSet RowSet(SDatabase db)
        {
            throw new NotImplementedException();
        }
        public new virtual SRow Eval(RowBookmark rb)
        {
            throw new NotImplementedException();
        }
        public override string ToString()
        {
            return "SQuery";
        }
    }
    public class SSearch : SQuery
    {
        public readonly SQuery sce;
        public readonly Serialisable alias;
        public readonly SList<Serialisable> where;
        public SSearch(SDatabase db, Reader f):base(Types.SSearch,f)
        {
            sce = f._Get(db) as SQuery ?? throw new Exception("Query expected");
            alias = f._Get(db);
            var w = SList<Serialisable>.Empty;
            var n = f.GetInt();
            for (var i=0;i<n;i++)
                w = w.InsertAt(f._Get(db),i);
            where = w;
        }
        public SSearch(SQuery s,Serialisable a, SList<Serialisable> w)
            :base(Types.SSearch, -1)
        {
            sce = s;
            alias = a;
            where = w;
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            sce.Put(f);
            alias.Put(f);
            f.PutInt(where.Length);
            for (var b=where.First();b!=null;b=b.Next())
                b.Value.Put(f);
        }
        public static SSearch Get(SDatabase d,Reader f)
        {
            return new SSearch(d,f);
        }
        public override RowSet RowSet(SDatabase db)
        {
            return new SearchRowSet(db, this);
        }
        public override SRow Eval(RowBookmark rb)
        {
            var srb = (SearchRowSet.SearchRowBookmark)rb;
            return sce.Eval(srb._bmk);
        }
        public override void Append(SDatabase? db,StringBuilder sb)
        {
            sce.Append(db,sb);
            sb.Append(" where ");
            var cm = "";
            for (var b=where.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = " and ";
                b.Value.Append(db,sb); 
            }
        }
    }
    public class SSelectStatement : SQuery
    {
        public readonly bool distinct,aggregates;
        public readonly SDict<int, string> als;
        public readonly SList<SOrder> order;
        public readonly SQuery qry;
        /// <summary>
        /// The select statement has a source query, 
        /// complex expressions and aliases for its columns,
        /// and an ordering
        /// </summary>
        /// <param name="d">Whrther distinct has been specified</param>
        /// <param name="a">The aliases</param>
        /// <param name="c">The column expressions</param>
        /// <param name="q">The source query, assumed analysed</param>
        /// <param name="or">The ordering</param>
        public SSelectStatement(bool d, SDict<int,string> a, SDict<int,Serialisable> c, SQuery q, SList<SOrder> or) 
            : base(Types.SSelect,a,c,q.names)
        {
            distinct = d;  als = a; qry = q; order = or;
            var ag = false;
            for (var b = cpos.First(); b != null; b = b.Next())
                if (b.Value.val.type == Types.SFunction)
                    ag = true;
            aggregates = ag;
        }
        public static SSelectStatement Get(SDatabase db,Reader f)
        {
            f.GetInt(); // uid for the SSelectStatement probably -1
            var d = f.ReadByte() == 1;
            var n = f.GetInt();
            var a = SDict<int, string>.Empty;
            var c = SDict<int,Serialisable>.Empty;
            for (var i = 0; i < n; i++)
            {
                a = a.Add(i, f.GetString());
                c = c.Add(i,f._Get(db));
            }
            var q = (SQuery)f._Get(db);
            var o = SList<SOrder>.Empty;
            n = f.GetInt();
            for (var i = 0; i < n; i++)
                o = o.InsertAt((SOrder)f._Get(db), i);
            return new SSelectStatement(d,a,c,q,o);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.WriteByte((byte)(distinct ? 1 : 0));
            f.PutInt(als.Length);
            var ab = als.First();
            for (var b = cpos.First(); ab!=null && b != null; b = b.Next(), ab=ab.Next())
            {
                f.PutString(ab.Value.val);
                b.Value.val.Put(f);
            }
            qry.Put(f);
            f.PutInt(order.Length.Value);
            for (var b=order.First();b!=null;b=b.Next())
                b.Value.Put(f);
        }
        public override void Append(SDatabase? db,StringBuilder sb)
        {
            if (distinct)
                sb.Append("distinct ");
            base.Append(db,sb);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Select ");
            var cm = "";
            var ab = als.First();
            for (var b = cpos.First(); ab!=null && b != null; b = b.Next(),ab=ab.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.Value.val);
                if (b.Value.val is SSelector sc && ab.Value.val.CompareTo(sc.name) == 0)
                    continue;
                sb.Append(" as "); sb.Append(ab.Value.val);
            }
            sb.Append(' ');
            return sb.ToString();
        }

        public override RowSet RowSet(SDatabase db)
        {
            RowSet r = new SelectRowSet(db,this);
            if (distinct)
                r = new DistinctRowSet(r);
            return r;
        }
        public override SRow Eval(RowBookmark rb)
        {
            if (als.Length.Value > 0)
                return new SRow(this, rb);
            return qry.Eval(rb);
        }
    }
    public class SOrder : Serialisable
    {
        public readonly Serialisable corr;
        public readonly SColumn col;
        public readonly bool desc;
        public SOrder(Serialisable cr,SColumn co,bool d) :base(Types.SOrder)
        {
            corr = cr; col = co; desc = d;
        }
        protected SOrder(SDatabase db,Reader f) :base(Types.SOrder)
        {
            corr = f._Get(db);
            col = (SColumn)f._Get(db);
            desc = f.ReadByte() == 1;
        }
        public static SOrder Get(SDatabase db,Reader f)
        {
            return new SOrder(db, f);
        }
        public override void Put(StreamBase f)
        {
            corr.Put(f);
            col.Put(f);
            f.WriteByte((byte)(desc ? 1 : 0));
        }
    }
}
