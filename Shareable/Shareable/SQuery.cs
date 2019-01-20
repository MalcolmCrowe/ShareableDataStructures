using System;
using System.Text;
#nullable enable
namespace Collection
{
    public class SQuery : SDbObject
    {
        public readonly SDict<int, string> display;
        public readonly SDict<int,Serialisable> cpos;
        public readonly SDict<string, Serialisable> names;
        public SQuery(Types t, long u) : base(t, u)
        {
            display = SDict<int, string>.Empty;
            cpos = SDict<int,Serialisable>.Empty;
            names = SDict<string, Serialisable>.Empty;
        }
        public SQuery(Types t, STransaction tr) : base(t, tr)
        {
            display = SDict<int, string>.Empty;
            cpos = SDict<int,Serialisable>.Empty;
            names = SDict<string, Serialisable>.Empty;
        }
        public SQuery(SQuery q) : base(q)
        {
            display = q.display;
            cpos = q.cpos;
            names = q.names;
        }
        public SQuery(Types t,SQuery q) : base(t)
        {
            display = q.display;
            cpos = q.cpos;
            names = q.names;
        }
        public SQuery(SQuery q,SDict<int,string> a,SDict<int,Serialisable>cp,
            SDict<string,Serialisable>cn) :base(q)
        {
            display = a;
            cpos = cp;
            names = cn;
        }
        /// <summary>
        /// a and c must have same length: c might have complex values
        /// </summary>
        /// <param name="t"></param>
        /// <param name="a">aliases</param>
        /// <param name="c">column expressions</param>
        /// <param name="source">symbol table for Lookup</param>
        public SQuery(Types t,SDict<int,string> a,SDict<int,Serialisable>c,
            SDict<string,Serialisable> source) : base(t)
        {
            var cp = SDict<int, Serialisable>.Empty;
            var cn = SDict<string, Serialisable>.Empty;
            var ab = a.First();
            for (var cb = c.First();ab!=null && cb!=null;ab=ab.Next(),cb=cb.Next())
            {
                var s = cb.Value.val;
                if (source.Length!=0)
                    s = cb.Value.val.Lookup(source);
                cp = cp+(cb.Value.key, s);
                cn = cn+(ab.Value.val, s);
            }
            display = a;
            cpos = cp;
            names = cn;
        }
        protected SQuery(Types t, Reader f) : base(t, f)
        {
            display = SDict<int, string>.Empty;
            cpos = SDict<int,Serialisable>.Empty;
            names = SDict<string, Serialisable>.Empty;
        }
        /// <summary>
        /// This constructor is only called when committing am STable.
        /// Ignore the columns defined in the transaction.
        /// </summary>
        /// <param name="q">The current state of the STable in the transaction</param>
        /// <param name="f"></param>
        protected SQuery(SQuery q, AStream f) : base(q, f)
        {
            display = SDict<int, string>.Empty;
            cpos = SDict<int, Serialisable>.Empty;
            names = SDict<string, Serialisable>.Empty;
        }
        public virtual Serialisable Lookup(string a)
        {
            return names.Lookup(a) ?? Null;
        }
        /// <summary>
        /// Construct the Rowset for the given SDatabase (may have changed since SQuery was built)
        /// </summary>
        /// <param name="db">The current state of the database or transaction</param>
        /// <returns></returns>
        public virtual RowSet RowSet(STransaction tr,ILookup<string,Serialisable>nms)
        {
            throw new NotImplementedException();
        }
        public new virtual string Alias => "";
        public virtual SDict<int, string> Display => display;
        public override string ToString()
        {
            return "SQuery";
        }
    }
    public class SJoin : SQuery
    {
        public enum JoinType { None=0, Natural=1, Cross=2, Left=3, Right=4, Full=5 };
        public readonly JoinType joinType;
        public readonly bool outer;
        public readonly SQuery left,right;
        public readonly SList<SExpression> ons;
        public SJoin(SDatabase db,Reader f): base(Types.STableExp,f)
        {
            left = f._Get(db) as SQuery ?? throw new Exception("Query expected");
            outer = f.GetInt() == 1;
            joinType = (JoinType)f.GetInt();
            right = f._Get(db) as SQuery ?? throw new Exception("Query expected");
            var n = f.GetInt();
            var on = SList<SExpression>.Empty;
            for (var i = 0; i < n; i++)
                on = on + (f._Get(db) as SExpression ?? throw new Exception("ON exp expected"), i);
            ons = on;
        }
        public SJoin(SQuery lf,bool ou,JoinType jt,SQuery rg,SList<SExpression> on,
            SDict<int,string> d,SDict<int,Serialisable> c,SDict<string,Serialisable> n) 
            :base(Types.STableExp,d,c,n)
        {
            left = lf; right = rg; outer = ou; joinType = jt; ons = on;
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            left.Put(f);
            f.PutInt(outer ? 1 : 0);
            f.PutInt((int)joinType);
            right.Put(f);
            f.PutInt(ons.Length.Value);
            for (var b = ons.First(); b != null; b = b.Next())
                b.Value.Put(f);
        }
        public static SJoin Get(SDatabase d,Reader f)
        {
            return new SJoin(d, f);
        }
        public override RowSet RowSet(STransaction tr, ILookup<string, Serialisable> nms)
        {
            return new JoinRowSet(tr, this, nms);
        }
        public override void Append(SDatabase db, StringBuilder sb)
        {
            left.Append(db, sb);
            if (outer)
                sb.Append(" outer ");
            if (joinType != JoinType.None)
            { sb.Append(" "); sb.Append(joinType); sb.Append(" "); }
            right.Append(db, sb);
            if (ons.Length.Value>0)
            {
                sb.Append(" on ");
                var cm = "";
                for (var b=ons.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    b.Value.Append(db, sb);
                }
            }
        }
    }
    public class SSearch : SQuery
    {
        public readonly SQuery sce;
        public readonly string alias;
        public readonly SList<Serialisable> where;
        public SSearch(SDatabase db, Reader f):base(Types.SSearch,f)
        {
            sce = f._Get(db) as SQuery ?? throw new Exception("Query expected");
            alias = f.GetString();
            var w = SList<Serialisable>.Empty;
            var n = f.GetInt();
            for (var i=0;i<n;i++)
                w = w+(f._Get(db).Lookup(sce.names),i);
            where = w;
        }
        public SSearch(SQuery s,string a, SList<Serialisable> w)
            :base(Types.SSearch, s.display, s.cpos, s.names)
        {
            sce = s;
            alias = a;
            where = w;
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            sce.Put(f);
            f.PutString(alias);
            f.PutInt(where.Length);
            for (var b=where.First();b!=null;b=b.Next())
                b.Value.Put(f);
        }
        public override Serialisable Lookup(string a)
        {
            if (alias.CompareTo(a) == 0)
                return sce;
            return sce.Lookup(a);
        }
        public static SSearch Get(SDatabase d,Reader f)
        {
            return new SSearch(d,f);
        }
        public override RowSet RowSet(STransaction tr,ILookup<string,Serialisable> nms)
        {
            return new SearchRowSet(tr, this,nms);
        }
        public override Serialisable Lookup(ILookup<string,Serialisable> nms)
        {
            return (nms is SearchRowSet.SearchRowBookmark srb)?sce.Lookup(srb._bmk):this;
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
        public override string Alias => (alias.Length!=0)?alias:base.Alias;
        public override SDict<int, string> Display => (display==SDict<int,string>.Empty)?sce.Display:display;
    }
    public class SGroupQuery : SQuery
    {
        public readonly SQuery source;
        public readonly SDict<int, string> groupby;
        public readonly SList<Serialisable> having;
        public SGroupQuery(SDatabase db,Reader f):base(Types.SGroupQuery,f)
        {
            source = f._Get(db) as SQuery ?? throw new Exception("Query expected");
            var g = SDict<int, string>.Empty;
            var h = SList<Serialisable>.Empty;
            var n = f.GetInt();
            for (var i = 0; i < n; i++)
                g = g + (i, f.GetString());
            n = f.GetInt();
            for (var i = 0; i < n; i++)
                h = h+(f._Get(db).Lookup(source.names), i);
            groupby = g;
            having = h;
        }
        public SGroupQuery(SQuery s,SDict<int,string> d,SDict<int,Serialisable> c,
            SDict<string,Serialisable> n,SDict<int,string> g,SList<Serialisable> h) 
            : base(Types.SGroupQuery, d,c,n) 
        {
            source = s;
            groupby = g;
            having = h;
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            source.Put(f);
            f.PutInt(groupby.Length);
            for (var b = groupby.First(); b != null; b = b.Next())
                f.PutString(b.Value.val);
            f.PutInt(having.Length);
            for (var b = having.First(); b != null; b = b.Next())
                b.Value.Put(f);
        }
        public static SGroupQuery Get(SDatabase d,Reader f)
        {
            return new SGroupQuery(d, f);
        }
        public override RowSet RowSet(STransaction tr, ILookup<string, Serialisable> nms)
        {
            return new GroupRowSet(tr, this, nms);
        }
        public override Serialisable Lookup(ILookup<string, Serialisable> nms)
        {
            return (nms is SearchRowSet.SearchRowBookmark srb) ? source.Lookup(srb._bmk) : this;
        }
        public override void Append(SDatabase? db, StringBuilder sb)
        {
            source.Append(db, sb);
            sb.Append(" groupby ");
            var cm = "";
            for (var b =groupby.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.Value.val);
            }
            if (having.Length>0)
            {
                sb.Append(" having ");
                cm = "";
                for (var b=having.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = " and ";
                    b.Value.Append(db,sb);
                }
            }
        }
        public override string Alias => source.Alias;
    }
    public class SSelectStatement : SQuery
    {
        public readonly bool distinct,aggregates;
        public readonly SList<SOrder> order;
        public readonly SQuery qry;
        /// <summary>
        /// The select statement has a source query, 
        /// complex expressions and aliases for its columns,
        /// and an ordering
        /// </summary>
        /// <param name="d">Whrther distinct has been specified</param>
        /// <param name="a">The aliases (display) or null</param>
        /// <param name="c">The column expressions or null</param>
        /// <param name="q">The source query, assumed analysed</param>
        /// <param name="or">The ordering</param>
        public SSelectStatement(bool d, SDict<int,string>? a, SDict<int,Serialisable>? c, SQuery q, SList<SOrder> or) 
            : base(Types.SSelect,a??q.display,c??q.cpos,q.names)
        {
            distinct = d;  qry = q; order = or;
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
            SDict<int,string>? a = (n>0)?SDict<int,string>.Empty:null;
            SDict<int,Serialisable>? c = (n>0)?SDict<int,Serialisable>.Empty:null;
            for (var i = 0; i < n; i++)
            {
                a = a+(i, f.GetString());
                c = c+(i,f._Get(db));
            }
            var q = (SQuery)f._Get(db);
            var o = SList<SOrder>.Empty;
            n = f.GetInt();
            for (var i = 0; i < n; i++)
                o = o+((SOrder)f._Get(db), i);
            return new SSelectStatement(d,a,c,q,o);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.WriteByte((byte)(distinct ? 1 : 0));
            f.PutInt(display.Length);
            var ab = display.First();
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
            var ab = display.First();
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

        public override RowSet RowSet(STransaction tr,ILookup<string,Serialisable> nms)
        {
            RowSet r = new SelectRowSet(tr,this,nms);
            if (distinct)
                r = new DistinctRowSet(r);
            if (order.Length != 0)
                r = new OrderedRowSet(r, this);
            return r;
        }
        public override Serialisable Lookup(ILookup<string,Serialisable> nms)
        {
            var r = (RowBookmark)nms;
            if (display.Length == 0)
                return r._ob;
            return new SRow(this,r);
        }
        public override string Alias => qry.Alias;
        public override SDict<int, string> Display => (display == SDict<int, string>.Empty) ? qry.Display : display;
    }
    public class SOrder : Serialisable
    {
        public readonly Serialisable col;
        public readonly bool desc;
        public SOrder(Serialisable c,bool d) :base(Types.SOrder)
        {
            col = c; desc = d;
        }
        protected SOrder(SDatabase db,Reader f) :base(Types.SOrder)
        {
            col = f._Get(db);
            desc = f.ReadByte() == 1;
        }
        public override bool isValue => false;
        public static SOrder Get(SDatabase db,Reader f)
        {
            return new SOrder(db, f);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            col.Put(f);
            f.WriteByte((byte)(desc ? 1 : 0));
        }
    }
}
