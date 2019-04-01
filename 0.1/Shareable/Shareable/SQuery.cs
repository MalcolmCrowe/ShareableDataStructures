using System;
using System.Text;
#nullable enable
namespace Shareable
{
    ///The only instance of this class is STATIC.
    ///All other instances are of subclasses.
    public class SQuery : SDbObject
    {
        public readonly SDict<int, (long,string)> display;
        public readonly SDict<int,Serialisable> cpos;
        public readonly SDict<long, Serialisable> refs;
        public static readonly SQuery Static = new SQuery(Types.SQuery,SysTable._uid--);
        public SQuery(Types t, long u) : base(t, u)
        {
            display = SDict<int, (long,string)>.Empty;
            cpos = SDict<int,Serialisable>.Empty;
            refs = SDict<long, Serialisable>.Empty;
        }
        public SQuery(Types t, STransaction tr) : base(t, tr)
        {
            display = SDict<int, (long,string)>.Empty;
            cpos = SDict<int,Serialisable>.Empty;
            refs = SDict<long, Serialisable>.Empty;
        }
        public SQuery(SQuery q) : base(q)
        {
            display = q.display;
            cpos = q.cpos;
            refs = q.refs;
        }
        public SQuery(Types t,SQuery q) : base(t)
        {
            display = q.display;
            cpos = q.cpos;
            refs = q.refs;
        }
        public SQuery(SQuery q,SDict<int,(long,string)> a,SDict<int,Serialisable>cp,
            SDict<long,Serialisable>cn) :base(q)
        {
            display = a;
            cpos = cp;
            refs = cn;
        }
        /// <summary>
        /// a and c must have same length: c might have complex values
        /// </summary>
        /// <param name="t"></param>
        /// <param name="a">aliases</param>
        /// <param name="c">column expressions</param>
        public SQuery(Types t,SDict<int,(long,string)> a,SDict<int,Serialisable> c) : base(t)
        {
            var cn = SDict<long, Serialisable>.Empty;
            var ab = a.First();
            for (var cb = c.First();ab!=null && cb!=null;ab=ab.Next(),cb=cb.Next())
                cn += (ab.Value.Item2.Item1, cb.Value.Item2);
            display = a;
            cpos = c;
            refs = cn;
        }
        protected SQuery(Types t, Reader f) : base(t, f)
        {
            display = SDict<int, (long,string)>.Empty;
            cpos = SDict<int,Serialisable>.Empty;
            refs = SDict<long, Serialisable>.Empty;
        }
        /// <summary>
        /// This constructor is only called when committing am STable.
        /// Ignore the columns defined in the transaction.
        /// </summary>
        /// <param name="q">The current state of the STable in the transaction</param>
        /// <param name="f"></param>
        protected SQuery(SQuery q, AStream f) : base(q, f)
        {
            display = SDict<int, (long,string)>.Empty;
            cpos = SDict<int, Serialisable>.Empty;
            refs = SDict<long, Serialisable>.Empty;
        }
        /// <summary>
        /// Add the names defined by this query to the given parsing symbol table. 
        /// </summary>
        /// <param name="db">Can be SDatabase when storing viewdefinitions</param>
        /// <param name="pt"></param>
        /// <returns></returns>
        public virtual SDict<long, long> Names(SDatabase db, SDict<long, long> pt)
        {
            // prepare a list of names this query defines
            var ns = SDict<string, long>.Empty;
            if (uid!=-1)
                ns+=(db.uids[uid],uid);
            for (var b = display.First(); b != null; b = b.Next())
                ns += (b.Value.Item2.Item2, b.Value.Item2.Item1);
            // scan the list of client-side uids if any to add entries to the parsing table
            for (var b = db.uids.PositionAt(maxAlias); b != null && b.Value.Item1<0; b = b.Next())
                if (ns.Contains(b.Value.Item2))
                    pt += (b.Value.Item1, ns[b.Value.Item2]);
            return pt;
        }
        protected long Use(long u, SDict<long, long> ta)
        {
            return ta.Contains(u) ? ta[u] : u;
        }
        protected (long, string) Use((long, string) n, SDict<long, long> ta)
        {
            return ta.Contains(n.Item1)?(ta[n.Item1],n.Item2):n;
        }
        protected (long,long) Use((long,long) u, SDict<long, long> ta)
        {
            return (Use(u.Item1,ta),Use(u.Item2,ta));
        }
        public new static SQuery Get(Reader f)
        {
            return Static;
        }
        public override Serialisable Prepare(STransaction db, SDict<long, long> pt)
        {
            var ds = SDict<int, (long,string)>.Empty;
            var cp = SDict<int, Serialisable>.Empty;
            for (var b = display.First(); b != null; b = b.Next())
                ds += (b.Value.Item1, Prepare(b.Value.Item2, pt));
            for (var b = cpos.First(); b != null; b = b.Next())
                cp += (b.Value.Item1, b.Value.Item2.Prepare(db, pt));
            return new SQuery(type,ds,cp);
        }
        /// <summary>
        /// Construct the Rowset for the given SDatabase (may have changed since SQuery was built)
        /// </summary>
        /// <param name="db">The current state of the database or transaction</param>
        /// <param name="ags">The aggregates found so far</param>
        /// <param name="cx">A context for evaluation</param>
        /// <returns></returns>
        public virtual RowSet RowSet(STransaction tr,SQuery top,SDict<long,Serialisable>ags)
        {
            throw new NotImplementedException();
        }
        public virtual long Alias => -1;
        public virtual SDict<int, (long,string)> Display => display;
        internal static long CheckAlias(SDict<long,string>uids,long u)
        {
            var r = u - 1000000;
            return uids.Contains(r) ? r : u;
        }
        internal static (long,string) CheckAlias(SDict<long,string>uids,(long,string)n)
        {
            var r = n.Item1 - 1000000;
            return uids.Contains(r) ? (r, n.Item2) : n;
        }
        internal static (long,long) CheckAlias(SDict<long, string> uids, (long,long) p)
        {
            return (CheckAlias(uids, p.Item1), CheckAlias(uids, p.Item2));
        }
        public override string ToString()
        {
            return "SQuery";
        }
    }
    public class SJoin : SQuery
    {
        [Flags]
        public enum JoinType { None=0, Inner=1, Natural=2, Cross=4,
            Left =8, Right=16, Named=32 };
        public readonly JoinType joinType;
        public readonly bool outer;
        public readonly SQuery left,right;
        public readonly SList<SExpression> ons; // constrained by Parser to lcol=rcol
        public readonly SDict<long,long> uses; // Item1 is for RIGHT, Item2 for LEFT
        public SJoin(Reader f) : base(Types.STableExp, _Join(f))
        {
            left = f._Get() as SQuery ?? throw new Exception("Query expected");
            outer = f.GetInt() == 1;
            joinType = (JoinType)f.GetInt();
            right = f._Get() as SQuery ?? throw new Exception("Query expected");
            var n = f.GetInt();
            var on = SList<SExpression>.Empty;
            var us = SDict<long, long>.Empty;
            var tr = (STransaction)f.db;
            var lns = SDict<string, long>.Empty;
            for (var b = left.Display.First(); b != null; b = b.Next())
                lns += (b.Value.Item2.Item2,b.Value.Item2.Item1);
            var rns = SDict<string, long>.Empty;
            for (var b = right.Display.First(); b != null; b = b.Next())
            {
                var nm = b.Value.Item2.Item2;
                if (joinType == JoinType.Natural && lns.Contains(nm))
                    us += (b.Value.Item2.Item1, lns[nm]);
                rns += (nm, b.Value.Item2.Item1);
            }
            if (joinType.HasFlag(JoinType.Named))
                for (var i = 0; i < n; i++)
                {
                    var nm = tr.uids[f.GetLong()];
                    if (!(lns.Contains(nm) && rns.Contains(nm)))
                        throw new Exception("name " + nm + " not present in Join");
                    us += (rns[nm], lns[nm]);
                }
            else if (!joinType.HasFlag(JoinType.Cross))
                for (var i = 0; i < n; i++)
                {
                    var e = f._Get() as SExpression
                        ?? throw new Exception("ON exp expected");
                    on += e;
                }
            ons = on;
            uses = us;
            f.context = this;
        }
        // We peek at the table expression to compute the set of columns of the join
        static SQuery _Join(Reader f)
        {
            f.GetInt();
            var st = f.pos;
            var d = SDict<int, (long,string)>.Empty;
            var c = SDict<int, Serialisable>.Empty;
            var nms = SDict<string, long>.Empty;
            var left = f._Get() as SQuery ?? throw new Exception("Query expected");
            var outer = f.GetInt() == 1;
            var joinType = (JoinType)f.GetInt();
            var right = f._Get() as SQuery ?? throw new Exception("Query expected");
            var ab = left.Display.First();
            var uses = SDict<long,long>.Empty;
            if (joinType.HasFlag(JoinType.Named))
            {
                var n = f.GetInt();
                for (var i = 0; i < n; i++)
                    uses += (f.GetLong(),f.GetLong());
            }
            var k = 0;
            for (var lb = left.cpos.First(); ab!=null && lb != null; ab=ab.Next(), lb = lb.Next())
            {
                var col = lb.Value;
                var u = ab.Value.Item2;
                d += (k, u);
                c += (k, col.Item2);
                nms += (u.Item2,u.Item1);
                k++;
            }
            ab = right.Display.First();
            for (var rb = right.cpos.First(); ab != null && rb != null; ab = ab.Next(), rb = rb.Next())
            {
                var u = ab.Value.Item2;
                var n = u.Item2;
                if (joinType == JoinType.Natural && nms.Contains(n))
                    continue;
                if (uses.Contains(u.Item1))
                    continue;
                var col = rb.Value;
                d += (k, u);
                c += (k, col.Item2);
                k++;
            }
            f.pos = st;
            return new SQuery(Types.STableExp, d, c);
        }
        public SJoin(SQuery lf, bool ou, JoinType jt, SQuery rg, SList<SExpression> on,
            SDict<long,long> us,SDict<int, (long,string)> d, SDict<int, Serialisable> c)
            : base(Types.STableExp, d, c)
        {
            left = lf; right = rg; outer = ou; joinType = jt; ons = on; uses = us;
        }
        public override SDict<long, long> Names(SDatabase tr, SDict<long, long> pt)
        {
            return right.Names(tr, left.Names(tr,pt));
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            left.Put(f);
            f.PutInt(outer ? 1 : 0);
            f.PutInt((int)joinType);
            right.Put(f);
            f.PutInt((ons.Length+uses.Length)??0); // at most one of these is nonzero
            for (var b = ons.First(); b != null; b = b.Next())
                b.Value.Put(f);
            for (var b = uses.First(); b != null; b = b.Next())
                f.PutLong(b.Value.Item1);
        }
        public override Serialisable UseAliases(SDatabase db, SDict<long, long> ta)
        {
            var os = SList<SExpression>.Empty;
            var us = SDict<long, long>.Empty;
            var ds = SDict<int, (long,string)>.Empty;
            var cs = SDict<int, Serialisable>.Empty;
            var n = 0;
            for (var b = ons.First(); b != null; b = b.Next())
                os += ((SExpression)b.Value.UseAliases(db, ta), n++);
            n = 0;
            for (var b = uses.First(); b != null; b = b.Next())
                us += Use(b.Value, ta);
            for (var b = display.First(); b != null; b = b.Next())
                ds += (b.Value.Item1, Use(b.Value.Item2, ta));
            for (var b = cpos.First(); b != null; b = b.Next())
                cs += (b.Value.Item1, b.Value.Item2.UseAliases(db, ta));
            return new SJoin((SQuery)left.UseAliases(db, ta), outer, joinType,
                (SQuery)right.UseAliases(db, ta), os, us, ds, cs);
        }
        public override Serialisable Prepare(STransaction db, SDict<long, long> pt)
        {
            var os = SList<SExpression>.Empty;
            var us = SDict<long,long>.Empty;
            var ds = SDict<int, (long,string)>.Empty;
            var cs = SDict<int, Serialisable>.Empty;
            var n = 0;
            for (var b = ons.First(); b != null; b = b.Next())
                os += ((SExpression)b.Value.Prepare(db, pt), n++);
            n = 0;
            for (var b = uses.First(); b != null; b = b.Next())
                us += Prepare(b.Value, pt);
            var lf = (SQuery)left.Prepare(db, pt);
            var rg = (SQuery)right.Prepare(db, pt);
            var lns = SDict<string, long>.Empty;
            for (var b = lf.Display.First(); b != null; b = b.Next())
            {
                var u = b.Value.Item2;
                lns += (u.Item2, u.Item1);
            }
            var rns = SDict<string,long>.Empty;
            for (var b = rg.Display.First(); b != null; b = b.Next())
            {
                var u = b.Value.Item2;
                rns += (u.Item2, u.Item1);
            }
            for (var b = lf.Display.First(); b != null; b = b.Next())
            {
                var ou = b.Value.Item2.Item1;
                var dn = b.Value.Item2.Item2;
                if (rns.Contains(dn) && joinType != JoinType.Natural)
                    dn = db.Name(lf.Alias) + "." + dn;
                ds += (n, (ou, dn));
                n++;
            }
            for (var b = rg.Display.First(); b != null; b = b.Next())
            {
                var ou = b.Value.Item2.Item1;
                var dn = b.Value.Item2.Item2;
                if (lns.Contains(dn) && joinType != JoinType.Natural)
                    dn = db.Name(rg.Alias) + "." + dn;
                ds += (n, (ou, dn));
                n++;
            }
            if (cpos != null)
                for (var b = cpos.First(); b != null; b = b.Next())
                {
                    var k = b.Value.Item1;
                    var v = b.Value.Item2.Prepare(db, pt);
                    cs += (k, v);
                }
            return new SJoin((SQuery)left.Prepare(db, pt), outer, joinType,
                (SQuery)right.Prepare(db, pt), os, us, ds, cs);
        }
        public override Serialisable UpdateAliases(SDict<long, string> uids)
        {
            var w = uids.First();
            if (w == null || w.Value.Item1 > -1000000)
                return this;
            var os = SList<SExpression>.Empty;
            var us = SDict<long,long>.Empty;
            var ds = SDict<int, (long,string)>.Empty;
            var cs = SDict<int, Serialisable>.Empty;
            var n = 0;
            for (var b = ons.First(); b != null; b = b.Next())
                os += ((SExpression)b.Value.UpdateAliases(uids), n++);
            n = 0;
            for (var b = uses.First(); b != null; b = b.Next())
                us += CheckAlias(uids,b.Value);
            for (var b = display.First(); b != null; b = b.Next())
                ds += (b.Value.Item1, CheckAlias(uids,b.Value.Item2));
            for (var b = cpos.First(); b != null; b = b.Next())
                cs += (b.Value.Item1, b.Value.Item2.UpdateAliases(uids));
            return new SJoin((SQuery)left.UpdateAliases(uids), outer, joinType,
                (SQuery)right.UpdateAliases(uids), os, us, ds, cs);
        }
        public new static SJoin Get(Reader f)
        {
            return new SJoin(f);
        }
        public int Compare(RowBookmark lb,RowBookmark rb)
        {
            for (var b = ons.First(); b != null; b = b.Next())
            {
                var ex = b.Value as SExpression;
                var c = ex.left.Lookup(lb._cx).CompareTo(ex.right.Lookup(rb._cx));
                if (c!=0)
                    return c;
            }
            for (var b = uses.First(); b != null; b = b.Next())
            {
                var c = lb._cx[b.Value.Item1].CompareTo(rb._cx[b.Value.Item2]);
                if (c != 0)
                    return c;
            }
            return 0;
        }
        public override RowSet RowSet(STransaction tr, SQuery top, SDict<long, Serialisable> ags)
        {
            var lf = left.RowSet(tr, left, ags);
            var rg = right.RowSet(lf._tr, right, ags);
            return new JoinRowSet(tr, top, this, lf, rg, ags);
        }
        public override void Append(SDatabase db, StringBuilder sb)
        {
            left.Append(db, sb);
            if (outer)
                sb.Append(" outer ");
            if (joinType != JoinType.Inner)
            { sb.Append(" "); sb.Append(joinType); sb.Append(" "); }
            right.Append(db, sb);
            if (ons.Length!=0)
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
    public class SAlias : SQuery
    {
        public readonly SQuery qry;
        public readonly long alias;
        public SAlias(SQuery q,long a,Reader f,long u) :
            base(Types.SAlias,u)
        {
            var tr = (STransaction)f.db;
            qry = (SQuery)q.Prepare(tr,SDict<long,long>.Empty);
            alias = a;
            f.db = tr + (a, qry);
        }
        public SAlias(SQuery q,long a,long u = -1) :base(Types.SAlias,u)
        {
            qry = q;
            alias = a;
        }
        public override SDict<long, long> Names(SDatabase tr, SDict<long, long> pt)
        {
            return qry.Names(tr, pt);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            qry.Put(f);
            f.PutLong(alias);
        }
        public override Serialisable UseAliases(SDatabase db, SDict<long, long> ta)
        {
            return new SAlias((SQuery)qry.UseAliases(db,ta),alias,uid);
        }
        public override Serialisable UpdateAliases(SDict<long, string> uids)
        {
            var q = (SQuery)qry.UpdateAliases(uids);
            return (q==qry)?this:new SAlias(q,alias,uid);
        }
        public override Serialisable Prepare(STransaction db, SDict<long, long> pt)
        {
            return new SAlias((SQuery)qry.Prepare(db,pt),alias,uid);
        }
        public new static SAlias Get(Reader f)
        {
            var u = f.GetLong();
            var q = (SQuery)f._Get();
            return new SAlias(q,f.GetLong(),f,u);
        }
        public override void Append(SDatabase db,StringBuilder sb)
        {
            qry.Append(db,sb);
            sb.Append(" "); sb.Append(alias);
        }
        public override long Alias => alias;
        public override RowSet RowSet(STransaction tr, SQuery top, SDict<long, Serialisable> ags)
        {
            return new AliasRowSet(this,qry.RowSet(tr, top, ags));
        }
        public override string ToString()
        {
            return qry.ToString() + " as " + _Uid(alias);
        }
    }
    public class SSearch : SQuery
    {
        public readonly SQuery sce;
        public readonly SList<Serialisable> where;
        public SSearch(SQuery sc,Reader f,long u):base(Types.SSearch,u)
        {
            sce = sc;
            var w = SList<Serialisable>.Empty;
            var n = f.GetInt();
            for (var i=0;i<n;i++)
                w += (f._Get(),i);
            where = w;
            f.context = this;
        }
        public SSearch(SQuery s,SList<Serialisable> w)
            :base(Types.SSearch, s.display, s.cpos)
        {
            sce = s;
            where = w;
        }
        public override SDict<long, long> Names(SDatabase tr, SDict<long, long> pt)
        {
            return sce.Names(tr, pt);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            sce.Put(f);
            f.PutInt(where.Length);
            for (var b=where.First();b!=null;b=b.Next())
                b.Value.Put(f);
        }
        public override Serialisable UseAliases(SDatabase db, SDict<long, long> ta)
        {
            var w = SList<Serialisable>.Empty;
            var n = 0;
            for (var b = where.First(); b != null; b = b.Next())
                w += (b.Value.UseAliases(db, ta), n++);
            return new SSearch((SQuery)sce.UseAliases(db, ta), w);
        }
        public override Serialisable Prepare(STransaction db, SDict<long, long> pt)
        {
            var w = SList<Serialisable>.Empty;
            var n = 0;
            for (var b = where.First(); b != null; b = b.Next())
                w += (b.Value.Prepare(db, pt), n++);
            return new SSearch((SQuery)sce.Prepare(db, pt),w);
        }
        public override Serialisable UpdateAliases(SDict<long, string> uids)
        {
            var uu = uids.First();
            if (uu == null || uu.Value.Item1 > -1000000)
                return this;
            var w = SList<Serialisable>.Empty;
            var n = 0;
            for (var b = where.First(); b != null; b = b.Next())
                w += (b.Value.UpdateAliases(uids), n++);
            return new SSearch((SQuery)sce.UpdateAliases(uids), w);
        }
        public new static SSearch Get(Reader f)
        {
            var u = f.GetLong();
            var sce = f._Get() as SQuery ?? throw new Exception("Query expected");
            return new SSearch(sce,f,u);
        }
        public override RowSet RowSet(STransaction tr,SQuery top,SDict<long,Serialisable>ags)
        {
            for (var b = where.First(); b != null; b = b.Next())
                ags = b.Value.Aggregates(ags);
            return new SearchRowSet(tr, top, this, ags);
        }
        public override Serialisable Lookup(Context cx)
        {
            return (cx.refs is SearchRowSet.SearchRowBookmark srb)?sce.Lookup(srb._bmk._cx):this;
        }
        public override void Append(SDatabase db,StringBuilder sb)
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
        public override SDict<int, (long,string)> Display => (display==SDict<int,(long,string)>.Empty)?sce.Display:display;
    }
    public class SGroupQuery : SQuery
    {
        public readonly SQuery source;
        public readonly SDict<int, long> groupby;
        public readonly SList<Serialisable> having;
        public SGroupQuery(SQuery sc, Reader f,long u):base(Types.SGroupQuery,u)
        {
            source = sc;
            var g = SDict<int, long>.Empty;
            var h = SList<Serialisable>.Empty;
            var n = f.GetInt();
            for (var i = 0; i < n; i++)
                g += (i, f.GetLong());
            n = f.GetInt();
            for (var i = 0; i < n; i++)
                h += (f._Get().Lookup(new Context(source.refs,null)), i);
            groupby = g;
            having = h;
            f.context = this;
        }
        public SGroupQuery(SQuery s,SDict<int,(long,string)> d,SDict<int,Serialisable> c,
            SDict<int,long> g,SList<Serialisable> h) 
            : base(Types.SGroupQuery, d,c) 
        {
            source = s;
            groupby = g;
            having = h;
        }
        public override SDict<long, long> Names(SDatabase tr, SDict<long, long> pt)
        {
            return source.Names(tr, pt);
        }
        public override Serialisable UseAliases(SDatabase db, SDict<long, long> ta)
        {
            var ds = SDict<int, (long,string)>.Empty;
            var cs = SDict<int, Serialisable>.Empty;
            var g = SDict<int, long>.Empty;
            var h = SList<Serialisable>.Empty;
            var n = 0;
            for (var b = display.First(); b != null; b = b.Next())
                ds += (b.Value.Item1, Use(b.Value.Item2, ta));
            for (var b = cpos.First(); b != null; b = b.Next())
                cs += (b.Value.Item1, b.Value.Item2.UseAliases(db, ta));
            for (var b = groupby.First(); b != null; b = b.Next())
                g += (b.Value.Item1, Use(b.Value.Item2, ta));
            for (var b = having.First(); b != null; b = b.Next())
                h += (b.Value.UseAliases(db, ta), n++);
            return new SGroupQuery((SQuery)source.UseAliases(db, ta), ds, cs, g, h);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            source.Put(f);
            f.PutInt(groupby.Length);
            for (var b = groupby.First(); b != null; b = b.Next())
                f.PutLong(b.Value.Item2);
            f.PutInt(having.Length);
            for (var b = having.First(); b != null; b = b.Next())
                b.Value.Put(f);
        }
        public override Serialisable Prepare(STransaction db, SDict<long, long> pt)
        {
            var ds = SDict<int, (long,string)>.Empty;
            var cs = SDict<int, Serialisable>.Empty;
            var g = SDict<int, long>.Empty;
            var h = SList<Serialisable>.Empty;
            var n = 0;
            for (var b = display.First(); b != null; b = b.Next())
                ds += (b.Value.Item1, Prepare(b.Value.Item2, pt));
            for (var b = cpos.First(); b != null; b = b.Next())
                cs += (b.Value.Item1, b.Value.Item2.Prepare(db, pt));
            for (var b = groupby.First(); b != null; b = b.Next())
                g += (b.Value.Item1, Prepare(b.Value.Item2, pt));
            for (var b = having.First(); b != null; b = b.Next())
                h += (b.Value.Prepare(db, pt), n++);
            return new SGroupQuery((SQuery)source.Prepare(db,pt),ds,cs,g,h);
        }
        public override Serialisable UpdateAliases(SDict<long, string> uids)
        {
            var w = uids.First();
            if (w == null || w.Value.Item1 > -1000000)
                return this;
            var ds = SDict<int, (long,string)>.Empty;
            var cs = SDict<int, Serialisable>.Empty;
            var g = SDict<int, long>.Empty;
            var h = SList<Serialisable>.Empty;
            var n = 0;
            for (var b = display.First(); b != null; b = b.Next())
            {
                var nm = b.Value.Item2;
                var u = nm.Item1;
                if (uids.Contains(u - 1000000))
                    u -= 1000000;
                ds += (b.Value.Item1, (u,nm.Item2));
            }
            for (var b = cpos.First(); b != null; b = b.Next())
                cs += (b.Value.Item1, b.Value.Item2.UpdateAliases(uids));
            for (var b = groupby.First(); b != null; b = b.Next())
            {
                var u = b.Value.Item2;
                if (uids.Contains(u - 1000000))
                    u -= 1000000;
                g += (b.Value.Item1, u);
            }
            for (var b = having.First(); b != null; b = b.Next())
                h += (b.Value.UpdateAliases(uids), n++);
            return new SGroupQuery((SQuery)source.UpdateAliases(uids), ds, cs,
                g, h);
        }
        public new static SGroupQuery Get(Reader f)
        {
            var u = f.GetLong();
            var source = f._Get() as SQuery ?? throw new Exception("Query expected");
            return new SGroupQuery(source,f,u);
        }
        public override RowSet RowSet(STransaction tr, SQuery top, SDict<long,Serialisable>ags)
        {
            return new GroupRowSet(tr, top, this, ags);
        }
        public override Serialisable Lookup(Context cx)
        {
            return (cx.refs is SearchRowSet.SearchRowBookmark srb) ? 
                source.Lookup(cx) : this;
        }
        public override void Append(SDatabase db, StringBuilder sb)
        {
            source.Append(db, sb);
            sb.Append(" groupby ");
            var cm = "";
            for (var b =groupby.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.Value.Item2);
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
        public override long Alias => source.Alias;
        public override string ToString()
        {
            var sb = new StringBuilder(source.ToString());
            var cm = " groupby ";
            for (var b=groupby.First();b!=null;b=b.Next())
            {
                sb.Append(cm);cm = ",";
                sb.Append(_Uid(b.Value.Item2));
            }
            cm = " having ";
            for (var b=having.First();b!=null;b=b.Next())
            {
                sb.Append(cm);cm = " and ";
                sb.Append(b.Value.ToString());
            }
            return sb.ToString();
        }
    }
    public class SSelectStatement : SQuery
    {
        public readonly bool distinct;
        public readonly SList<SOrder> order;
        public readonly SQuery qry;
        /// <summary>
        /// The select statement has a source query, 
        /// complex expressions and aliases for its columns,
        /// and an ordering
        /// </summary>
        /// <param name="d">Whether distinct has been specified</param>
        /// <param name="a">The aliases (display) or null</param>
        /// <param name="c">The column expressions or null</param>
        /// <param name="q">The source query, assumed analysed</param>
        /// <param name="or">The ordering</param>
        public SSelectStatement(bool d, SDict<int,(long,string)> a, SDict<int,Serialisable> c, 
            SQuery q, SList<SOrder> or) 
            : base(Types.SSelect,a,c)
        {
            distinct = d;  qry = q; order = or;
        }
        public override SDict<long, long> Names(SDatabase tr, SDict<long, long> pt)
        {
            return qry.Names(tr, pt);
        }
        public new static SSelectStatement Get(Reader f)
        {
            var db = f.db;
            f.GetInt(); // uid for the SSelectStatement probably -1
            var d = f.ReadByte() == 1;
            var n = f.GetInt();
            var a = SDict<int,(long,string)>.Empty;
            var c = SDict<int,Serialisable>.Empty;
            for (var i = 0; i < n; i++)
            {
                var al = f.GetLong();
                a += (i, (al,f.db.Name(al)));
                c += (i,f._Get());
            }
            var q = (SQuery)f._Get();
            var o = SList<SOrder>.Empty;
            var m = f.GetInt();
            for (var i = 0; i < m; i++)
                o += ((SOrder)f._Get(), i);
            var ss = new SSelectStatement(d,a,c,q,o);
            f.context = ss;
            return ss;
        }
        public override Serialisable UseAliases(SDatabase db, SDict<long, long> ta)
        {
            var os = SList<SOrder>.Empty;
            var n = 0;
            for (var b = order.First(); b != null; b = b.Next())
                os += ((SOrder)b.Value.UseAliases(db, ta), n++);
            var ds = SDict<int, (long,string)>.Empty;
            var cs = SDict<int, Serialisable>.Empty;
            for (var b = display.First(); b != null; b = b.Next())
                ds += (b.Value.Item1, Use(b.Value.Item2, ta));
            for (var b = cpos.First(); b != null; b = b.Next())
                cs += (b.Value.Item1, b.Value.Item2.UseAliases(db, ta));
            var qy = (SQuery)qry.UseAliases(db, ta);
            return new SSelectStatement(distinct, ds, cs, qy, os);
        }
        public override Serialisable Prepare(STransaction db, SDict<long, long> pt)
        {
            var os = SList<SOrder>.Empty;
            var n = 0;
            for (var b = order.First(); b != null; b = b.Next())
                os += ((SOrder)b.Value.Prepare(db, pt), n++);
            var ds = SDict<int, (long,string)>.Empty;
            var cs = SDict<int, Serialisable>.Empty;
            for (var b = display.First(); b != null; b = b.Next())
                ds += (b.Value.Item1, Prepare(b.Value.Item2, pt));
            for (var b = cpos.First(); b != null; b = b.Next())
                cs += (b.Value.Item1, b.Value.Item2.Prepare(db, pt));
            var qy = (SQuery)qry.Prepare(db, pt);
            return new SSelectStatement(distinct, ds, cs, qy, os);
        }
        public override Serialisable UpdateAliases(SDict<long, string> uids)
        {
            var w = uids.First();
            if (w == null || w.Value.Item1 > -1000000)
                return this;
            var os = SList<SOrder>.Empty;
            var n = 0;
            for (var b = order.First(); b != null; b = b.Next())
                os += ((SOrder)b.Value.UpdateAliases(uids), n++);
            var ds = SDict<int, (long,string)>.Empty;
            var cs = SDict<int, Serialisable>.Empty;
            for (var b = display.First(); b != null; b = b.Next())
            {
                var nm = b.Value.Item2;
                var u = nm.Item1;
                if (uids.Contains(u - 1000000))
                    u -= 1000000;
                ds += (b.Value.Item1, (u,nm.Item2));
            }
            for (var b = cpos.First(); b != null; b = b.Next())
                cs += (b.Value.Item1, b.Value.Item2.UpdateAliases(uids));
            var qy = (SQuery)qry.UpdateAliases(uids);
            return new SSelectStatement(distinct, ds, cs, qy, os);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.WriteByte((byte)(distinct ? 1 : 0));
            f.PutInt(display.Length);
            var ab = display.First();
            for (var b = cpos.First(); ab!=null && b != null; b = b.Next(), ab=ab.Next())
            {
                f.PutLong(ab.Value.Item2.Item1);
                b.Value.Item2.Put(f);
            }
            qry.Put(f);
            f.PutInt(order.Length.HasValue?order.Length.Value:0);
            for (var b=order.First();b!=null;b=b.Next())
                b.Value.Put(f);
        }
        public override void Append(SDatabase db,StringBuilder sb)
        {
            if (distinct)
                sb.Append("distinct ");
            var cm = "";
            var ab = display.First();
            for (var b = cpos.First(); ab != null && b != null; b = b.Next(), ab = ab.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.Value.Item2);
                if (b.Value.Item2 is SSelector sc && ab.Value.Item2.Item1 == sc.uid)
                    continue;
                sb.Append(" as "); sb.Append(ab.Value.Item2.Item2);
            }
            sb.Append(' ');
            sb.Append(qry);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Select ");
            var cm = "";
            var ab = display.First();
            for (var b = cpos.First(); ab!=null && b != null; b = b.Next(),ab=ab.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.Value.Item2);
                if (b.Value.Item2 is SSelector sc && ab.Value.Item2.Item1==sc.uid)
                    continue;
                sb.Append(" as "); sb.Append(ab.Value.Item2.Item2);
            }
            sb.Append(' ');
            sb.Append(qry);
            return sb.ToString();
        }

        public override RowSet RowSet(STransaction tr,SQuery top,SDict<long,Serialisable>ags)
        {
            for (var b = order.First(); b != null; b = b.Next())
                ags = b.Value.col.Aggregates(ags);
            var ags1 = ags;
            for (var b = cpos.First(); b != null; b = b.Next())
                ags1 = b.Value.Item2.Aggregates(ags1);
            RowSet r = new SelectRowSet(qry.RowSet(tr, this, ags1), this,ags);
            // perform another pass on the selectlist just in case
            if (!(qry is SGroupQuery))
            {
                for (var b = cpos.First(); b != null; b = b.Next())
                    ags = b.Value.Item2.Aggregates(ags);
                if (ags.Length != 0)
                    r = new EvalRowSet(((SelectRowSet)r)._source, this, ags);
            }
            if (distinct)
                r = new DistinctRowSet(r);
            if (order.Length != 0)
                r = new OrderedRowSet(r, this);
            return r;
        }
        public override Serialisable Lookup(Context cx)
        {
            if (display.Length == 0)
                return (SRow)cx.refs;
            return new SRow(this,cx);
        }
        public override long Alias => qry.Alias;
        public override SDict<int, (long,string)> Display => (display == SDict<int, (long,string)>.Empty) ? qry.Display : display;
    }
    public class SOrder : Serialisable
    {
        public readonly Serialisable col;
        public readonly bool desc;
        public SOrder(Serialisable c,bool d) :base(Types.SOrder)
        {
            col = c; desc = d;
        }
        protected SOrder(Reader f) :base(Types.SOrder)
        {
            col = f._Get();
            desc = f.ReadByte() == 1;
        }
        public override bool isValue => false;
        public new static SOrder Get(Reader f)
        {
            return new SOrder(f);
        }
        public override Serialisable UseAliases(SDatabase db, SDict<long, long> ta)
        {
            return new SOrder(col.UseAliases(db, ta),desc);
        }
        public override Serialisable UpdateAliases(SDict<long, string> uids)
        {
            var c = col.UpdateAliases(uids);
            return (c == col) ? this : new SOrder(c, desc);
        }
        public override Serialisable Prepare(STransaction db, SDict<long, long> pt)
        {
            return new SOrder(col.Prepare(db, pt),desc);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            col.Put(f);
            f.WriteByte((byte)(desc ? 1 : 0));
        }
    }
}
