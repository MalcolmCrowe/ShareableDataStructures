using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Text;
using System;
using System.Configuration;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

namespace Pyrrho.Level3
{
    /// <summary>
    /// From is named after the SQL reserved word (explicit in most syntax)
    /// although the reserved word FROM can be followed by a table expression.
    /// From._static is the default From: the domain of the SELECT is
    /// computed from the select list (where * means use the From's domain).
    /// The domain of the From by default is that of its target
    /// (such as a base table, procedure, subquery or view)
    /// but will differ from it in general because of
    /// explicit column lists in syntax such as INSERT and CREATE VIEW.
    /// The domain of the VALUES will be the display columns of the From domain.
    /// </summary>
    internal class From : Query
    {
        internal const long
            Static = -152, // From (defpos for STATIC)
            Target = -153; // long (a table or view)
        internal long target => (long)(mem[Target]??-1L);
        internal readonly static From _static = new From();
        From() : base(Static) { }
        public From(Ident ic, Context cx, DBObject ob, QuerySpecification q=null,
            Grant.Privilege pr=Grant.Privilege.Select, string a= null, BList<Ident> cr = null) 
            : base(ic.iix, _Mem(ic,cx, ob,q,pr,a,cr))
        {
            if (ob is Table tb && tb.enforcement.HasFlag(pr) && cx.db._user!=cx.db.owner &&
                !cx.db.user.clearance.ClearanceAllows(tb.classification))
                throw new DBException("42105");
            var (_, ids) = cx.defs[ic.ident];
            for (var b=rowType.First();b!=null;b=b.Next())
            {
                var c = (SqlCopy)cx.obs[b.value()];
                if (ids[c.name].Item1<Transaction.Executables)
                    ids += (c.name, c.defpos, Ident.Idents.Empty); 
            }
            cx.defs += (ic.ident, ic.iix, ids); 
        }
        public From(long dp,Context cx,SqlCall pc,CList<long> cr=null)
            :base(dp,_Mem(dp,cx,pc,cr))
        { }
        public From(long dp,Context cx,RowSet rs,string a)
            :base(dp,_Mem(dp,cx,rs,a))
        { }
        protected From(long defpos, BTree<long, object> m) : base(defpos, m)
        { }
        public static From operator+(From f,(long,object) x)
        {
            return (From)f.New(f.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new From(defpos, m);
        }
        internal override DBObject New(Context cx,BTree<long, object> m)
        {
            if (defpos >= Transaction.Analysing || cx.parse == ExecuteStatus.Parse)
                return (m == mem) ? this : (Query)New(m);
            return (Query)cx.Add(new From(cx.GetUid(), m));
        }
        /// <summary>
        /// The main task here is to compute the rowType for the new From. 
        /// All columns in the From's rowtype will be SqlCopy. None will have table uids.
        /// We want to ensure that the From rowtype is different
        /// for each occurrence of a table in the query.
        /// If there are no stars in the select list, then we will find which columns
        /// are needed in the select list, and these will have unique uids.
        /// If there is a star in the select list that might be for this table,
        /// we append a list of all columns to the query and construct a rowType from
        /// that.
        /// </summary>
        /// <param name="dp"></param>
        /// <param name="db"></param>
        /// <param name="tb"></param>
        /// <param name="q">The query with the select list</param>
        /// <param name="pr"></param>
        /// <param name="cr">Aliases supplied if any</param>
        /// <returns></returns>
        static BTree<long, object> _Mem(Ident ic, Context cx, DBObject ob, QuerySpecification q,
           Grant.Privilege pr = Grant.Privilege.Select, string a=null,BList<Ident> cr = null)
        {
            var vs = BList<SqlValue>.Empty;
            var de = 1; // we almost always have some columns
            Domain dm = null;
            if (ob is Table)
                dm = ob.Inf(cx).domain;
            else if (ob is View)
                dm = ob.domain;
            cx._Add(ob);
            cx.AddDefs(ic, dm);
            var mg = q?.matching;
            var mp = CTree<long, bool>.Empty;
            if (cr == null)
            {
                var ma = BTree<string, DBObject>.Empty;
                for (var b = dm.rowType.First(); b != null && b.key()<dm.display; b = b.Next())
                {
                    var p = b.value();
                    var tc = (DBObject)cx.db.objects[p]??cx.obs[p];
                    var ci = (ObInfo)cx.role.infos[tc.defpos];
                    var nm = ci?.name ?? ((SqlValue)tc).name;
                    ma += (nm, tc);
                    if (ci?.alias != null)
                        ma += (ci.alias, tc);
                    if (tc is SqlCopy sc && sc.alias != null)
                        ma += (sc.alias, tc);
                }
                // we want to add everything from ti that matches cx.stars or q.Needs
                if (q != null)
                {
                    var qn = q.Needs(cx, CTree<long, bool>.Empty);
                    for (var b = qn.First(); b != null; b = b.Next())
                    {
                        var p = b.key();
                        if (cx.obs[p] is SqlValue uv && uv.domain == Domain.Content)
                        {
                            var tc = ma[uv.name];
                            if (tc == null)
                                continue;
                            var nv = new SqlCopy(uv.defpos, cx, uv.name, ic.iix, tc.defpos);
                            if (uv.alias != null)
                                nv += (_Alias, uv.alias);
                            cx.Replace(uv, nv);
                            q = (QuerySpecification)cx.obs[q.defpos];
                            if (nv is SqlCopy su && cx.obs[su.copyFrom] is SqlCopy)
                            {
                                var mgu = mg[su.copyFrom] ?? CTree<long, bool>.Empty;
                                var mgp = mg[p] ?? CTree<long, bool>.Empty;
                                mg = mg + (uv.defpos, mgu + (p, true))
                                    + (p, mgp + (su.copyFrom, true));
                                q += (Matching, mg);
                                q = (QuerySpecification)cx.Add(q);
                            }
                            vs += nv;
                            mp += (tc.defpos, true);
                        }
                    }
                    if (q.HasStar(cx))
                        for (var b = dm.rowType.First(); b != null && b.key()<ob.domain.display; b = b.Next())
                        {
                            var p = b.value();
                            var ci = cx.Inf(p); // for Table
                            var sc = cx.obs[p] as SqlValue; // for View
                            var u = cx.GetUid();
                            var sv = new SqlCopy(u, cx, ci?.name??sc.alias??sc.name, ic.iix, p);
                            cx.Add(sv);
                            vs += sv;
                            mp += (p, true);
                        }
                }
            }
            else
            {
                for (var b = cr.First(); b != null; b = b.Next())
                {
                    var c = b.value();
                    var tc = cx.obs[cx.defs[c]]
                        ?? throw new DBException("42112", c.ident);
                    var sv = new SqlCopy(c.iix, cx, c.ident, ic.iix, tc.defpos);
                    cx.Add(sv);
                    vs += sv;
                    mp += (tc.defpos, true);
                }
            }
            var d = vs.Length;
            for (var b = dm.rowType.First(); b != null && b.key()<ob.domain.display; b = b.Next())
            {
                var p = b.value();
                if (mp.Contains(p))
                    continue;
                var ci = cx.Inf(p);
                var sc = cx.obs[p] as SqlValue;
                sc = new SqlCopy(cx.GetUid(), cx, ci?.name??sc.alias??sc.name, ic.iix, p);
                cx.Add(sc);
                vs += sc;
            }
            dm = new Domain(Sqlx.TABLE,vs,d);
            de = _Max(de, ob.depth);
            return BTree<long, object>.Empty + (Name, ic.ident)
                   + (Target, ob.defpos) + (_Domain, dm)
                   + (Depth, de + 1);
        }
        static BTree<long,object> _Mem(long dp,Context cx,SqlCall ca,CList<long> cr=null)
        {
            var pc = (CallStatement)cx.obs[ca.call];
            var proc = (Procedure)cx.db.objects[pc.procdefpos];
            var disp = cr?.Length ?? proc.domain.Length;
            var s = CList<long>.Empty;
            var oi = cx.Inf(proc.defpos);
            for (var b = oi.domain.representation.First(); b != null; b = b.Next())
            {
                var ci = cx.Inf(b.key());
                cx.Add( new SqlRowSetCol(ci.defpos,ci, dp));
                s += ci.defpos;
            }
            cx.data += (dp,new ProcRowSet(dp, ca, cx));
            return BTree<long, object>.Empty
                + (Target,pc.procdefpos) + (Depth,1+ca.depth)
                + (_Domain,new Domain(Sqlx.ROW,cx,s,disp)) + (Name, proc.name);
        }
        static BTree<long,object> _Mem(long dp,Context cx, RowSet rs, string a)
        {
            cx.data += (dp, rs); 
            return _Mem(rs, a);
        }
        static BTree<long, object> _Mem(DBObject ob, string a)
        {
            return BTree<long, object>.Empty
                + (Target, ob.defpos) + (Depth, 1 + ob.depth)
                + (_Domain, ob.domain) + (Name, a);
        }
        internal override bool Knows(Context cx, long p)
        {
            for (var b = domain.rowType.First(); b != null; b = b.Next())
                if (b.value() == p)
                    return true;
            return false;
        }
        internal override TypedValue Eval(Context cx)
        {
            return cx.cursors[defpos];
        }
        internal override DBObject _Replace(Context cx, DBObject was, DBObject now)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (From)base._Replace(cx,was,now);
            var ch = (r!=this);
            if (cx._Replace(target,was,now) is Query so && so.defpos != r.target)
            {
                ch = true;
                r += (Target, so);
            }
            var ua = CTree<UpdateAssignment,bool>.Empty;
            for (var b = assig?.First(); b != null; b = b.Next())
                ua += (b.key().Replace(cx, was, now),true);
            if (ua != assig)
                r += (Assig, ua);
            if (ch)
                cx.Add(r);
            r = (From)New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Relocate(long dp)
        {
            return new From(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (From)base._Relocate(wr);
            r += (Assig, wr.Fix(assig));
            var tg = wr.Fix(target);
            if (tg != target)
                r += (Target, tg);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (From)base.Fix(cx);
            var na = cx.Fix(assig);
            if (assig != na)
                r += (Assig, na);
            var nt = cx.obuids[target] ?? target;
            if (nt != target)
                r += (Target, nt);
            return r;
        }
        internal override SqlValue ToSql(Ident id,Database db)
        {
            return new SqlTable(id.iix,this);
        }
        internal override bool Uses(Context cx,long t)
        {
            return target==t;
        }
        internal override RowSet RowSets(Context cx, CTree<long, RowSet.Finder> fi)
        {
            if (defpos == Static)
                return new TrivialRowSet(defpos,cx,new TRow(domain, cx.values),-1,fi);
            if (cx.data.Contains(defpos))
                return cx.data[defpos];
            cx.obs[target].RowSets(cx, this, fi);
            return cx.data[defpos];
        }
        /// <summary>
        /// Accessor: Check a new table check constraint
        /// </summary>
        /// <param name="tr">Transaction</param>
        /// <param name="c">The new Check constraint</param>
        internal void TableCheck(Transaction tr, PCheck c)
        {
            var cx = new Context(tr);
            var trs = new TableRowSet(cx,target,CTree<long,RowSet.Finder>.Empty,where);
            if (trs.First(cx) != null)
                throw new DBException("44000", c.check).ISO();
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            t = Scan(t, target, VIC.OK | VIC.OV);
            return base.Scan(t);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (defpos == _static.defpos) sb.Append(" STATIC");
            if (mem.Contains(_Alias)) { sb.Append(" Alias "); sb.Append(alias); }
            if (mem.Contains(Target)) { sb.Append(" Target="); sb.Append(Uid(target)); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// The interesting bit here is that if we have something like "insert into a(b,c) select d,e from f"
    /// the table-valued subquery silently gets its columns renamed to b,c and types coerced to match a, 
    /// and then the resulting columns get reordered to become candidate rows of a so that trigger processing
    /// etc can proceed.
    /// This is a bit more complex than "insert into a values(..." and requires some planning.
    /// The current approach is that in the above example domain is a's row type, nominaltype is for (b,c)
    /// and rows is a subquery before the renaming. 
    /// The renaming, reordering and coercion steps complicate the coding.
    /// </summary>
    internal class SqlInsert : Executable
    {
        internal const long
            Provenance = -155, //string
            Value = -156; // long RowSet
        internal long nuid => (long)(mem[TableExpression.Nuid] ?? -1L);
        internal long target => (long)(mem[From.Target] ?? -1L);
        /// <summary>
        /// Provenance information if supplied
        /// </summary>
        public string provenance => (string)mem[Provenance];
        public long value => (long)(mem[Value]??-1L);
        /// <summary>
        /// Constructor: an INSERT statement from the parser.
        /// </summary>
        /// <param name="cx">The parsing context</param>
        /// <param name="name">The name of the table to insert into</param>
        public SqlInsert(long dp,From fm,string prov, RowSet v) 
           : base(dp,BTree<long,object>.Empty + (From.Target,fm.target) 
                 +(TableExpression.Nuid,fm.defpos) + (Provenance, prov)+(Value,v.defpos))
        { }
        protected SqlInsert(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlInsert(defpos,m);
        }
        public static SqlInsert operator+(SqlInsert s,(long,object)x)
        {
            return new SqlInsert(s.defpos, s.mem + x);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = base._Replace(cx,so,sv);
            var tg = cx.Replace(target, so, sv);
            if (tg!=target)
                r += (From.Target, tg);
            r = (SqlInsert)New(cx,r.mem);
            cx.done += (defpos, r);
            return cx.Add(New(cx,r.mem));
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlInsert(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r =  (SqlInsert)base._Relocate(wr);
            r += (From.Target, wr.Fix(target));
            r += (Value, ((RowSet)wr.cx.data[value]._Relocate(wr)).defpos);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlInsert)base.Fix(cx);
            var tg = cx.obuids[target] ?? target;
            if (tg!=target)
                r += (From.Target, tg);
            var nv = cx.rsuids[value] ?? value;
            if (nv != value)
                r += (Value, nv);
            return r;
        }
        public override Context Obey(Context cx)
        {
            var tg = cx.data[nuid];
            cx = tg.Insert(cx,cx.data[value],provenance, classification);
            return cx;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Target: ");
            sb.Append(Uid(target));
            sb.Append(" Value: "); sb.Append(Uid(value));
            if (provenance != null)
            { sb.Append(" Provenance: "); sb.Append(provenance); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// QuerySearch is for DELETE and UPDATE 
    /// </summary>
    internal class QuerySearch : Executable
    {
        internal long target => (long)(mem[From.Target]??-1L);
        internal long nuid => (long)(mem[TableExpression.Nuid] ?? -1L);
        internal QuerySearch(long dp,Context cx,Ident ic,DBObject tb,Grant.Privilege how) 
            : this(Type.DeleteWhere,dp,cx,ic,tb,how)
            // detected for HttpService for DELETE verb
        { }
        protected QuerySearch(Type et, long dp, Context cx, Ident ic, DBObject tb,
            Grant.Privilege how, BList<Ident> cr = null)
            : this(et, dp, cx, 
                  (From)cx.Add(new From(ic, cx, tb, null, how,null,cr)),
                 tb, how)
        { }
        /// <summary>
        /// Constructor: a DELETE or UPDATE statement from the parser
        /// </summary>
        /// <param name="cx">The parsing context</param>
        protected QuerySearch(Type et,long dp,Context cx,From f,DBObject tb,
            Grant.Privilege how, CTree<UpdateAssignment,bool> ua=null)
            : base(dp,BTree<long, object>.Empty + (From.Target,f.target)
                  +(TableExpression.Nuid,f.defpos)
                  +(Depth,f.depth+1)+(_Type,et)+(Query.Assig,ua))
        {
            if (f.rowType.Length == 0)
                throw new DBException("2E111", cx.db.user, dp).Mix();
        }
        protected QuerySearch(long dp,BTree<long,object>m) :base(dp,m) { }
        public static QuerySearch operator+(QuerySearch q,(long,object)x)
        {
            return (QuerySearch)q.New(q.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new QuerySearch(defpos,m);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (QuerySearch)base._Replace(cx, so, sv);
            var tg = cx.Replace(target, so, sv);
            if (tg!=target)
                r += (From.Target, tg);
            r = (QuerySearch)New(cx, r.mem);
            cx.done += (defpos, r);
            return New(cx,r.mem);
        }
        internal override DBObject Relocate(long dp)
        {
            return new QuerySearch(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (QuerySearch)base._Relocate(wr);
            r += (From.Target, wr.Fix(target));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (QuerySearch)base.Fix(cx);
            var tg = cx.Fix(target);
            if (tg != target)
                r += (From.Target, tg);
            return r;
        }
        public override Context Obey(Context cx)
        {
            var rs = cx.data[nuid];
            return rs.Delete(cx,rs);
        }
        /// <summary>
        /// A readable version of the delete statement
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Target: "); sb.Append(Uid(target));
            return sb.ToString();
        }
    }
    /// <summary>
    /// Implement a searched UPDATE statement as a kind of QuerySearch
    /// </summary>
    internal class UpdateSearch : QuerySearch
    {
        /// <summary>
        /// Constructor: A searched UPDATE statement from the parser
        /// </summary>
        /// <param name="cx">The context</param>
        public UpdateSearch(long dp, Context cx, Ident ic, DBObject tb,
            Grant.Privilege how)
            : base(Type.UpdateWhere, dp, cx, ic, tb, how)
        {  }
        protected UpdateSearch(long dp, BTree<long, object> m) : base(dp, m) { }
        public static UpdateSearch operator+(UpdateSearch u,(long,object)x)
        {
            return (UpdateSearch)u.New(u.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new UpdateSearch(defpos,m);
        }
        public override Context Obey(Context cx)
        {
            var rs = cx.data[nuid];
            return rs.Update(cx,rs);
        }
        internal override DBObject Relocate(long dp)
        {
            return new UpdateSearch(dp, mem);
        }
    }
}