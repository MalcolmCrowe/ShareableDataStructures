using System;
using System.Collections.Generic;
using Pyrrho.Common;
using System.Text;
using Pyrrho.Level2;
using Pyrrho.Level3;
using System.CodeDom.Compiler;
using System.Configuration;
using System.Net;
using System.Net.Sockets;
using System.Data.SqlTypes;
using System.Runtime.InteropServices;
using System.CodeDom;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.
namespace Pyrrho.Level4
{
    /// <summary>
    /// A RowSet is the result of a stage of query processing. 
    /// We take responsibility for cx.values in the finder list.
    /// 
    /// RowSet is a subclass of TypedValue so inherits domain and a column ordering.
    /// RowSet Domains have fully specified representation and defpos matches the
    /// rowSet.
    /// TableRowSet, IndexRowSet and TransitionRowSet access physical columns
    /// and ignore the column ordering.
    /// Other RowSets must not contain any physical uids in their column uids
    /// or within their domain's representation.
    /// 
    /// RowSet defpos (uids) are carefully managed:
    /// SystemRowSet uids match the relevant System Table (thus a negative uid)
    /// TableRowSet uids match the table defining position
    /// IndexRowSet uids match the index defpos
    /// TransitionRowSet is somewhere on the heap (accessible transition and target contexts)
    /// EmptyRowSet uid is -1
    /// The following rowsets always have physical or lexical uids and
    /// rowtype and domain information comes from the syntax:
    /// TrivialRowSet, SelectedRowSet, JoinRowSet, MergeRowSet, 
    /// SelectRowSet, EvalRowSet, GroupRowSet, TableExpRowSet, ExplicitRowSet.
    /// All other rowSets get their defining position by cx.nextId++
    /// 
    /// IMMUTABLE
    /// </summary>
    internal abstract class RowSet : TypedValue
    {
        internal readonly long defpos; // uid see above
        internal readonly Domain domain;
        internal CList<long> rt => domain.rowType;
        internal readonly CList<long> keys; // defpos -1 unless same as RowSet
        internal readonly CList<long> rowOrder;
        internal readonly BTree<long, bool> where;
        internal readonly BTree<long, TypedValue> matches;
        internal readonly BTree<long, BTree<long, bool>> matching;
        internal readonly BTree<long, Finder> needed;
        internal readonly bool built;
        internal readonly GroupSpecification grouping;
        /// <summary>
        /// We use rowSet uid instead of actual RowSet because of order of construction
        /// </summary>
        internal readonly struct Finder 
        {
            public readonly long col;
            public readonly long rowSet; 
            public Finder(long c, long r) { col = c; rowSet = r; }
            public override string ToString()
            {
                return DBObject.Uid(rowSet) + "[" + DBObject.Uid(col) + "]";
            }
        }
        /// <summary>
        /// The finder maps the list of SqlValues whose values are in cursors,
        /// to a Finder.
        /// In a Context there may be several rowsets being calculated independently
        /// (e.g. a join of subqueries), and it can combine sources into a from association
        /// saying which rowset (and so which cursor) contains the current values.
        /// We could simply search along cx.data to pick the rowset we want.
        /// Before we compute a Cursor, we place the source finder in the  Context, 
        /// so that the evaluation can use values retrieved from the source cursors.
        /// </summary>
        internal readonly BTree<long, Finder> finder; // what we provide
        internal readonly int display;
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dp">The uid for the RowSet</param>
        /// <param name="rt">The way for a cursor to calculate a row</param>
        /// <param name="kt">The way for a cursor to calculate a key (by default same as rowType)</param>
        /// <param name="or">The ordering of rows in this rowset</param>
        /// <param name="wh">A set of boolean conditions on row values</param>
        /// <param name="ma">A filter for row column values</param>
        protected RowSet(long dp, Context cx, Domain dt,int ds = -1,
            BTree<long, Finder> fin = null, CList<long> kt = null,
            BTree<long, bool> wh = null, CList<long> or = null,
            BTree<long, TypedValue> ma = null,
            BTree<long, BTree<long, bool>> mg = null,
            GroupSpecification gp = null) : base(dt)
        {
            defpos = dp;
            domain = dt;
            keys = kt ?? rt;
            display = (ds > 0) ? ds : rt.Length;
            rowOrder = or ?? CList<long>.Empty;
            where = wh ?? BTree<long, bool>.Empty;
            matches = ma ?? BTree<long, TypedValue>.Empty;
            matching = mg ?? BTree<long, BTree<long, bool>>.Empty;
            grouping = gp;
            needed = null;
            fin = fin ?? BTree<long, Finder>.Empty;
            for (var b = rt.First(); b != null; b = b.Next())
            {
                var p = b.value();
                if (cx.obs[p] is SqlValue sv && sv.alias is string a
                    && cx.defs.Contains(a))
                    fin += (cx.defs[a].Item1, new Finder(p, defpos));
                fin += (p, new Finder(p, defpos));
            }
            finder = fin;
            cx.data += (dp, this);
            cx.val = this;
        }

        protected RowSet(long dp, Context cx, RowSet rs) 
            : this(dp, cx, rs, rs.rt, rs.keys) { }
        protected RowSet(Context cx,RowSet rs,BTree<long,Finder>nd, bool bt) 
            :base(rs.domain)
        {
            defpos = rs.defpos;
            domain = rs.domain;
            display = rs.display;
            finder = rs.finder;
            keys = rs.keys;
            rowOrder = rs.rowOrder;
            where = rs.where;
            matches = rs.matches;
            matching = rs.matching;
            grouping = rs.grouping;
            needed = nd;
            built = bt || nd == BTree<long, Finder>.Empty;
            cx.data += (rs.defpos, this);
            cx.val = this;
        }
        RowSet(long dp,Context cx,RowSet rs,CList<long> inf,CList<long> keyInf) :base(rs.dataType)
        {
            defpos = dp;
            domain = rs.domain + (Domain.RowType, inf);
            display = rs.display;
            finder = rs.finder;
            keys = keyInf;
            rowOrder = rs.rowOrder;
            where = rs.where;
            matches = rs.matches;
            matching = rs.matching;
            grouping = rs.grouping;
            needed = rs.needed;
            cx.data += (dp, this);
            cx.val = this;
        }
        protected RowSet(RowSet rs,long a,long b):base(rs.dataType)
        {
            defpos = rs.defpos;
            domain = rs.domain;
            display = rs.display;
            keys = rs.keys;
            rowOrder = rs.rowOrder;
            where = rs.where;
            matches = rs.matches;
            needed = rs.needed;
            var m = rs.matching;
            var ma = m[a] ?? BTree<long, bool>.Empty;
            var mb = m[b] ?? BTree<long, bool>.Empty;
            matching = m + (a, ma + (b, true)) + (b, mb + (a, true));
            grouping = rs.grouping;
        }
        internal abstract RowSet New(Context cx, BTree<long, Finder> nd, bool bt);
        internal abstract RowSet New(long a, long b);
        internal virtual bool TableColsOk => false;
        public static RowSet operator+(RowSet rs,(long,long)x)
        {
            return rs.New(x.Item1, x.Item2);
        }
        internal virtual int? Count => null; // number of rows is usually unknown
        public override bool IsNull => false;
        internal override object Val()
        {
            return this;
        }
        public override int _CompareTo(object obj)
        {
            throw new NotImplementedException();
        }
        internal virtual RowSet Build(Context cx,BTree<long,Finder>nd)
        {
            if (PyrrhoStart.StrategyMode)
                Strategy(0);
            return New(cx,nd,true);
        }
        /// <summary>
        /// Deal with RowSet prequisites (a kind of implementation of LATERAL).
        /// We can't calculate these earlier since the source rowsets haven't been constructed.
        /// Cursors will contain corresponding values of prerequisites.
        /// Once First()/Next() has been called, we need to see if they
        /// have been evaluated/changed. Either we can't or we must build this RowSet.
        /// </summary>
        /// <param name="was"></param>
        /// <param name="now"></param>
        /// <returns></returns>
        internal RowSet MaybeBuild(Context cx,BTree<long,TypedValue>was=null)
        {
            var r = this;
            // We cannot Build if Needs not met by now.
            if (was == null && needed == null)
            {
                var nd = BTree<long, Finder>.Empty;
                nd = cx.Needs(nd, this, rt);
                nd = cx.Needs(nd, this, keys);
                nd = cx.Needs(nd, this, rowOrder);
                nd = AllWheres(cx,nd);  
                nd = AllMatches(cx,nd); 
                r = r.New(cx, nd, false); // install nd immediately in case Build looks for it
                var bd = true;
                for (var b = nd.First(); b != null; b = b.Next())
                {
                    var p = b.key();
                    cx.from += (p,b.value()); // enable build
                    if (cx.obs[p].Eval(cx) == null)
                        bd = false;
                }
                if (bd) // all prerequisites are ok
                    r = r.Build(cx, nd);  // we can Build (will set built to true)
                return r;
            }
            for (var b = r.needed.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var fi = b.value();
                var v = cx.cursors[fi.rowSet]?[fi.col];
                if (v == null)
                    throw new PEException("PE188");
                var ov = was?[p];
                if (ov == null || ov.CompareTo(v) != 0)
                {
                    r = r.Build(cx, r.needed); // we must Build again
                //    cx.cursors += (r.defpos,r._First(cx));
                    return r;
                }
            }
            // Otherwise we don't need to
            return this;
        }
        public string NameFor(Context cx, int i)
        {
            var p = rt[i];
            var sv = cx.obs[p];
            var n = sv?.alias ?? (string)sv?.mem[Basis.Name];
            return cx.Inf(p)?.name ?? n??("Col"+i);
        }
        internal virtual RowSet Source(Context cx)
        {
            return null;
        }
        internal virtual bool Knows(Context cx,long rp)
        {
            return rp == defpos || (Source(cx) is RowSet rs && rs.Knows(cx,rp));
        }
        internal virtual BTree<long,Finder> AllWheres(Context cx,BTree<long,Finder> nd)
        {
            nd = cx.Needs(nd,this,where);
            if (Source(cx) is RowSet sce) 
                nd = cx.Needs(nd,this,sce.AllWheres(cx,nd));
            return nd;
        }
        internal virtual BTree<long,Finder> AllMatches(Context cx,BTree<long,Finder> nd)
        {
            nd = cx.Needs(nd,this,matches);
            if (Source(cx) is RowSet sce)
                nd = cx.Needs(nd,this,sce.AllMatches(cx,nd));
            return nd;
        }
        /// <summary>
        /// Test if the given source RowSet matches the requested ordering
        /// </summary>
        /// <returns>whether the orderings match</returns>
        protected bool SameOrder(Context cx,Query q,RowSet sce)
        {
            if (rowOrder == null || rowOrder.Length == 0)
                return true;
            return cx.SameRowType(q,rowOrder,sce?.rowOrder);
        }
        /// <summary>
        /// Compute schema information (for the client) for this row set.
        /// 
        /// </summary>
        /// <param name="flags">The column flags to be filled in</param>
        internal void Schema(Context cx, int[] flags)
        {
            int m = rt.Length;
            bool addFlags = true;
            var adds = new int[flags.Length];
            // see if we are going to add index flags stuff
            var j = 0;
            for (var ib = keys.First(); j<flags.Length && ib != null; 
                ib = ib.Next(), j++)
            {
                var found = false;
                for (var b = rt.First(); b != null; 
                    b = b.Next())
                    if (b.key() == ib.key())
                    {
                        adds[b.key()] = (j + 1) << 4;
                        found = true;
                        break;
                    }
                if (!found)
                    addFlags = false;
            }
            for (int i = 0; i < flags.Length; i++)
            {
                var cp = rt[i];
                var dc = dataType.representation[cp];
                if (dc.kind == Sqlx.SENSITIVE)
                    dc = dc.elType;
                flags[i] = dc.Typecode() + (addFlags ? adds[i] : 0);
                if (cx.db.objects[cp] is TableColumn tc)
                    flags[i] += ((tc.notNull) ? 0x100 : 0) +
                        ((tc.generated != GenerationRule.None) ? 0x200 : 0);
            }
        }
        /// <summary>
        /// Bookmarks are used to traverse rowsets
        /// </summary>
        /// <returns>a bookmark for the first row if any</returns>
        protected abstract Cursor _First(Context _cx);
        public virtual Cursor First(Context cx)
        {
            var rs = MaybeBuild(cx);
            if (!rs.built)
                throw new DBException("PE198");
            return rs._First(cx);
        }
        /// <summary>
        /// Find a bookmark for the given key
        /// </summary>
        /// <param name="key">a key</param>
        /// <returns>a bookmark or null if it is not there</returns>
        public virtual Cursor PositionAt(Context _cx,PRow key)
        {
            for (var bm = _First(_cx); bm!=null; bm=bm.Next(_cx))
            {
                var k = key;
                for (var b=keys.First();b!=null;b=b.Next())
                    if (_cx.obs[b.value()].Eval(_cx).CompareTo(k._head) != 0)
                        goto next;
                return bm;
                next:;
            }
            return null;
        }
        internal virtual void _Strategy(StringBuilder sb,int indent)
        {
            Conds(sb, where, " where ");
            Matches(sb);
        }
        internal void Conds(StringBuilder sb,BTree<long,bool> conds,string cm)
        {
            for (var b = conds.First(); b != null; b = b.Next())
            {
                sb.Append(cm);
                sb.Append(DBObject.Uid(b.key()));
                cm = " and ";
            }
        }
        internal void Matches(StringBuilder sb)
        {
            var cm = " match ";
            for (var b = matches.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(DBObject.Uid(b.key())); sb.Append("=");
                sb.Append(b.value());
            }
        }
        internal void Strategy(int indent)
        {
        }
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            sb.Append(' ');sb.Append(DBObject.Uid(defpos));
            var cm = "(";
            for (var b = rt.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(DBObject.Uid(b.value()));
            }
            sb.Append(")"); 
            if (keys!=rt && keys.Count!=0)
            {
                cm = " key (";
                for (var b = keys.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(DBObject.Uid(b.value()));
                }
                sb.Append(")");
            }
            cm = "(";
            sb.Append(" Finder: ");
            for (var b=finder.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = "],";
                sb.Append(DBObject.Uid(b.key()));
                var f = b.value();
                sb.Append("="); sb.Append(DBObject.Uid(f.rowSet));
                sb.Append('['); sb.Append(DBObject.Uid(f.col));
            }
            sb.Append("])");
            return sb.ToString();
        }
        internal virtual string ToString(Context cx,int n)
        {
            return ToString();
        }
    }
    /// <summary>
    /// 1. Throughout the code RowSets are navigated using Cursors instead of
    /// Enumerators of any sort. The great advantage of bookmarks generally is
    /// that they are immutable. (E.g. the ABookmark classes always refer to a
    /// snapshot of the tree at the time of constructing the bookmark.)
    /// 2. The Rowset field is readonly but NOT immutable as it contains the Context _cx.
    /// _cx.values is updated to provide shortcuts to current values.
    /// 3. The TypedValues obtained by Value(..) are mostly immutable also, but
    /// updates to row columns (e.g. during trigger operations) are managed by 
    /// allowing the Cursor to override the usual results of 
    /// evaluating a row column: this is needed in (at least) the following 
    /// circumstances (a) UPDATE (b) INOUT and OUT parameters that are passed
    /// column references (c) triggers.
    /// 4. In view of the anxieties of 3. these overrides are explicit
    /// in the Cursor itself.
    /// </summary>
    internal abstract class Cursor : TRow
    {
        public readonly long _rowsetpos;
        public readonly int _pos;
        public readonly long _defpos;
        public readonly int display;
        public readonly BTree<long, TypedValue> _needed;
        public Cursor(Context _cx,RowSet rs,int pos,long defpos,TRow rw,BTree<long,object>m=null)
            :base(rs.domain,rw.values)
        {
            _rowsetpos = rs.defpos;
            _pos = pos;
            _defpos = defpos;
            display = rs.display;
            var nd = BTree<long, TypedValue>.Empty;
            for (var b=rs.needed.First();b!=null;b=b.Next())
            {
                var fi = b.value();
                nd += (b.key(), _cx.cursors[fi.rowSet][fi.col]);
            }
            _needed = nd;
            _cx.cursors += (rs.defpos, this);
        }
        public Cursor(Context _cx, RowSet rs, int pos, long defpos, TypedValue[] vs,
            BTree<long,object>m = null)
            : base(rs, vs)
        {
            _rowsetpos = rs.defpos;
            _pos = pos;
            _defpos = defpos;
            display = rs.display;
            var nd = BTree<long, TypedValue>.Empty;
            for (var b = rs.needed.First(); b != null; b = b.Next())
            {
                var fi = b.value();
                nd += (b.key(), _cx.cursors[fi.rowSet][fi.col]);
            }
            _needed = nd;
            _cx.cursors += (rs.defpos, this);
        }
        // a more detailed version for trigger-side transition cursors
        protected Cursor(Context cx,long rd,int pos,long defpos,TRow rw,BTree<long,object>m=null)
            :base(cx.data[rd].domain,rw.values)
        {
            _rowsetpos = rd;
            _pos = pos;
            _defpos = defpos;
            cx.cursors += (rd, this);
        }
        public Cursor(Context _cx, long rp, int pos, long defpos, Domain dt, TRow rw,
            BTree<long,object>m=null) : base(dt,rw.values)
        {
            _rowsetpos = rp;
            _pos = pos;
            _defpos = defpos;
            _cx.cursors += (rp, this);
        }
        protected Cursor(Cursor cu,Context cx,long p,TypedValue v) 
            :base (cu.dataType,cu.values+(p,v))
        {
            _rowsetpos = cu._rowsetpos;
            _pos = cu._pos;
            _defpos = cu._defpos;
            display = cu.display;
            cx.cursors += (cu._rowsetpos, this);
        }
        public static Cursor operator+(Cursor cu,(Context,long,TypedValue)x)
        {
            return cu.New(x.Item1,x.Item2,x.Item3);
        }
        protected abstract Cursor New(Context cx,long p, TypedValue v);
        public virtual Cursor Next(Context cx)
        {
    //        var rs = cx.data[_rowsetpos];
    //        rs.MaybeBuild(cx,_needed);
            return _Next(cx);
        }
        protected abstract Cursor _Next(Context cx);
        internal bool Matches(Context cx)
        {
            var rs = cx.data[_rowsetpos];
            for (var b = rs.matches.First(); b != null; b = b.Next())
                if (cx.obs[b.key()].Eval(cx).CompareTo(b.value()) != 0)
                    return false;
            for (var b = rs.where.First(); b != null; b = b.Next())
                if (cx.obs[b.key()].Eval(cx) != TBool.True)
                    return false;
            return true;
        }
        /// <summary>
        /// These methods are for join processing
        /// </summary>
        /// <returns></returns>
        public virtual MTreeBookmark Mb()
        {
            return null; // throw new PEException("PE543");
        }
        public virtual Cursor ResetToTiesStart(Context _cx, MTreeBookmark mb)
        {
            return null; // throw new PEException("PE544");
        }
        internal abstract TableRow Rec();
        public override int _CompareTo(object obj)
        {
            throw new NotImplementedException();
        }
        internal override object Val()
        {
            throw new NotImplementedException();
        }
        internal string NameFor(Context cx,int i)
        {
            var rs = cx.data[_rowsetpos];
            var p = rs.rt[i];
            var ob = (SqlValue)cx.obs[p];
            return ob.name;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' ');sb.Append(DBObject.Uid(_rowsetpos));
            return sb.ToString();
        }
    }
    internal class TrivialRowSet: RowSet
    {
        internal readonly TRow row;
        internal readonly long rc = -1;
        internal readonly TrivialCursor here;
        internal TrivialRowSet(long dp, Context cx, CList<long>cols, TRow r, long rc, BTree<long,Finder> fi)
            : base(dp, cx, new Domain(Sqlx.ROW,cx,cols),cols.Length,fi)
        {
            row = r ?? new TRow(ObInfo.Any);
            here = new TrivialCursor(cx, this, 0, rc);
            cx.data+=(defpos,this);
        }
        internal TrivialRowSet(long dp, Context cx, Domain dt, TRow r, long rc,
            BTree<long,Finder>fi)
            : base(dp, cx, dt, r.Length, fi)
        {
            row = r ?? new TRow(dt,BTree<long,TypedValue>.Empty);
            here = new TrivialCursor(cx, this, 0, rc);
            cx.data += (defpos, this);
        }
        internal TrivialRowSet(long dp,Context cx, Record rec) 
            : this(dp,cx, 
                  new Domain(Sqlx.ROW,cx,cx.Cols(rec.tabledefpos)), 
                  new TRow(((Table)cx.db.objects[rec.tabledefpos]).domain, rec.fields), 
                  rec.defpos, BTree<long,Finder>.Empty)
        { }
        protected TrivialRowSet(TrivialRowSet rs, long a, long b) : base(rs, a, b)
        {
            row = rs.row;
            rc = rs.rc;
            here = rs.here;
        }
        internal override RowSet New(long a, long b)
        {
            return new TrivialRowSet(this, a, b);
        }
        internal override RowSet New(Context cx,BTree<long,Finder> nd,bool bt)
        {
            return new TrivialRowSet(defpos,cx,rt,row,rc,finder);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Trivial ");
            sb.Append(row.ToString());
            base._Strategy(sb, indent);
        }
        internal override int? Count => 1;

        public override bool IsNull => throw new NotImplementedException();

        internal override bool TableColsOk => true;

        protected override Cursor _First(Context _cx)
        {
            return here;
        }
        public override Cursor PositionAt(Context _cx,PRow key)
        {
            for (int i = 0; key != null; i++, key = key._tail)
                if (row[i].CompareTo(key._head) != 0)
                    return null;
            return here;
        }

        internal class TrivialCursor : Cursor
        {
            readonly TrivialRowSet trs;
            internal TrivialCursor(Context _cx, TrivialRowSet t,long d,long r) 
                :base(_cx,t,0,d,t.row)
            {
                trs = t;
            }
            TrivialCursor(TrivialCursor cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v) 
            {
                trs = cu.trs;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new TrivialCursor(this, cx, p, v);
            }
            protected override Cursor _Next(Context _cx)
            {
                return null;
            }
            internal override TableRow Rec()
            {
                return null;
            }
        }
    }
    /// <summary>
    /// A rowset consisting of selected Columns from another rowset (e.g. SELECT A,B FROM C).
    /// NB we assume that the rowtype contains only simple SqlCopy expressions
    /// </summary>
    internal class SelectedRowSet : RowSet
    {
        internal readonly BList<long> rowType;
        internal readonly long source;
        // qSmap if it existed would be the same as this.finder
        internal readonly BTree<long, Finder> sQmap;
        internal readonly BTree<long, DBObject> _obs;
        internal readonly Domain _domain;
        /// <summary>
        /// This constructor builds a rowset for the given Table
        /// directly using its defpos, rowType, ordering, where and match info.
        /// Suggestion here is to use the source keyType. Maybe the source ordering too?
        /// </summary>
        internal SelectedRowSet(Context cx,Query q,RowSet r,BTree<long,Finder> fi)
            :base(q.defpos, cx, q.domain, q.display,_Fin(cx,q,fi),null,q.where,
                 q.ordSpec,q.matches)
        {
            source = r.defpos;
            rowType = q.rowType;
            var sq = BTree<long, Finder>.Empty;
            for (var b=rowType.First();b!=null;b=b.Next())
            {
                var p = b.value();
                if (cx.obs[p] is SqlCopy sc)
                {
                    var s = sc.copyFrom;
                    sq += (p, new Finder(s,source));
                }
            }
            sQmap = sq;
            _obs = cx.obs;
            _domain = q.domain;
        }
        SelectedRowSet(Context cx, SelectedRowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {
            rowType = rs.rowType;
            source = rs.source;
            sQmap = rs.sQmap;
            _obs = rs._obs;
            _domain = rs._domain;
        }
        protected SelectedRowSet(SelectedRowSet rs, long a, long b) : base(rs, a, b)
        {
            rowType = rs.rowType;
            source = rs.source;
            sQmap = rs.sQmap;
            _obs = rs._obs;
            _domain = rs._domain;
        }
        static BTree<long, Finder> _Fin(Context cx, Query q, BTree<long,Finder>fi)
        {
            for (var b = q.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                if (cx.obs[p] is SqlCopy sc)
                    p = sc.copyFrom;
                fi += (b.value(), new Finder(p, q.defpos));
            }
            return fi;
        } 
        internal override RowSet New(long a, long b)
        {
            return new SelectedRowSet(this, a, b);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Selected ");
            sb.Append(DBObject.Uid(defpos));
            base._Strategy(sb, indent);
        }
        internal override RowSet New(Context cx,BTree<long,Finder> nd,bool bt)
        {
            return new SelectedRowSet(cx,this,nd,bt);
        }
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        internal override RowSet Build(Context cx, BTree<long, Finder> nd)
        {
            for (var i=0; i<rowType.Length;i++)
                cx.obs[rowType[i]]?.Build(cx,this);
            return (nd==needed && built)?this:New(cx,nd,true);
        }
        protected override Cursor _First(Context _cx)
        {
            return SelectedCursor.New(_cx,this);
        }
        public override Cursor PositionAt(Context cx, PRow key)
        {
            if (cx.data[source] is IndexRowSet irs)
                return new SelectedCursor(cx, this, IndexRowSet.IndexCursor.New(cx, irs, key),0);
            return base.PositionAt(cx, key);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = " sQmap: (";
            for (var b=sQmap.First();b!=null;b=b.Next())
            {
                var f = b.value();
                sb.Append(cm); cm = "],";
                sb.Append(DBObject.Uid(b.key()));
                sb.Append("=");sb.Append(DBObject.Uid(f.rowSet));
                sb.Append('[');sb.Append(DBObject.Uid(f.col));
            }
            sb.Append("])");
            sb.Append(" Source: "); sb.Append(DBObject.Uid(source));
            return sb.ToString();
        }
        internal class SelectedCursor : Cursor
        {
            readonly SelectedRowSet _srs;
            internal readonly Cursor _bmk; // for rIn
            internal SelectedCursor(Context _cx,SelectedRowSet srs, Cursor bmk, int pos) 
                : base(_cx,srs, pos, bmk._defpos, new TRow(srs.domain,srs.sQmap,bmk.values)) 
            {
                _bmk = bmk;
                _srs = srs;
            }
            SelectedCursor(SelectedCursor cu,Context cx,long p,TypedValue v)
                :base(cu,cx,cu._srs.sQmap[p].col,v)
            {
                _bmk = cu._bmk;
                _srs = cu._srs;
            }
            internal static SelectedCursor New(Context cx,SelectedRowSet srs)
            {
                var ox = cx.from;
                var sce = srs.Source(cx);
                cx.from += sce.finder; 
                for (var bmk = sce.First(cx); bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new SelectedCursor(cx,srs, bmk, 0);
                    if (rb.Matches(cx))
                    {
                        cx.from = ox;
                        return rb;
                    }
                }
                cx.from = ox;
                return null;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new SelectedCursor(this, cx, p, v);
            }
            protected override Cursor _Next(Context _cx)
            {
                var ox = _cx.from;
                _cx.from += _srs.finder; // just for SelectedRowSet
                for (var bmk = _bmk.Next(_cx); bmk != null; bmk = bmk.Next(_cx))
                {
                    var rb = new SelectedCursor(_cx,_srs, bmk, _pos + 1);
                    if (rb.Matches(_cx))
                    {
                        _cx.from = ox;
                        return rb;
                    }
                }
                _cx.from = ox;
                return null;
            }
            internal override TableRow Rec()
            {
                return _bmk.Rec();
            }
        }
    }
    internal class SelectRowSet : RowSet
    {
        internal readonly long source;
        internal readonly QuerySpecification qry;
        /// <summary>
        /// This constructor builds a rowset for the given QuerySpec
        /// directly using its defpos, rowType, ordering, where and match info.
        /// Note we cannot assume that columns are simple SqlCopy.
        /// Suggestion here is to use the source keyType. Maybe the source ordering too?
        /// </summary>
        internal SelectRowSet(Context cx, QuerySpecification q, RowSet r)
            : base(q.defpos, cx, q.domain, q.display, r.finder, null, q.where, q.ordSpec, 
                  q.matches,q.matching)
        {
            source = r.defpos;
            qry = q;
        }
        protected SelectRowSet(SelectRowSet rs, long a, long b) : base(rs, a, b)
        {
            source = rs.source;
            qry = rs.qry;
        }
        SelectRowSet(Context cx, SelectRowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {
            source = rs.source;
            qry = rs.qry;
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd,bool bt)
        {
            return new SelectRowSet(cx, this, nd, bt);
        }
        internal override RowSet New(long a, long b)
        {
            return new SelectRowSet(this, a, b);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Select ");
            sb.Append(DBObject.Uid(defpos));
            base._Strategy(sb, indent);
        }
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        internal override RowSet Build(Context cx,BTree<long,Finder> nd)
        {
            for (var i=0;i<qry.rowType.Length;i++)
                cx.obs[qry.rowType[i]].Build(cx, this);
            return (nd==needed && built)?this:New(cx,nd,true);
        }
        protected override Cursor _First(Context _cx)
        {
            return SelectCursor.New(_cx, this);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Source: "); sb.Append(DBObject.Uid(source));
            return sb.ToString();
        }
        internal class SelectCursor : Cursor
        {
            readonly SelectRowSet _srs;
            readonly Cursor _bmk; // for rIn, not used directly for Eval
            SelectCursor(Context _cx, SelectRowSet srs, Cursor bmk, int pos)
                : base(_cx, srs, pos, bmk._defpos, _Row(_cx, bmk, srs))
            {
                _bmk = bmk;
                _srs = srs;
            }
            SelectCursor(SelectCursor cu,Context cx,long p,TypedValue v):base(cu,cx,p,v)
            {
                _bmk = cu._bmk;
                _srs = cu._srs;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new SelectCursor(this, cx,p, v);
            }
            static TRow _Row(Context cx, Cursor bmk, SelectRowSet srs)
            {
                var ox = cx.from;
                cx.copy = srs.matching;
                cx.from += cx.data[srs.source].finder;
                var vs = BTree<long,TypedValue>.Empty;
                for (var b = srs.qry.rowType.First(); b != null; b = b.Next())
                {
                    var s = cx.obs[b.value()]; 
                    var v = s?.Eval(cx)??TNull.Value;
                    if (v == TNull.Value && bmk[b.value()] is TypedValue tv 
                        && tv != TNull.Value) 
                        // tv would be the right value but something has gone wrong
                        throw new PEException("PE788");
                    vs += (b.value(),v);
                }
                cx.from = ox;
                return new TRow(srs.domain, vs);
            }
            internal static SelectCursor New(Context cx, SelectRowSet srs)
            {
                for (var bmk = cx.data[srs.source].First(cx); bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new SelectCursor(cx, srs, bmk, 0);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor _Next(Context cx)
            {
                for (var bmk = _bmk.Next(cx); bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new SelectCursor(cx, _srs, bmk, _pos + 1);
                    for (var b = _srs.dataType.representation.First(); b != null; b = b.Next())
                        ((SqlValue)cx.obs[b.key()]).OnRow(cx,rb);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            internal override TableRow Rec()
            {
                return _bmk.Rec();
            }
        }
    }

    internal class EvalRowSet : RowSet
    {
        /// <summary>
        /// The RowSet providing the data for aggregation. 
        /// </summary>
		internal readonly long source;
        /// <summary>
        /// The having search condition for the aggregation
        /// </summary>
		internal readonly BTree<long,bool> having;
        internal readonly TRow row;
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        /// <summary>
        /// Constructor: Build a rowSet that aggregates data from a given source
        /// </summary>
        /// <param name="rs">The source data</param>
        /// <param name="h">The having condition</param>
		public EvalRowSet(Context cx, QuerySpecification q, RowSet rs)
            : base(q.defpos, cx, q.domain, q.display, rs.finder, null, null,
                  q.ordSpec, q.matches, q.matching)
        {
            source = rs.defpos;
            having = q.where;
        }
        protected EvalRowSet(EvalRowSet rs, long a, long b) : base(rs, a, b)
        {
            source = rs.source;
            having = rs.having;
            row = rs.row;
        }
        protected EvalRowSet(Context cx,EvalRowSet rs,BTree<long,Finder> nd,TRow rw) 
            :base(cx,rs,nd,true)
        {
            source = rs.source;
            having = rs.having;
            row = rw;
            cx.values += (defpos, rw);
        }
        internal override RowSet New(long a, long b)
        {
            return new EvalRowSet(this, a, b);
        }
        EvalRowSet(Context cx, EvalRowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {
            source = rs.source;
            having = rs.having;
            row = rs.row;
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new EvalRowSet(cx, this, nd, bt);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Eval ");
            sb.Append(rt);
            var cm = " having ";
            for (var b=having.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = " and ";
                sb.Append(b.value().ToString());
            }
            base._Strategy(sb, indent);
        }
        protected override Cursor _First(Context _cx)
        {
            return new EvalBookmark(_cx,this);
        }

        internal override RowSet Build(Context _cx,BTree<long,Finder> nd)
        {
            var tg = BTree<long, Register>.Empty;
            var cx = new Context(_cx);
            cx.copy = matching;
            var sce = cx.data[source];
            cx.from += sce.finder;
            var oi = rt;
            var k = new TRow(domain,BTree<long,TypedValue>.Empty);
            cx.data += (defpos, this);
            for (var b=rt.First(); b!=null; b=b.Next())
                tg = ((SqlValue)_cx.obs[b.value()]).StartCounter(cx,this,tg);
            var ebm = sce.First(cx);
            if (ebm != null)
            {
                for (; ebm != null; ebm = ebm.Next(cx))
                    if ((!ebm.IsNull) && Query.Eval(having,cx))
                        for (var b = rt.First(); b != null; b = b.Next())
                            tg = ((SqlValue)_cx.obs[b.value()]).AddIn(cx,ebm,tg);
            }
            var cols = oi;
            var vs = BTree<long,TypedValue>.Empty;
            cx.funcs = tg;
            for (int i = 0; i < cols.Length; i++)
            {
                var s = cols[i];
                vs += (s,_cx.obs[s].Eval(cx));
            }
            return new EvalRowSet(_cx,this,nd,new TRow(domain,vs));
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Source: "); sb.Append(DBObject.Uid(source));
            return sb.ToString();
        }
        internal class EvalBookmark : Cursor
        {
            readonly EvalRowSet _ers;
            internal EvalBookmark(Context _cx, EvalRowSet ers) 
                : base(_cx, ers, 0, 0, ers.row)
            {
                _ers = ers;
            }
            EvalBookmark(EvalBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v) 
            {
                _ers = cu._ers;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new EvalBookmark(this,cx,p,v);
            }
            protected override Cursor _Next(Context _cx)
            {
                return null; // just one row in the rowset
            }
            internal override TableRow Rec()
            {
                return null;
            }
        }
    }

    /// <summary>
    /// A TableRowSet consists of all of the *accessible* TableColumns from a Table
    /// </summary>
    internal class TableRowSet : RowSet
    {
        internal readonly long tabledefpos;
        internal readonly int? count;
        /// <summary>
        /// Constructor: a rowset defined by a base table without a primary key.
        /// Independent of role, user, command.
        /// Context must have a suitable tr field
        /// </summary>
        internal TableRowSet(Context cx, long t,BTree<long,Finder>fi)
            : base(t, cx, cx.Inf(t).domain,-1,fi)
        {
            tabledefpos = t;
            count = (int?)(cx.db.objects[t] as Table)?.tableRows.Count;
        }
        TableRowSet(long dp,Context cx,TableRowSet trs):base(dp,cx,trs)
        {
            tabledefpos = trs.tabledefpos;
            count = trs.count;
        }
        protected TableRowSet(TableRowSet rs, long a, long b) : base(rs, a, b)
        {
            tabledefpos = rs.tabledefpos;
            count = rs.count;
        }
        internal override RowSet New(long a, long b)
        {
            return new TableRowSet(this, a, b);
        }
        TableRowSet(Context cx, TableRowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {
            tabledefpos = rs.tabledefpos;
            count = rs.count;
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new TableRowSet(cx, this, nd, bt);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("TableRowSet ");
            sb.Append(DBObject.Uid(tabledefpos));
            base._Strategy(sb, indent);
        }
        internal override bool TableColsOk => true;
        internal override int? Count => count;
        protected override Cursor _First(Context _cx)
        {
            return TableCursor.New(_cx,this);
        }
        internal class TableCursor : Cursor
        {
            internal readonly Table _table;
            internal readonly TableRowSet _trs;
            internal readonly ABookmark<long, TableRow> _bmk;
            protected TableCursor(Context _cx, TableRowSet trs, Table tb, int pos, 
                ABookmark<long, TableRow> bmk) 
                : base(_cx,trs.defpos, pos, bmk.key(), _Row(trs,bmk.value()))
            {
                _bmk = bmk; _table = tb; _trs = trs;
            }
            TableCursor(TableCursor cu,Context cx, long p,TypedValue v) :base(cu,cx,p,v)
            {
                _bmk = cu._bmk; _table = cu._table; _trs = cu._trs;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new TableCursor(this, cx, p, v);
            }
            static TRow _Row(TableRowSet trs,TableRow rw)
            {
                var vs = BTree<long, TypedValue>.Empty;
                for (var b=trs.dataType.representation.First();b!=null;b=b.Next())
                {
                    var p = b.key();
                    vs += (p, rw.vals[p]);
                }
                return new TRow(trs.dataType, vs);
            }
            internal static TableCursor New(Context _cx, TableRowSet trs)
            {
                var table = _cx.db.objects[trs.tabledefpos] as Table;
                for (var b = table.tableRows.First(); b != null; b = b.Next())
                {
                    var rec = b.value();
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) &&
                        _cx.db.user != null && _cx.db.user.defpos != table.definer
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new TableCursor(_cx, trs, table, 0, b);
                }
                return null;
            }
            protected override Cursor _Next(Context _cx)
            {
                var bmk = _bmk;
                var rec = (TableRow)_bmk.value();
                var table = _table;
                for (;;) 
                {
                    if (bmk != null)
                        bmk = bmk.Next();
                    if (bmk == null)
                        return null;
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) && 
                        _cx.db.user.defpos!=table.definer 
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new TableCursor(_cx,_trs,_table, _pos + 1, bmk);
                }
            }
            internal override TableRow Rec()
            {
                return _bmk.value();
            }
        }
    }
    /// <summary>
    /// A RowSet defined by an Index (e.g. the primary key for a table)
    /// </summary>
    internal class IndexRowSet : RowSet
    {
        internal readonly Table table;
        /// <summary>
        /// The Index to use
        /// </summary>
        internal readonly Index index;
        internal readonly PRow filter;
        /// <summary>
        /// Constructor: A rowset for a table using a given index. 
        /// Unusally, the rowSet defpos is in the index's defining position,
        /// as the INRS is independent of role, user, command.
        /// Context must have a suitable tr field.
        /// </summary>
        /// <param name="f">the from part</param>
        /// <param name="x">the index</param>
        internal IndexRowSet(Context cx, Table tb, Index x, PRow filt, BTree<long,Finder>fi) 
            : base(x.defpos,cx,cx.Inf(tb.defpos).domain,-1,fi)
        {
            table = tb;
            index = x;
            filter = filt;
        }
        IndexRowSet(Context cx,IndexRowSet irs) :base(irs.defpos,cx,irs)
        {
            table = irs.table;
            index = irs.index;
            filter = irs.filter;
        }
        protected IndexRowSet(IndexRowSet irs, long a, long b) : base(irs, a, b)
        {
            table = irs.table;
            index = irs.index;
            filter = irs.filter;
        }
        internal override RowSet New(long a, long b)
        {
            return new IndexRowSet(this, a, b);
        }
        IndexRowSet(Context cx, IndexRowSet irs, BTree<long, Finder> nd,bool bt) 
            : base(cx, irs, nd, bt) 
        {
            table = irs.table;
            index = irs.index;
            filter = irs.filter;
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new IndexRowSet(cx, this, nd, bt);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("IndexRowSet ");
            sb.Append(table);
            base._Strategy(sb, indent);
        }
        internal override int? Count => (int?)index.rows.Count;
        internal override bool TableColsOk => true;
        protected override Cursor _First(Context _cx)
        {
            return IndexCursor.New(_cx,this);
        }
        /// <summary>
        /// We assume the key matches our key type, and that the filter is null
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public override Cursor PositionAt(Context _cx,PRow key)
        {
            return IndexCursor.New(_cx,this, key);
        }
        internal class IndexCursor : Cursor
        {
            internal readonly IndexRowSet _irs;
            internal readonly MTreeBookmark _bmk;
            internal readonly TableRow _rec;
            internal readonly PRow _key;
            IndexCursor(Context _cx, IndexRowSet irs, int pos, MTreeBookmark bmk, TableRow trw,
                PRow key=null)
                : base(_cx, irs.defpos, pos, trw.defpos, irs.dataType,
                      new TRow(irs.domain, trw.vals))
            {
                _bmk = bmk; _irs = irs; _rec = trw; _key = key;
            }
            IndexCursor(IndexCursor cu,Context cx,long p,TypedValue v):base(cu,cx,p,v)
            {
                _bmk = cu._bmk; _irs = cu._irs; _rec = cu._rec; _key = cu._key;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new IndexCursor(this, cx, p, v);
            }
            public override MTreeBookmark Mb()
            {
                return _bmk;
            }
            internal override TableRow Rec()
            {
                return _rec;
            }
            public override Cursor ResetToTiesStart(Context _cx, MTreeBookmark mb)
            {
                return new IndexCursor(_cx,_irs, _pos + 1, mb, _irs.table.tableRows[mb.Value().Value]);
            }
            internal static IndexCursor New(Context _cx,IndexRowSet irs, PRow key = null)
            {
                var _irs = irs;
                var table = _irs.table;
                for (var bmk = irs.index.rows.PositionAt(key ?? irs.filter); bmk != null;
                    bmk = bmk.Next())
                {
                    var iq = bmk.Value();
                    if (!iq.HasValue)
                        continue;
                    var rec = table.tableRows[iq.Value];
                    if (rec == null || (table.enforcement.HasFlag(Grant.Privilege.Select)
                        && _cx.db.user.defpos != table.definer
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification)))
                        continue;
                    return new IndexCursor(_cx,_irs, 0, bmk, rec, key??irs.filter);
                }
                return null;
            }
            protected override Cursor _Next(Context _cx)
            {
                var bmk = _bmk;
                var _table = _irs.table;
                for (; ; )
                {
                    bmk = bmk.Next();
                    if (bmk == null)
                        return null;
                    if (!bmk.Value().HasValue)
                        continue;
                    var rec = _table.tableRows[bmk.Value().Value];
                    if (_table.enforcement.HasFlag(Grant.Privilege.Select)
                        && _cx.db.user.defpos != _table.definer
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new IndexCursor(_cx, _irs, _pos + 1, bmk, rec);
                }
            }
        }
    }

    /// <summary>
    /// A rowset for distinct values
    /// </summary>
    internal class DistinctRowSet : RowSet
    {
        readonly MTree mtree = null;
        internal readonly long source;
        internal readonly BTree<int, TRow> rows = BTree<int,TRow>.Empty;
        /// <summary>
        /// constructor: a distinct rowset
        /// </summary>
        /// <param name="r">a source rowset</param>
        internal DistinctRowSet(Context _cx,RowSet r) 
            : base(_cx.GetUid(),_cx,r.dataType,r.display,r.finder,r.keys,r.where)
        {
            source = r.defpos;
        }
        protected DistinctRowSet(Context cx,DistinctRowSet rs,BTree<long,Finder> nd,MTree mt) 
            :base(cx,rs,nd,true)
        {
            source = rs.source;
            mtree = mt;
        }
        protected DistinctRowSet(DistinctRowSet rs,long a,long b):base(rs,a,b)
        {
            source = rs.source;
            mtree = rs.mtree;
        }
        internal override RowSet New(long a, long b)
        {
            return new DistinctRowSet(this,a,b);
        }
        DistinctRowSet(Context cx, DistinctRowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {
            source = rs.source;
            mtree = rs.mtree;
        }
        internal override RowSet New(Context cx, BTree<long,Finder> nd, bool bt)
        {
            return new DistinctRowSet(cx, this, nd, bt);
        }
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Distinct");
            base._Strategy(sb, indent);
        }
        internal override RowSet Build(Context cx,BTree<long,Finder> nd)
        {
            var ds = BTree<long,DBObject>.Empty;
            var sce = cx.data[source];
            for (var b = sce.keys.First(); b != null; b = b.Next())
            {
                var sv = (SqlValue)cx.obs[b.value()];
                ds += (sv.defpos,sv);
            }
            var mt = new MTree(new TreeInfo(sce.keys, ds, TreeBehaviour.Allow, TreeBehaviour.Allow));
            var vs = BList<TypedValue>.Empty;
            var rs = BTree<int, TRow>.Empty;
            for (var a = sce.First(cx); a != null; a = a.Next(cx))
            {
                for (var ti = mt.info; ti != null; ti = ti.tail)
                    vs+= a[ti.head];
                MTree.Add(ref mt, new PRow(vs), 0);
                rs += ((int)rs.Count, a);
            }
            return new DistinctRowSet(cx,this,nd,mt);
        }

        protected override Cursor _First(Context _cx)
        {
            return DistinctCursor.New(_cx,this);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Source: "); sb.Append(DBObject.Uid(source));
            return sb.ToString();
        }
        internal class DistinctCursor : Cursor
        {
            readonly DistinctRowSet _drs;
            readonly MTreeBookmark _bmk;
            DistinctCursor(Context cx,DistinctRowSet drs,int pos,MTreeBookmark bmk) 
                :base(cx,drs,pos,0,drs.rows[(int)bmk.Value().Value])
            {
                _bmk = bmk;
                _drs = drs;
            }
            internal static DistinctCursor New(Context cx,DistinctRowSet drs)
            {
                var ox = cx.from;
                cx.from += cx.data[drs.source].finder;
                for (var bmk = drs.mtree.First(); bmk != null; bmk = bmk.Next() as MTreeBookmark)
                {
                    var rb = new DistinctCursor(cx,drs,0, bmk);
                    if (rb.Matches(cx) && Query.Eval(drs.where, cx))
                    {
                        cx.from = ox;
                        return rb;
                    }
                }
                cx.from = ox;
                return null;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                throw new NotImplementedException();
            }
            protected override Cursor _Next(Context cx)
            {
                var ox = cx.from;
                cx.from += cx.data[_drs.source].finder;
                for (var bmk = _bmk.Next(); bmk != null; bmk = bmk.Next())
                {
                    var rb = new DistinctCursor(cx, _drs,_pos + 1, bmk);
                    if (rb.Matches(cx) && Query.Eval(_drs.where, cx))
                    {
                        cx.from = ox;
                        return rb;
                    }
                }
                cx.from = ox;
                return null;
            }
            internal override TableRow Rec()
            {
                throw new NotImplementedException();
            }
        }
    }
    internal class OrderedRowSet : RowSet
    {
        internal readonly RTree tree;
        internal readonly long source;
        internal readonly CList<long> ordSpec;
        internal readonly bool distinct;
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        public OrderedRowSet(Context _cx,RowSet r,CList<long> os,bool dct)
            :base(_cx.nextHeap++, _cx, r.dataType,r.display,r.finder,os,r.where,
                 os,r.matches)
        {
            source = r.defpos;
            distinct = dct;
            ordSpec = os;
        }
        protected OrderedRowSet(OrderedRowSet rs, long a, long b) : base(rs, a, b)
        {
            source = rs.source;
            distinct = rs.distinct;
            ordSpec = rs.ordSpec;
            tree = rs.tree;
        }
        protected OrderedRowSet(Context cx,OrderedRowSet rs,BTree<long,Finder> nd,RTree tr) 
            :base(cx,rs,nd,true)
        {
            source = rs.source;
            distinct = rs.distinct;
            ordSpec = rs.ordSpec;
            tree = tr;
        }
        internal override RowSet New(long a, long b)
        {
            return new OrderedRowSet(this, a, b);
        }
        OrderedRowSet(Context cx, OrderedRowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {
            source = rs.source;
            distinct = rs.distinct;
            ordSpec = rs.ordSpec;
            tree = rs.tree;
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new OrderedRowSet(cx, this, nd, bt);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Ordered ");
            if (distinct)
                sb.Append("distinct ");
            sb.Append(keys);
            base._Strategy(sb, indent);
        }
        internal override int? Count => (int)tree.rows.Count;
        internal override RowSet Build(Context cx,BTree<long,Finder> nd)
        {
            var _cx = new Context(cx);
            var sce = cx.data[source];
            _cx.from += sce.finder;
            var dm = new Domain(Sqlx.ROW,cx, keys);
            var tree = new RTree(defpos,cx, keys, dm, 
                distinct ? TreeBehaviour.Ignore : TreeBehaviour.Allow, TreeBehaviour.Allow);
            for (var e = sce.First(_cx); e != null; e = e.Next(_cx))
            {
                var vs = BTree<long,TypedValue>.Empty;
                for (var b = rowOrder.First(); b != null; b = b.Next())
                {
                    var s = cx.obs[b.value()];
                    vs += (s.defpos,s.Eval(_cx));
                }
                RTree.Add(ref tree, new TRow(dm, vs), e);
            }
            return new OrderedRowSet(cx,this,nd,tree);
        }
        protected override Cursor _First(Context _cx)
        {
            return OrderedCursor.New(_cx, this, RTreeBookmark.New(_cx, tree));
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Source: "); sb.Append(DBObject.Uid(source));
            return sb.ToString();
        }
        internal class OrderedCursor : Cursor
        {
            readonly OrderedRowSet _ors;
            readonly Cursor _rb;
            internal OrderedCursor(Context cx,OrderedRowSet ors,Cursor rb)
                :base(cx,ors,rb._pos,rb._defpos,rb)
            {
                cx.cursors += (ors.defpos, this);
                _ors = ors; _rb = rb;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                throw new NotImplementedException();
            }
            internal static OrderedCursor New(Context cx,OrderedRowSet ors,Cursor rb)
            {
                if (rb == null)
                    return null;
                return new OrderedCursor(cx, ors, rb);
            }
            protected override Cursor _Next(Context _cx)
            {
                return New(_cx, _ors, _rb.Next(_cx));
            }
            internal override TableRow Rec()
            {
                return _rb.Rec();
            }
        }
    }
    internal class EmptyRowSet : RowSet
    {
        public static readonly EmptyRowSet Value = new EmptyRowSet();
        EmptyRowSet() : base(-1,null,null) { }
        internal EmptyRowSet(long dp, Context cx) : base(dp, cx, Value) { }
        protected EmptyRowSet(EmptyRowSet rs, long a, long b) : base(rs, a, b)
        { }
        internal override RowSet New(long a, long b)
        {
            return new EmptyRowSet(this, a, b);
        }
        EmptyRowSet(Context cx, RowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) { }
        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new EmptyRowSet(cx, this, nd, bt);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Empty");
            base._Strategy(sb, indent);
        }
        internal override int? Count => 0;
        protected override Cursor _First(Context _cx)
        {
            return null;
        }
    }
    /// <summary>
    /// A rowset for SqlRows
    /// </summary>
    internal class SqlRowSet : RowSet
    {
        internal readonly BList<long> rows;
        internal SqlRowSet(long dp,Context cx,Domain xp, BList<long> rs) 
            : base(dp, cx, xp)
        {
            rows = rs;
        }
        protected SqlRowSet(SqlRowSet rs, long a, long b) : base(rs, a, b)
        {
            rows = rs.rows;
        }
        internal override RowSet New(long a, long b)
        {
            return new SqlRowSet(this, a, b);
        }
        SqlRowSet(Context cx, SqlRowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {
            rows = rs.rows;
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new SqlRowSet(cx, this, nd, bt);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("SqlRows ");
            sb.Append(rows.Length);
            base._Strategy(sb, indent);
        }
        protected override Cursor _First(Context _cx)
        {
            return SqlCursor.New(_cx,this);
        }
        internal class SqlCursor : Cursor
        {
            readonly SqlRowSet _srs;
            readonly ABookmark<int, long> _bmk;
            SqlCursor(Context cx,SqlRowSet rs,int pos,ABookmark<int,long> bmk)
                : base(cx,rs,pos,0,(TRow)cx.obs[rs.rows[bmk.key()]].Eval(cx))
            {
                _srs = rs;
                _bmk = bmk;
            }
            SqlCursor(SqlCursor cu,Context cx,long p,TypedValue v):base(cu,cx,p,v)
            {
                _srs = cu._srs;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new SqlCursor(this, cx, p, v);
            }
            internal static SqlCursor New(Context _cx,SqlRowSet rs)
            {
                for (var b=rs.rows.First();b!=null;b=b.Next())
                {
                    var rb = new SqlCursor(_cx,rs, 0, b);
                    if (rb.Matches(_cx) && Query.Eval(rs.where, _cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor _Next(Context _cx)
            {
                for (var b=_bmk.Next();b!=null;b=b.Next())
                {
                    var rb = new SqlCursor(_cx,_srs, _pos+1,b);
                    if (rb.Matches(_cx) && Query.Eval(_srs.where, _cx))
                        return rb;
                }
                return null;
            }
            internal override TableRow Rec()
            {
                throw new NotImplementedException();
            }
        }
    }
    internal class TableExpRowSet : RowSet
    {
        internal readonly long source;
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        internal TableExpRowSet(long dp, Context cx, CList<long> cs, CList<long> ks, 
            RowSet sc,BTree<long, bool> wh, BTree<long, TypedValue> ma,BTree<long,Finder> fi)
            : base(dp, cx, new Domain(Sqlx.ROW,cx,cs), cs.Length, _Fin(fi,sc), ks, wh, sc.rowOrder, ma) 
        {
            source = sc.defpos;
        }
        protected TableExpRowSet(TableExpRowSet rs, long a, long b) : base(rs, a, b)
        {
            source = rs.source;
        }
        TableExpRowSet(Context cx, TableExpRowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {
            source = rs.source;
        } 
        internal override RowSet New(long a, long b)
        {
            return new TableExpRowSet(this, a, b);
        }
        static BTree<long,Finder> _Fin(BTree<long,Finder> fi,RowSet sc)
        {
            for (var b = sc.finder.First(); b != null; b = b.Next())
                if (!fi.Contains(b.key()))
                    fi += (b.key(), b.value());
            return fi;
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd,bool bt)
        {
            return new TableExpRowSet(cx, this, nd, bt);
        }
        protected override Cursor _First(Context _cx)
        {
            return TableExpCursor.New(_cx,this);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Source: "); sb.Append(DBObject.Uid(source));
            return sb.ToString();
        }
        internal class TableExpCursor : Cursor
        {
            internal readonly TableExpRowSet _trs;
            internal readonly Cursor _bmk;
            TableExpCursor(Context cx,TableExpRowSet trs,Cursor bmk,int pos) 
                :base(cx,trs,pos,bmk._defpos,new TRow(trs,bmk))
            {
                _trs = trs;
                _bmk = bmk;
            }
            TableExpCursor(TableExpCursor cu,Context cx, long p,TypedValue v):base(cu,cx,p,v)
            {
                _trs = cu._trs;
                _bmk = cu._bmk;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new TableExpCursor(this, cx, p, v);
            }
            public static TableExpCursor New(Context cx,TableExpRowSet trs)
            {
                var ox = cx.from;
                var sce = cx.data[trs.source];
                cx.from += sce.finder;
                for (var bmk=sce.First(cx);bmk!=null;bmk=bmk.Next(cx))
                {
                    var rb = new TableExpCursor(cx, trs, bmk, 0);
                    if (rb.Matches(cx) && Query.Eval(trs.where, cx))
                    {
                        cx.from = ox;
                        return rb;
                    }
                }
                cx.from = ox;
                return null;
            }
            protected override Cursor _Next(Context cx)
            {
                var ox = cx.from;
                cx.from += cx.data[_trs.source].finder;
                for (var bmk = _bmk.Next(cx); bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new TableExpCursor(cx, _trs, bmk, _pos+1);
                    if (rb.Matches(cx) && Query.Eval(_trs.where, cx))
                    {
                        cx.from = ox;
                        return rb;
                    }
                }
                cx.from = ox;
                return null;
            }
            internal override TableRow Rec()
            {
                return _bmk.Rec();
            }
        }
    }
    /// <summary>
    /// a row set for TRows
    /// </summary>
    internal class ExplicitRowSet : RowSet
    {
        internal readonly BList<(long,TRow)> rows;
        /// <summary>
        /// constructor: a set of explicit rows
        /// </summary>
        /// <param name="rt">a row type</param>
        /// <param name="r">a a set of TRows from q</param>
        internal ExplicitRowSet(long dp,Context cx,Domain dt,BList<(long,TRow)>r)
            : base(dp,cx,dt)
        {
            rows = r;
        }
        internal ExplicitRowSet(long dp,Context cx,RowSet sce)
            : base(dp, cx, sce.dataType,sce.display,sce.finder)
        { }
        protected ExplicitRowSet(ExplicitRowSet rs, long a, long b) : base(rs, a, b)
        {
            rows = rs.rows;
        }
        internal override RowSet New(long a, long b)
        {
            return new ExplicitRowSet(this, a, b);
        }
        ExplicitRowSet(Context cx, ExplicitRowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {
            rows = rs.rows;
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd,bool bt)
        {
            return new ExplicitRowSet(cx, this, nd, bt);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Explicit");
            base._Strategy(sb, indent);
            if (Count < 10)
                for (var r =rows.First();r!=null;r=r.Next())
                {
                    var s = new StringBuilder();
                    for (var i = 0; i < indent; i++)
                        s.Append(' ');
                    s.Append(r.value());
                }
            else
                sb.Append("" + Count + " rows");
        }
        internal override int? Count => (int)rows.Count;
        protected override Cursor _First(Context _cx)
        {
            return ExplicitCursor.New(_cx,this,0);
        }
        public override string ToString()
        {
            var r = base.ToString();
            if (rows.Count<10)
            {
                var sb = new StringBuilder(r);
                var cm = "";
                sb.Append("[");
                for (var b=rows.First();b!=null;b=b.Next())
                {
                    var (p, rw) = b.value();
                    sb.Append(cm); cm = ",";
                    sb.Append(DBObject.Uid(p));
                    sb.Append(": ");sb.Append(rw);
                }
                sb.Append("]");
                r = sb.ToString();
            }
            return r;
        }
        internal class ExplicitCursor : Cursor
        {
            readonly ExplicitRowSet _ers;
            readonly ABookmark<int,(long,TRow)> _prb;
            ExplicitCursor(Context _cx, ExplicitRowSet ers,ABookmark<int,(long,TRow)>prb,int pos) 
                :base(_cx,ers,pos,prb.value().Item1,prb.value().Item2.ToArray())
            {
                _ers = ers;
                _prb = prb;
            }
            ExplicitCursor(ExplicitCursor cu,Context cx, long p,TypedValue v):base(cu,cx,p,v)
            {
                _ers = cu._ers;
                _prb = cu._prb;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new ExplicitCursor(this, cx, p, v);
            }
            internal static ExplicitCursor New(Context _cx,ExplicitRowSet ers,int i)
            {
                var ox = _cx.from;
                _cx.from += ers.finder;
                for (var b=ers.rows.First();b!=null;b=b.Next())
                {
                    var rb = new ExplicitCursor(_cx,ers, b, 0);
                    if (rb.Matches(_cx) && Query.Eval(ers.where, _cx))
                    {
                        _cx.from = ox;
                        return rb;
                    }
                }
                _cx.from = ox;
                return null;
            }
            protected override Cursor _Next(Context _cx)
            {
                var ox = _cx.from;
                _cx.from += _ers.finder;
                for (var prb = _prb.Next(); prb!=null; prb=prb.Next())
                {
                    var rb = new ExplicitCursor(_cx,_ers, prb, _pos+1);
                    if (rb.Matches(_cx) && Query.Eval(_ers.where, _cx))
                    {
                        _cx.from = ox;
                        return rb;
                    }
                }
                _cx.from = ox;
                return null;
            }
            internal override TableRow Rec()
            {
                return null;
            }
        }
    }
    /// <summary>
    /// Deal with execution of Triggers. The main complication here is that triggers execute 
    /// in their definer's role (possibly many roles involved in defining table, columns, etc)
    /// so the column names and data types will be different for different triggers, and
    /// trigger actions may affect different tables.
    /// We need to ensure that the TransitionRowSet rowType contains all the columns for the target,
    /// even if they are not in the data rowSet.
    /// As with TableRowSet and IndexRowSet, _finder refers to physical uids.
    /// Another nuisance is that we need to manage our own copies of autokeys, as we may be preparing
    /// many new rows for a table.
    /// </summary>
    internal class TransitionRowSet : RowSet
    {
        internal readonly BTree<long, TypedValue> defaults = BTree<long, TypedValue>.Empty; 
        internal readonly From from; // will be a SqlInsert, QuerySearch or UpdateSearch
        internal readonly ObInfo targetInfo;
        internal readonly Activation targetAc;
        internal readonly BTree<long, Finder> targetTrans,transTarget;
        internal readonly long indexdefpos = -1L;
        internal readonly PTrigger.TrigType _tgt;
        /// <summary>
        /// There may be several triggers of any type, so we manage a set of transition activations for each.
        /// These are for table before, table instead, table after, row before, row instead, row after.
        /// </summary>
        internal readonly BTree<long, TriggerActivation> rb, ri, ra;
        internal readonly BTree<long, TriggerActivation> tb, ti, ta, td;
        internal readonly Adapters _eqs;
        internal TransitionRowSet(Context cx, From q, PTrigger.TrigType tg, Adapters eqs)
            : base(cx.nextHeap++, cx, q.domain, q.display,
                  cx.data[q.defpos]?.finder??BTree<long,Finder>.Empty, null, q.where)
        {
            from = q;
            _eqs = eqs;
            var tr = cx.db;
            var t = tr.objects[from.target] as Table;
            indexdefpos = t.FindPrimaryIndex(tr)?.defpos ?? -1L;
            // check now about conflict with generated columns
            if (t.Denied(cx, Grant.Privilege.Insert))
                throw new DBException("42105", q);
            var rt = q.rowType; // data rowType
            targetInfo = tr.schemaRole.infos[t.defpos] as ObInfo;
            var tgTn = BTree<long, Finder>.Empty;
            var tnTg = BTree<long, Finder>.Empty;
            for (var b = rt.First(); b != null; b = b.Next()) // at this point q is the insert statement, simpleQuery is the base table
            {
                var p = b.value();
                var s = cx.obs[p];
                var c = (s is SqlCopy sc) ? sc.copyFrom : s.defpos;
                tgTn += (c,new Finder(p,defpos));
                tnTg += (p,new Finder(c,q.defpos));
            }
            targetTrans = tgTn;
            transTarget = tnTg;
            targetAc = new Activation(cx, q.name);
            if (tg != PTrigger.TrigType.Delete)
            {
                for (var b = targetInfo.domain.rowType.First(); b != null; b = b.Next())
                {
                    var p = b.value();
                    cx.from += (p, new Finder(p,defpos));
                    var tc = (TableColumn)tr.objects[p];
                    var tv = tc.defaultValue ?? tc.domain.defaultValue;
                    if (tv != TNull.Value)
                    {
                        defaults += (tc.defpos, tv);
                        cx.values += (tc.defpos, tv);
                    }
                    for (var c = tc.constraints.First(); c != null; c = c.Next())
                    {
                        cx.Frame(c.key());
                        for (var d = tc.domain.constraints.First(); d != null; d = d.Next())
                            cx.Frame(d.key());
                    }
                    cx.Frame(tc.generated.exp,tc.defpos);
                }
            }
            _tgt = tg;
            // Get the trigger sets and set up the activations
            tb = Setup(cx, t, t.triggers[_tgt | PTrigger.TrigType.EachStatement | PTrigger.TrigType.Before]);
            ti = Setup(cx, t, t.triggers[_tgt | PTrigger.TrigType.EachStatement | PTrigger.TrigType.Instead]);
            ta = Setup(cx, t, t.triggers[_tgt | PTrigger.TrigType.EachStatement | PTrigger.TrigType.After]);
            td = Setup(cx, t, t.triggers[_tgt | PTrigger.TrigType.Deferred]);
            rb = Setup(cx, t, t.triggers[_tgt | PTrigger.TrigType.EachRow | PTrigger.TrigType.Before]);
            ri = Setup(cx, t, t.triggers[_tgt | PTrigger.TrigType.EachRow | PTrigger.TrigType.Instead]);
            ra = Setup(cx, t, t.triggers[_tgt | PTrigger.TrigType.EachRow | PTrigger.TrigType.After]);
        }
        protected TransitionRowSet(TransitionRowSet rs, long a, long b) : base(rs, a, b)
        {
            defaults = rs.defaults;
            from = rs.from;
            targetInfo = rs.targetInfo;
            indexdefpos = rs.indexdefpos;
            _tgt = rs._tgt;
            tb = rs.tb;
            ti = rs.ti;
            ta = rs.ta;
            td = rs.td;
            ri = rs.ri;
            ra = rs.ra;
            rb = rs.rb;
            targetTrans = rs.targetTrans;
            transTarget = rs.transTarget;
            targetAc = rs.targetAc;
            _eqs = rs._eqs;
        }
        protected TransitionRowSet(Context cx,TransitionRowSet rs,BTree<long,Finder> nd,
            bool bt)
            :base(cx,rs,nd,bt)
        {
            defaults = rs.defaults;
            from = rs.from;
            targetInfo = rs.targetInfo;
            indexdefpos = rs.indexdefpos;
            _tgt = rs._tgt;
            tb = rs.tb;
            ti = rs.ti;
            ta = rs.ta;
            td = rs.td;
            ri = rs.ri;
            ra = rs.ra;
            rb = rs.rb;
            targetTrans = rs.targetTrans;
            transTarget = rs.transTarget;
            targetAc = rs.targetAc;
            _eqs = rs._eqs;
        }
        internal override RowSet New(long a, long b)
        {
            return new TransitionRowSet(this, a, b);
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd,bool bt)
        {
            return new TransitionRowSet(cx,this,nd,bt);
        }
        internal override bool TableColsOk => true;
        internal override RowSet Source(Context cx)
        {
            return cx.data[from.defpos];
        }
        protected override Cursor _First(Context _cx)
        {
            return TransitionCursor.New(_cx,this);
        }
        /// <summary>
        /// Implement the autokey feature: if a key column is an integer type,
        /// the engine will pick a suitable unused value. 
        /// The works cleverly for multi-column indexes. 
        /// The transition rowset adds to a private copy of the index as there may
        /// be several rows to add, and the null column(s!) might not be the first key column.
        /// </summary>
        /// <param name="fl"></param>
        /// <param name="ix"></param>
        /// <returns></returns>
        TargetCursor CheckPrimaryKey(Context cx,TargetCursor tgc)
        {
            var ix = (Index)cx.db.objects[indexdefpos];
            if (ix == null)
                return tgc;
            var k = BList<TypedValue>.Empty;
            for (var b=ix.keys.First(); b!=null; b=b.Next())
            {
                var tc = (TableColumn)cx.obs[b.value()];
                var v = tgc[tc.defpos];
                if (v == null || v == TNull.Value)
                {
                    if (tc.domain.kind != Sqlx.INTEGER)
                        throw new DBException("22004");
                    v = ix.rows.NextKey(k, 0, b.key());
                    if (v == TNull.Value)
                        v = new TInt(0);
                    tgc += (cx, tc.defpos, v);
                    cx.values += (tc.defpos, v);
                }
                k += v;
            }
            return tgc;
        }
        /// <summary>
        /// Set up activations for executing a set of triggers
        /// </summary>
        /// <param name="q"></param>
        /// <param name="tgs"></param>
        /// <returns></returns>
        BTree<long, TriggerActivation> Setup(Context _cx,Table tb,BTree<long, bool> tgs)
        {
            var r = BTree<long, TriggerActivation>.Empty;
            var cx = new Context(_cx.tr);
            cx.nextHeap = _cx.nextHeap;
            cx.obs = _cx.obs;
            cx.data = _cx.data;
            cx.obs += (tb.defpos,tb);
            for (var b = tb.tblCols.First(); b != null; b = b.Next())
            {
                var tc = (DBObject)_cx.tr.objects[b.key()];
                cx.obs += (tc.defpos,tc);
            }
            if (tgs != null)
                for (var tg = tgs.First(); tg != null; tg = tg.Next())
                {
                    var t = tg.key();
                    cx.Frame(t);
                    // NB at the cx.obs[t] version of the trigger has the wrong action field
                    r += (t, new TriggerActivation(cx, this, (Trigger)cx.db.objects[t]));
                }
            return r;
        }
        /// <summary>
        /// Perform the triggers in a set. 
        /// </summary>
        /// <param name="acts"></param>
        (TransitionCursor,bool) Exec(Context _cx, BTree<long, TriggerActivation> acts)
        {
            var r = false;
            if (acts == null)
                return (_cx.cursors[defpos] as TransitionCursor, r);
            targetAc.db = _cx.db;
            var c = _cx.cursors[defpos];
            TargetCursor row = (c as TargetCursor)
                ??((c is TransitionCursor tc)? new TargetCursor(targetAc,tc):null);
            bool skip;
            for (var a = acts.First(); a != null; a = a.Next())
            {
                var ta = a.value();
                ta.db = _cx.db;
                (row, skip) = ta.Exec(targetAc, row);
                r = r || skip;
                targetAc.db = ta.db;
            }
            _cx = targetAc.SlideDown();
            _cx.val = TBool.For(r);
            var cu = row?._trsCu;
            _cx.cursors += (defpos, cu); // restore the TransitionCursor
            return (cu,r);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Source: "); sb.Append(DBObject.Uid(from.defpos));
            return sb.ToString();
        }
        internal (TransitionCursor,bool) InsertSA(Context _cx)
        { return Exec(_cx,ta); }
        internal (TransitionCursor, bool) InsertSB(Context _cx)
        { return Exec(_cx, tb); }
        internal (TransitionCursor, bool) UpdateSA(Context _cx)
        { return Exec(_cx,ta); }
        internal (TransitionCursor, bool) UpdateSB(Context _cx)
        { Exec(_cx, tb);  return Exec(_cx, ti); }
        internal (TransitionCursor, bool) DeleteSB(Context _cx)
        { Exec(_cx,tb); return Exec(_cx,ti); }
        internal (TransitionCursor, bool) DeleteSA(Context _cx)
        { return Exec(_cx, td); }
        internal class TransitionCursor : Cursor
        {
            internal readonly TransitionRowSet _trs;
            internal readonly Cursor _fbm; // transition source cursor
            internal readonly TRow targetRow; // for physical record construction, triggers, constraints
            internal readonly BTree<long,TypedValue> _oldVals;
            internal TransitionCursor(Context cx, TransitionRowSet trs, Cursor fbm, int pos)
                : base(cx, trs, pos, fbm._defpos, new TRow(trs,fbm))
            {
                _trs = trs;
                _fbm = fbm;
                _oldVals = values;
                targetRow = new TRow(_trs.targetInfo.domain,_trs.targetTrans, values);
                cx.values += (targetRow.values,false);
            }
            TransitionCursor(TransitionCursor cu,Context cx,long p,TypedValue v):base(cu,cx,p,v)
            {
                _trs = cu._trs;
                _fbm = cu._fbm;
                _oldVals = cu._oldVals;
                targetRow = new TRow(_trs.targetInfo.domain,_trs.targetTrans, values);
                cx.values += (targetRow.values,false);
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                cx.values += (p, v);
                return new TransitionCursor(this, cx, p, v);
            }
            public static TransitionCursor operator+(TransitionCursor cu,
                (Context,long,TypedValue)x)
            {
                var (cx, p, tv) = x;
                cx.values += (p, tv);
                return new TransitionCursor(cu, cx, p, tv);
            }
            static TransitionCursor New(Context cx,TransitionRowSet trs, int pos,
                 Cursor fbm)
            { 
                var trc = new TransitionCursor(cx, trs, fbm, pos);
                var tb = trs.rt.First();
                for (var b=fbm.columns.First(); b!=null&&tb!=null;b=b.Next(),tb=tb.Next())
                {
                    var cp = b.value();
                    var sl = cx.obs[cp];
                    TypedValue tv = fbm[cp];
                    if (sl is SqlProcedureCall sv)
                    {
                        var fp = ((CallStatement)cx.obs[sv.call]).procdefpos;
                        var m = trs._eqs.Match(fp, sl.defpos);
                        if (m.HasValue)
                        {
                            if (m.Value == 0)
                                tv = sl.Eval(cx);
                            else // there's an adapter function
                            {
                                // tv = fn (fbm[j])
                                var pr = cx.db.objects[m.Value] as Procedure;
                                var ac = new CalledActivation(cx, pr, Domain.Null);
                                tv = cx.obs[pr.body].Eval(ac);
                            }
                        }
                    }
                    trc += (cx, tb.value(), tv);
                }
                trc = TargetCursor.New(trs.targetAc, trc, pos)._trsCu;
                return trc;
            }
            internal static TransitionCursor New(Context _cx,TransitionRowSet trs)
            {
                var ox = _cx.from;
                var sce = _cx.data[trs.from.defpos];
                _cx.from += sce?.finder;
                for (var fbm = sce?.First(_cx); fbm != null;
                    fbm = fbm.Next(_cx))
                    if (fbm.Matches(_cx) && Query.Eval(trs.from.where, _cx))
                    {
                        var r = New(_cx, trs, 0, fbm);
                        _cx.from = ox;
                        _cx.cursors += (r._rowsetpos, r);
                        return r;
                    }
                _cx.from = ox;
               return null;
            }
            /// <summary>
            /// Set up activations for executing a set of triggers
            /// </summary>
            /// <param name="q"></param>
            /// <param name="tgs"></param>
            /// <returns></returns>
            BTree<long, TriggerActivation> Setup(Context cx, BTree<long, bool> tgs)
            {
                var r = BTree<long, TriggerActivation>.Empty;
                cx = new Context(cx); // for this trs
                if (tgs != null)
                    for (var tg = tgs.First(); tg != null; tg = tg.Next())
                    {
                        var trg = tg.key();
                        var ta = new TriggerActivation(cx,_trs, (Trigger)cx.obs[trg]);
                        r +=(trg, ta);
                    }
                return r;
            }
            protected override Cursor _Next(Context cx)
            {
                var ox = cx.from;
                cx.from += cx.data[_fbm._rowsetpos].finder;
                var from = _trs.from;
                if(cx.obs[from.where.First()?.key()??-1L] is SqlValue sv && sv.domain.kind == Sqlx.CURRENT)
                        return null;
                var t = cx.db.objects[_trs.from.target] as Table;
                for (var fbm = _fbm.Next(cx); fbm != null; fbm = fbm.Next(cx))
                {
                    var ret = New(cx,_trs, 0, fbm);
                    for (var b = from.where.First(); b != null; b = b.Next())
                        if (cx.obs[b.key()].Eval(cx) != TBool.True)
                            goto skip;
                    cx.from = ox;
                    return ret;
                    skip:;
                }
                cx.from = ox;
                return null;
            }
            internal override TableRow Rec()
            {
                return _fbm.Rec();
            }
            /// <summary>
            /// Some convenience functions for calling from Transaction.Execute(..)
            /// </summary>
            internal (TransitionCursor,bool) InsertRA(Context _cx)
            { return _trs.Exec(_cx,_trs.ra); }
            internal (TransitionCursor,bool) InsertRB(Context _cx)
            { _trs.Exec(_cx, _trs.rb); return _trs.Exec(_cx,_trs.ri); }
            internal (TransitionCursor,bool) UpdateRA(Context _cx)
            { return _trs.Exec(_cx,_trs.ra); }
            internal (TransitionCursor,bool) UpdateRB(Context _cx)
            { _trs.Exec(_cx,_trs.rb); return _trs.Exec(_cx,_trs.ri); }
            internal (TransitionCursor,bool) DeleteRB(Context _cx)
            { _trs.Exec(_cx, _trs.rb); return _trs.Exec(_cx,_trs.ri); }

        }
        internal class TargetCursor : Cursor
        {
            internal readonly BTree<long, Finder> _map;
            internal readonly TransitionCursor _trsCu;
            internal readonly BTree<long, TypedValue> _oldVals;
            internal TargetCursor(Context cx, TransitionCursor cu)
                : base(cx, cu._trs.defpos, cu._pos, cu._defpos, 
                     cu._trs.targetInfo.domain, cu.targetRow)
            {
                _trsCu = cu;
                var ov = BTree<long, TypedValue>.Empty;
                var ti = cu._trs.targetInfo;
                for (var b = ti.domain.rowType.First(); b != null; b = b.Next())
                    ov += (b.value(), cu._oldVals[cu._trs.targetTrans[b.value()].col]);
                _oldVals = ov;
                _map = cu._trs.targetTrans;
            }
            internal static TargetCursor New(Context cx, TransitionCursor trc,int pos)
            {
                var tgc = new TargetCursor(cx, trc);
                var trs = trc._trs;
                var ti = trs.targetInfo;
                var j = 0;
                var fi = BTree<long, Finder>.Empty;
                var tb = ti.domain.rowType.First();
                for (var b=trs.rt.First();b!=null&&tb!=null;b=b.Next(),tb=tb.Next())
                    fi += (tb.value(), new Finder(b.value(),trs.defpos));
                var oc = cx.from;
                var od = cx.cursors;
                cx.from = fi;
                cx.cursors += (trs.defpos, trc);
                for (var b = ti.domain.rowType.First();b!=null;b=b.Next(), j++)
                    if (cx.db.objects[b.value()] is TableColumn tc)
                    {

                        switch (tc.generated.gen)
                        {
                            case Generation.Expression:
                                if (tgc[tc.defpos] != TNull.Value)
                                    throw new DBException("0U000", cx.Inf(tc.defpos).name);
                                cx.from = oc;
                                cx.cursors = od;   
                                cx.Frame(tc.generated.exp,tc.defpos);
                                tgc += (cx, tc.defpos, cx.obs[tc.generated.exp].Eval(cx));
                                cx.from = fi;
                                cx.cursors += (trs.defpos, trc);
                                break;
                        }
                        if (tc.defaultValue != TNull.Value && tgc[tc.defpos] == TNull.Value)
                            tgc += (cx, tc.defpos, tc.defaultValue);
                        if (tc.notNull 
                            && (tgc[tc.defpos]==null || tgc[tc.defpos] == TNull.Value))
                            throw new DBException("22206", cx.Inf(tc.defpos).name);
                        for (var cb = tc.constraints?.First(); cb != null; cb = cb.Next())
                        {
                            var cp = cb.key();
                            var ck = (Check)cx.db.objects[cp];
                            cx.obs += (ck.defpos, ck);
                            cx.Frame(ck.search,ck.defpos);
                            var se = (SqlValue)cx.obs[ck.search];
                            if (se.Eval(cx) != TBool.True)
                                throw new DBException("22212", cx.Inf(tc.defpos).name);
                        }
                    }
                cx.from = oc;
                cx.cursors = od;
                if (trs.indexdefpos > 0)
                    tgc = trs.CheckPrimaryKey(cx, tgc);
                return tgc;
            }
            TargetCursor(TargetCursor cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
            {
                _map = cu._map;
                _trsCu = cu._trsCu+(cx,_map[p].col,v);
                cx.values += (p,v);
                cx.cursors += (cu._trsCu._trs.defpos, this);
            }
            public static TargetCursor operator +(TargetCursor cu,(Context, long, TypedValue) x)
            {
                var (cx, p, tv) = x;
                return new TargetCursor(cu, cx, p, tv);
            }
            protected override Cursor _Next(Context _cx)
            {
                var cu = (TransitionCursor)_trsCu.Next(_cx);
                if (cu == null)
                    return null;
                return new TargetCursor(_cx, cu);
            }

            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new TargetCursor(cx,_trsCu+(cx,_map[p].col,v));
            }

            internal override TableRow Rec()
            {
                return _trsCu.Rec();
            }
        }
    }
    /// <summary>
    /// Used for oldTable/newTable in trigger execution
    /// </summary>
    internal class TransitionTableRowSet : RowSet
    {
        internal readonly TransitionRowSet _trs;
        internal readonly BTree<long,TableRow> data;
        internal readonly BTree<long, Finder> map,rmap;
        /// <summary>
        /// Old table: compute the rows that will be in the update/delete
        /// </summary>
        /// <param name="dp"></param>
        /// <param name="cx"></param>
        /// <param name="trs"></param>
        internal TransitionTableRowSet(long dp,Context cx,TransitionRowSet trs)
            :base(dp,cx,trs.dataType,-1,trs.finder)
        {
            var dat = BTree<long, TableRow>.Empty;
            for (var b = trs.First(cx); b != null; b = b.Next(cx))
                dat += (b._defpos, b.Rec());
            _trs = trs;
            data = dat;
            map = trs.targetTrans;
            rmap = trs.transTarget;
        }
        /// <summary>
        /// New table: Get the new rows from the context
        /// </summary>
        /// <param name="dp"></param>
        /// <param name="cx"></param>
        /// <param name="trs"></param>
        internal TransitionTableRowSet(long dp,Context cx,long trs)
            :base(dp,cx,cx.data[trs].dataType)
        {
            _trs = (TransitionRowSet)cx.data[trs];
            data = cx.newTables[trs];
            map = _trs.targetTrans;
            rmap = _trs.transTarget;
        }
        internal TransitionTableRowSet(Context cx,TransitionTableRowSet rs,
            BTree<long,Finder>nd,bool bt) :base(cx,rs,nd,bt)
        {
            _trs = rs._trs;
            data = rs.data;
            map = rs.map;
            rmap = rs.rmap;
        }
        internal override RowSet Source(Context cx)
        {
            return _trs.Source(cx);
        }
        protected override Cursor _First(Context _cx)
        {
            return TransitionTableCursor.New(_cx,this);
        }

        internal override RowSet New(Context cx,BTree<long,Finder> nd,bool bt)
        {
            return new TransitionTableRowSet(cx, this, nd, bt);
        }

        internal override RowSet New(long a, long b)
        {
            throw new NotImplementedException();
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Source: "); sb.Append(DBObject.Uid(_trs.from.defpos));
            return sb.ToString();
        }
        internal class TransitionTableCursor : Cursor
        {
            internal readonly ABookmark<long, TableRow> _bmk;
            internal readonly TransitionTableRowSet _tt;
            TransitionTableCursor(Context cx,TransitionTableRowSet tt,ABookmark<long,TableRow> bmk,int pos)
                :base(cx,tt,pos,bmk.key(),new TRow(tt.domain,tt.rmap,bmk.value().vals))
            {
                _bmk = bmk; _tt = tt;
            }
            internal static TransitionTableCursor New(Context cx,TransitionTableRowSet tt)
            {
                var bmk = tt.data.First();
                return (bmk!=null)? new TransitionTableCursor(cx, tt, bmk, 0): null;
            }
            protected override Cursor _Next(Context _cx)
            {
                var bmk = _bmk.Next();
                return (bmk != null) ? new TransitionTableCursor(_cx, _tt, bmk, _pos + 1):null;
            }

            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                throw new NotImplementedException();
            }

            internal override TableRow Rec()
            {
                return _bmk.value();
            }
        }
    }
    /// <summary>
    /// RoutineCallRowSet is a table-valued routine call
    /// </summary>
    internal class RoutineCallRowSet : RowSet
    {
        readonly Query from;
        readonly Procedure proc;
        readonly BList<long> actuals;
        readonly long rowSet;
        internal RoutineCallRowSet(Context cx,From f,Procedure p, BList<long> r) 
            :base(cx.GetUid(),cx,f.domain,-1,null,null,f.where)
        {
            from = f;
            proc = p;
            actuals = r;
        }
        protected RoutineCallRowSet(RoutineCallRowSet rs, long a, long b) : base(rs, a, b)
        {
            from = rs.from;
            proc = rs.proc;
            actuals = rs.actuals;
            rowSet = rs.rowSet;
        }
        protected RoutineCallRowSet(Context cx,RoutineCallRowSet rs,BTree<long,Finder> nd,RowSet r) 
            :base(cx,rs,nd,true)
        {
            from = rs.from;
            proc = rs.proc;
            actuals = rs.actuals;
            rowSet = r.defpos;
        }
        internal override RowSet New(long a, long b)
        {
            return new RoutineCallRowSet(this, a, b);
        }
        internal override RowSet New(Context cx, BTree<long,Finder> nd,bool bt)
        {
            throw new NotImplementedException();
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("RoutineCall ");
            sb.Append(DBObject.Uid(proc.defpos));
            var cm = '(';
            for(var b = actuals.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ',';
                sb.Append(b.value());
            }
            base._Strategy(sb, indent);
        }
        internal override RowSet Build(Context cx,BTree<long,Finder> nd)
        {
            var _cx = new Context(cx);
            cx = proc.Exec(_cx, actuals);
            return new RoutineCallRowSet(cx, this, nd, (RowSet)cx.val);
        }
        protected override Cursor _First(Context cx)
        {
            return cx.data[rowSet].First(cx);
        }
    }
    internal class RowSetSection : RowSet
    {
        internal readonly long source;
        internal readonly int offset,count;
        internal RowSetSection(Context _cx,RowSet s, int o, int c)
            : base(_cx.GetUid(),_cx,s.dataType,s.display,s.finder,s.keys,s.where)
        {
            source = s.defpos; offset = o; count = c;
        }
        protected RowSetSection(RowSetSection rs, long a, long b) : base(rs, a, b)
        {
            source = rs.source;
            offset = rs.offset;
            count = rs.count;
        }
        protected RowSetSection(Context cx, RowSetSection rs, BTree<long,Finder> nd,bool bt)
            :base(cx,rs,nd,bt)
        {
            source = rs.source;
            offset = rs.offset;
            count = rs.count;
        }
        internal override RowSet New(long a, long b)
        {
            return new RowSetSection(this, a, b);
        }
        internal override RowSet New(Context cx, BTree<long,Finder>nd,bool bt)
        {
            return new RowSetSection(cx, this, nd, bt);
        }
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        protected override Cursor _First(Context cx)
        {
            var b = cx.data[source].First(cx);
            for (int i = 0; b!=null && i < offset; i++)
                b = b.Next(cx);
            return RowSetSectionCursor.New(cx,this,b);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Source: "); sb.Append(DBObject.Uid(source));
            return sb.ToString();
        }
        internal class RowSetSectionCursor : Cursor
        {
            readonly RowSetSection _rss;
            readonly Cursor _rb;
            RowSetSectionCursor(Context cx, RowSetSection rss, Cursor rb)
                : base(cx, rss, rb._pos, rb._defpos, rb) 
            { 
                _rss = rss;
                _rb = rb; 
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                throw new NotImplementedException();
            }
            internal static RowSetSectionCursor New(Context cx,RowSetSection rss,Cursor rb)
            {
                if (rb == null)
                    return null;
                return new RowSetSectionCursor(cx, rss, rb);
            }
            protected override Cursor _Next(Context cx)
            {
                if (_pos+1 >= _rss.count)
                    return null;
                if (_rb.Next(cx) is Cursor rb)
                    return new RowSetSectionCursor(cx, _rss, rb);
                return null;
            }

            internal override TableRow Rec()
            {
                throw new NotImplementedException();
            }
        }
    }
    internal class DocArrayRowSet : RowSet
    {
        internal readonly BList<SqlValue> vals;
        internal DocArrayRowSet(Context cx,Query q, SqlRowArray d)
            : base(cx.GetUid(),cx,q.domain,q.display,null,null,q.where)
        {
            var vs = BList<SqlValue>.Empty;
            if (d != null)
                for(int i=0;i<d.rows.Count;i++)
                    vs += (SqlValue)cx.obs[d.rows[i]];
            vals = vs;
        }
        DocArrayRowSet(Context cx,DocArrayRowSet ds, BTree<long,Finder> nd,bool bt) 
            :base(cx,ds,nd,bt)
        {
            vals = ds.vals;
        }
        protected DocArrayRowSet(DocArrayRowSet rs, long a, long b) : base(rs, a, b)
        {
            vals = rs.vals;
        }
        internal override RowSet New(long a, long b)
        {
            return new DocArrayRowSet(this, a, b);
        }
        internal override RowSet New(Context cx, BTree<long,Finder> nd, bool bt)
        {
            return new DocArrayRowSet(cx,this, nd, bt);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("DocArray");
            base._Strategy(sb, indent);
            for (var b = vals.First(); b != null; b = b.Next())
            {
                var s = new StringBuilder();
                for (var i = 0; i < indent; i++)
                    s.Append(' ');
                s.Append(b.value());
                s.Append(": ");
                s.Append(b.value().ToString());
                Console.WriteLine(s.ToString());
            }
        }
        protected override Cursor _First(Context _cx)
        {
            return DocArrayBookmark.New(_cx,this);
        }

        internal class DocArrayBookmark : Cursor
        {
            readonly DocArrayRowSet _drs;
            readonly ABookmark<int, SqlValue> _bmk;

            DocArrayBookmark(Context _cx,DocArrayRowSet drs, ABookmark<int, SqlValue> bmk) 
                :base(_cx,drs,bmk.key(),0,_Row(_cx,bmk.value())) 
            {
                _drs = drs;
                _bmk = bmk;
            }
            static TRow _Row(Context cx,SqlValue sv)
            {
                return new TRow(sv,new BTree<long,TypedValue>(sv.defpos,(TDocument)sv.Eval(cx)));
            }
            internal static DocArrayBookmark New(Context _cx,DocArrayRowSet drs)
            {
                var bmk = drs.vals.First();
                if (bmk == null)
                    return null;
                return new DocArrayBookmark(_cx,drs, bmk);
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                throw new NotImplementedException();
            }
            protected override Cursor _Next(Context _cx)
            {
                var bmk = ABookmark<int, SqlValue>.Next(_bmk, _drs.vals);
                if (bmk == null)
                    return null;
                return new DocArrayBookmark(_cx,_drs, bmk);
            }

            internal override TableRow Rec()
            {
                throw new NotImplementedException();
            }
        }
    }
    /// <summary>
    /// WindowRowSet is built repeatedly during traversal of the source rowset.
    /// </summary>
    internal class WindowRowSet : RowSet
    {
        internal readonly RTree tree;
        internal readonly TMultiset values;
        readonly long source;
        readonly SqlFunction wf;
        internal WindowRowSet(Context cx,RowSet sc, SqlFunction f)
            :base(cx.nextHeap++,cx,sc.domain,sc.display,null,null,sc.where,null)
        {
            source = sc.defpos;
            wf = f;
        }
        protected WindowRowSet(WindowRowSet rs, long a, long b) : base(rs, a, b)
        {
            source = rs.source;
            wf = rs.wf;
        }
        protected WindowRowSet(Context cx,WindowRowSet rs,BTree<long,Finder> nd,
            RTree tr,TMultiset vs) :base(cx,rs,nd,true)
        {
            source = rs.source;
            wf = rs.wf;
            tree = tr;
            values = vs;
        }
        internal override RowSet New(long a, long b)
        {
            return new WindowRowSet(this, a, b);
        }
        protected override Cursor _First(Context _cx)
        {
            return WindowCursor.New(_cx,this);
        }
        internal override RowSet Build(Context cx, BTree<long, Finder> nd)
        {
            var w = (WindowSpecification)cx.obs[wf.window];
            // we first compute the needs of this window function
            // The key will consist of partition/grouping and order columns
            // The value part consists of the parameter of the window function
            // (There may be an argument for including the rest of the row - might we allow triggers?)
            // We build the whole WRS at this stage for saving in f
            var kd = new Domain(Sqlx.ROW, w.domain.representation, w.order);
            var tree = new RTree(source,cx, w.order, w.domain, 
                TreeBehaviour.Allow, TreeBehaviour.Disallow);
            var values = new TMultiset(cx,wf.domain);
            for (var rw = cx.data[source].First(cx); rw != null; 
                rw = rw.Next(cx))
            {
                var v = rw[wf.val];
                RTree.Add(ref tree, new TRow(kd,rw.values), new TRow(w.domain, v));
                values.Add(v);
            }
            return new WindowRowSet(cx, this, BTree<long,Finder>.Empty, tree, values);
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            throw new NotImplementedException();
        }
        internal class WindowCursor : Cursor
        {
            WindowCursor(Context cx,long rp,int pos,long defpos,TRow rw) 
                : base(cx,rp,pos,defpos,rw)
            { }
            internal static Cursor New(Context cx,WindowRowSet ws)
            {
                ws = (WindowRowSet)ws.MaybeBuild(cx);
                var bmk = ws?.tree.First(cx);
                return (bmk == null) ? null 
                    : new WindowCursor(cx,ws.defpos,0,bmk._defpos,bmk._key);
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                throw new NotImplementedException();
            }

            protected override Cursor _Next(Context cx)
            {
                throw new NotImplementedException();
            }

            internal override TableRow Rec()
            {
                throw new NotImplementedException();
            }
        }
    }
}