using System;
using Pyrrho.Common;
using System.Text;
using Pyrrho.Level2;
using Pyrrho.Level3;
using System.Diagnostics.Eventing.Reader;
using System.Data;
using System.Runtime.Remoting.Channels;
using System.Security.Policy;
using System.Runtime.InteropServices;
using System.Runtime.Remoting.Messaging;
using System.Net;
using System.IO;
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
    internal abstract class RowSet : DBObject
    {
        internal const long
            Built = -402, // bool
            _Finder = -403, // BTree<long,Finder> SqlValue
            _Needed = -401, //BTree<long,Finder>  SqlValue
            RowOrder = -404, //CList<long> SqlValue 
            _Rows = -407; // CList<TRow> 
        internal CList<long> rt => domain.rowType;
        internal CList<long> keys => (CList<long>)mem[Index.Keys] ?? CList<long>.Empty;
        internal CList<long> rowOrder => (CList<long>)mem[RowOrder] ?? CList<long>.Empty;
        internal BTree<long, bool> where =>
            (BTree<long, bool>)mem[Query.Where] ?? BTree<long, bool>.Empty;
        internal BTree<long, TypedValue> matches =>
            (BTree<long, TypedValue>)mem[Query._Matches] ?? BTree<long, TypedValue>.Empty;
        internal BTree<long, BTree<long, bool>> matching =>
            (BTree<long, BTree<long, bool>>)mem[Query.Matching] ?? BTree<long, BTree<long, bool>>.Empty;
        internal BTree<long, Finder> needed =>
            (BTree<long, Finder>)mem[_Needed]; // must be initially null
        internal bool built => (bool)(mem[Built]??false);
        internal long groupSpec => (long)(mem[TableExpression.Group]??-1L);
        internal BList<TRow> rows =>
            (BList<TRow>)mem[_Rows] ?? BList<TRow>.Empty;

        internal readonly struct Finder 
        {
            public readonly long col;
            public readonly long rowSet;
            public Finder(long c, long r) 
            {
                if (r == 0 || c == 0)
                    Console.WriteLine("Bad finder");
                col = c; rowSet = r; 
            }
            internal Finder Relocate(Context cx)
            {
                var c = cx.obuids[col]??col;
                var r = cx.rsuids[rowSet]??rowSet;
                return (c != col || r != rowSet) ? new Finder(c, r) : this;
            }
            internal Finder Relocate(Writer wr)
            {
                var c = wr.Fix(col);
                var r = wr.rss.Contains(rowSet) ? wr.rss[rowSet].defpos
                    : (rowSet==wr.curs) ? wr.Fix(wr.curs)
                    : ((RowSet)wr.cx.data[rowSet]._Relocate(wr)).defpos;
                return (c != col || r != rowSet) ? new Finder(c, r) : this;
            }
            public override string ToString()
            {
                return Uid(rowSet) + "[" + Uid(col) + "]";
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
        internal BTree<long, Finder> finder =>
            (BTree<long, Finder>)mem[_Finder] ?? BTree<long, Finder>.Empty;
        internal int display => domain.display;
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dp">The uid for the RowSet</param>
        /// <param name="rt">The way for a cursor to calculate a row</param>
        /// <param name="kt">The way for a cursor to calculate a key (by default same as rowType)</param>
        /// <param name="or">The ordering of rows in this rowset</param>
        /// <param name="wh">A set of boolean conditions on row values</param>
        /// <param name="ma">A filter for row column values</param>
        protected RowSet(long dp, Context cx, Domain dt,
            BTree<long, Finder> fin = null, CList<long> kt = null,
            BTree<long, bool> wh = null, CList<long> or = null,
            BTree<long, TypedValue> ma = null,
            BTree<long, BTree<long, bool>> mg = null,
            GroupSpecification gp = null,BTree<long,object> m=null) 
            : base(dp,_Fin(dp,cx,fin,dt.rowType,gp,m)
                +(_Domain,dt)+(Index.Keys,kt??dt.rowType)
                +(RowOrder,or??CList<long>.Empty)
                +(Query.Where, wh??BTree<long,bool>.Empty)
                +(Query._Matches,ma??BTree<long, TypedValue>.Empty)
                + (Query.Matching,mg ?? BTree<long, BTree<long, bool>>.Empty))
        {
            cx.data += (dp, this);
            if (cx.db.parse==ExecuteStatus.Obey)
                cx.result = this;
        }
        protected RowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        protected RowSet(Context cx,RowSet rs,BTree<long,Finder>nd, bool bt) 
            :this(rs.defpos,rs.mem+(_Needed,nd)+(Built,bt))
        {
            cx.data += (rs.defpos, this);
            if (cx.db.parse == ExecuteStatus.Obey)
                cx.result = this;
        }
        static BTree<long,object> _Fin(long dp,Context cx,BTree<long,Finder> fin,CList<long> rt,
            GroupSpecification gp,BTree<long,object>m)
        {
            fin = fin ?? BTree<long, Finder>.Empty;
            for (var b = rt.First(); b != null; b = b.Next())
            {
                var p = b.value();
                if (cx.obs[p] is SqlValue sv && sv.alias is string a
                    && cx.defs.Contains(a))
                    fin += (cx.defs[a].Item1, new Finder(p, dp));
                fin += (p, new Finder(p, dp));
            }
            var r = (m??BTree<long,object>.Empty) +(_Finder,fin);
            if (gp != null)
                r += (TableExpression.Group, gp.defpos);
            return r;
        }
        protected static BTree<long,TypedValue> _Matches(Context cx,BTree<long,bool> wh)
        {
            var r = BTree<long, TypedValue>.Empty;
            for (var b=wh.First();b!=null;b=b.Next())
                r = cx.obs[b.key()].AddMatch(cx,r);
            return r;
        }
        internal abstract RowSet New(Context cx, BTree<long, Finder> nd, bool bt);
        internal virtual bool TableColsOk => false;
        public static RowSet operator+(RowSet rs,(long,object)x)
        {
            return (RowSet)rs.New(rs.mem+x);
        }
        internal virtual RowSet Build(Context cx)
        {
            if (needed == null)
                throw new PEException("PE422");
            return New(cx,needed,true);
        }
        internal RowSet ComputeNeeds(Context cx)
        {
            if (needed != null)
                return this;
            var nd = BTree<long, Finder>.Empty;
            nd = cx.Needs(nd, this, rt);
            nd = cx.Needs(nd, this, keys);
            nd = cx.Needs(nd, this, rowOrder);
            nd = AllWheres(cx, nd);
            if (!(this is IndexRowSet || this is TableRowSet))
                nd = AllMatches(cx, nd);
            return New(cx, nd, false); 
        }
        /// <summary>
        /// Deal with RowSet prequisites (a kind of implementation of LATERAL).
        /// We can't calculate these earlier since the source rowsets haven't been constructed.
        /// Cursors will contain corresponding values of prerequisites.
        /// Once First()/Next() has been called, we need to see if they
        /// have been evaluated/changed. Either we can't or we must build this RowSet.
        /// </summary>
        /// <param name="was"></param>
        /// <returns></returns>
        internal RowSet MaybeBuild(Context cx,BTree<long,TypedValue>was=null)
        {
            var r = this;
            // We cannot Build if Needs not met by now.
            if (was == null || this is RestRowSet)
            {
                var ox = cx.from;
                r = ComputeNeeds(cx); // install nd immediately in case Build looks for it
                var bd = true;
                for (var b = r.needed.First(); b != null; b = b.Next())
                {
                    var p = b.key();
                    cx.from += (p,b.value()); // enable build
                    if (cx.obs[p].Eval(cx) == null)
                        bd = false;
                }
                cx.from = ox;
                if (bd) // all prerequisites are ok
                    r = r.Build(cx);  // we can Build (will set built to true)
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
                    r = r.Build(cx); // we must Build again
                //    cx.cursors += (r.defpos,r._First(cx));
                    return r;
                }
            }
            // Otherwise we don't need to
            return r;
        }
        protected void Fixup(Context cx,RowSet now)
        {
            var fi = BTree<long, Finder>.Empty;
            for (var b = finder.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var f = b.value();
                if (f.rowSet == defpos)
                    fi += (p, new Finder(f.col, now.defpos));
                else
                    fi += (p,f);
            }
            now += (_Finder, fi);
            cx.data += (now.defpos, now);
            for (var b = cx.data.First(); b != null; b = b.Next())
                if (b.value() == this)
                    cx.data += (b.key(), now);  
        }
        internal override Basis Fix(Context cx)
        {
            var r = (RowSet)Relocate(cx.rsuids[defpos]??defpos);
            var dm = (Domain)domain.Fix(cx);
            var gs = (GroupSpecification)
                cx.obs[cx.obuids[groupSpec]??groupSpec]?.Fix(cx);
            r += (_Domain,dm);
            r += (_Finder, cx.Fix(finder));
            r += (Index.Keys, cx.Fix(keys));
            r += (RowOrder, cx.Fix(rowOrder));
            r += (Query.Where, cx.Fix(where));
            r += (Query._Matches, cx.Fix(matches));
            r += (Query.Matching, cx.Fix(matching));
            if (gs!=null)
                r += (TableExpression.Group, gs.defpos);
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            if (wr.rss.Contains(defpos))
                return wr.rss[defpos];
            var r = (RowSet)Relocate(wr.Fix(defpos));
            var oc = wr.curs;
            wr.curs = defpos;
            var dm = (Domain)domain._Relocate(wr);
            r += (_Domain, dm);
            r += (_Finder, wr.Fix(finder));
            r += (Index.Keys, wr.Fix(keys));
            r += (RowOrder, wr.Fix(rowOrder));
            r += (Query.Where, wr.Fix(where));
            r += (Query._Matches, wr.Fix(matches));
            r += (Query.Matching, wr.Fix(matching));
            r += (TableExpression.Group, wr.Fix(groupSpec));
            wr.rss += (defpos, r);
            wr.curs = oc;
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (RowSet)base._Replace(cx,so,sv);
            var dm = (Domain)domain._Replace(cx,so,sv);
            r += (_Domain, dm);
            r += (_Finder, cx.Replaced(finder));
            r += (Index.Keys, cx.Replaced(keys));
            r += (RowOrder, cx.Replaced(rowOrder));
            r += (Query.Where, cx.Replaced(where));
            r += (Query._Matches, cx.Replaced(matches));
            r += (Query.Matching, cx.Replaced(matching));
            if (cx.done.Contains(groupSpec))
                r += (TableExpression.Group, cx.done[groupSpec].defpos);
            cx.data += (defpos, r);
            return r;
        }
        public string NameFor(Context cx, int i)
        {
            var p = rt[i];
            var sv = cx.obs[p];
            var n = sv?.alias ?? (string)sv?.mem[Basis.Name];
            return cx.Inf(p)?.name ?? n??("Col"+i);
        }
        public virtual Rvv _Rvv(Context cx)
        {
            return Rvv.Empty;
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
                var dc = domain.representation[cp];
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
                throw new PEException("PE198");
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
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            sb.Append(' ');sb.Append(Uid(defpos));
            var cm = "(";
            for (var b = rt.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.value()));
            }
            sb.Append(")"); 
            if (keys!=rt && keys.Count!=0)
            {
                cm = " key (";
                for (var b = keys.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.value()));
                }
                sb.Append(")");
            }
            cm = "(";
            sb.Append(" Finder: ");
            for (var b=finder.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = "],";
                sb.Append(Uid(b.key()));
                var f = b.value();
                sb.Append("="); sb.Append(Uid(f.rowSet));
                sb.Append('['); sb.Append(Uid(f.col));
            }
            sb.Append("])");
            return sb.ToString();
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
            for (var b=rs.needed?.First();b!=null;b=b.Next())
            {
                var fi = b.value();
                nd += (b.key(), _cx.cursors[fi.rowSet][fi.col]);
            }
            _needed = nd;
            _cx.cursors += (rs.defpos, this);
            PyrrhoServer.Debug(1, GetType().Name);
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
        internal const long
            Singleton = -405; //TRow
        internal TRow row => (TRow)mem[Singleton];
        internal TrivialRowSet(long dp, Context cx, TRow r, long rc=-1L, BTree<long,Finder> fi=null)
            : base(dp, cx, r.dataType,fi,
                  null,null,null,null,null,null,new BTree<long,object>(Singleton,r))
        { }
        internal TrivialRowSet(long dp,Context cx, Record rec) 
            : this(dp,cx, new TRow(((Table)cx.db.objects[rec.tabledefpos]).domain, rec.fields), 
                  rec.defpos)
        { }
        protected TrivialRowSet(Context cx, RowSet rs, BTree<long, Finder> nd, bool bt)
            : base(cx, rs, nd, bt) { }
        protected TrivialRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override RowSet New(Context cx,BTree<long,Finder> nd,bool bt)
        {
            return new TrivialRowSet(cx,this,nd,bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new TrivialRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        internal override bool TableColsOk => true;
        public static TrivialRowSet operator +(TrivialRowSet rs, (long, object) x)
        {
            return (TrivialRowSet)rs.New(rs.mem + x);
        }
        protected override Cursor _First(Context _cx)
        {
            return new TrivialCursor(_cx,this,-1L);
        }
        public override Cursor PositionAt(Context _cx,PRow key)
        {
            for (int i = 0; key != null; i++, key = key._tail)
                if (row[i].CompareTo(key._head) != 0)
                    return null;
            return new TrivialCursor(_cx, this, -1L);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TrivialRowSet(dp,mem);
        }
        internal override Basis Fix(Context cx)
        {
            var r = (TrivialRowSet)base.Fix(cx);
            if (row!=null)
                r += (Singleton, row.Fix(cx));
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TrivialRowSet(defpos,m);
        }

        internal class TrivialCursor : Cursor
        {
            readonly TrivialRowSet trs;
            internal TrivialCursor(Context _cx, TrivialRowSet t,long d) 
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
        internal const long
            SQMap = -408; // BTree<long,Finder> TableColumn
        internal long source => (long)(mem[From.Source]??-1L);
        internal BTree<long, Finder> sQMap =>
            (BTree<long, Finder>)mem[SQMap];
        /// <summary>
        /// This constructor builds a rowset for the given Table
        /// directly using its defpos, rowType, ordering, where and match info.
        /// Suggestion here is to use the source keyType. Maybe the source ordering too?
        /// </summary>
        internal SelectedRowSet(Context cx,Query q,RowSet r,BTree<long,Finder> fi)
            :base(q.defpos,cx,q.domain,fi,null,q.where,
                 q.ordSpec,q.matches,null,null,_Fin(cx,q,fi,q.mem+(From.Source,r.defpos)))
        { }
        SelectedRowSet(Context cx, SelectedRowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        { }
        protected SelectedRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long, object> _Fin(Context cx, Query q, BTree<long,Finder>fi,
            BTree<long,object>m)
        {
            for (var b = q.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                if (cx.obs[p] is SqlCopy sc)
                    p = sc.copyFrom;
                fi += (b.value(), new Finder(p, q.defpos));
            }
            return m + (SQMap, fi);
        } 
        internal override Basis New(BTree<long, object> m)
        {
            return new SelectedRowSet(defpos, m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new SelectedRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        public override Rvv _Rvv(Context cx)
        {
            return cx.data[source]._Rvv(cx);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SelectedRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SelectedRowSet)base._Replace(cx, so, sv);
            r += (SQMap, cx.Replaced(sQMap));
            return r;
        }
        internal override RowSet New(Context cx,BTree<long,Finder> nd,bool bt)
        {
            return new SelectedRowSet(cx,this,nd,bt);
        }
        public static SelectedRowSet operator +(SelectedRowSet rs, (long, object) x)
        {
            return (SelectedRowSet)rs.New(rs.mem + x);
        }
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        internal override RowSet Build(Context cx)
        {
            for (var i=0; i<rt.Length;i++)
                cx.obs[rt[i]]?.Build(cx,this);
            return built?this:New(cx,needed,true);
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
        internal override Basis Fix(Context cx)
        {
            var r = (SelectedRowSet)base.Fix(cx);
            r += (From.Source, cx.rsuids[source]??source);
            r += (SQMap, cx.Fix(sQMap));
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SelectedRowSet)base._Relocate(wr);
            r += (From.Source, wr.Fix(source));
            r += (SQMap, wr.Fix(sQMap));
            wr.cx.data += (r.defpos, r);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Source: "); sb.Append(Uid(source));
            sb.Append(" SQMap: "); sb.Append(sQMap);
            return sb.ToString();
        }
        internal class SelectedCursor : Cursor
        {
            readonly SelectedRowSet _srs;
            internal readonly Cursor _bmk; // for rIn
            internal SelectedCursor(Context _cx,SelectedRowSet srs, Cursor bmk, int pos) 
                : base(_cx,srs, pos, bmk._defpos, new TRow(srs.domain,srs.sQMap,bmk.values)) 
            {
                _bmk = bmk;
                _srs = srs;
            }
            SelectedCursor(SelectedCursor cu,Context cx,long p,TypedValue v)
                :base(cu,cx,cu._srs.finder[p].col,v)
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
        internal long source => (long)(mem[From.Source]??-1L);
        /// <summary>
        /// This constructor builds a rowset for the given QuerySpec
        /// directly using its defpos, rowType, ordering, where and match info.
        /// Note we cannot assume that columns are simple SqlCopy.
        /// </summary>
        internal SelectRowSet(Context cx, QuerySpecification q, RowSet r)
            : base(q.defpos, cx, q.domain, r.finder, null, q.where, q.ordSpec, 
                  q.matches,q.matching,null,new BTree<long,object>(From.Source,r.defpos))
        { }
        SelectRowSet(Context cx, SelectRowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {  }
        protected SelectRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override RowSet New(Context cx, BTree<long, Finder> nd,bool bt)
        {
            return new SelectRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new SelectRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SelectRowSet(defpos,m);
        }
        public override Rvv _Rvv(Context cx)
        {
            return cx.data[source]._Rvv(cx);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SelectRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SelectRowSet)base._Replace(cx, so, sv);
            r += (From.Source, cx.Replace(source, so, sv));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SelectRowSet)base.Fix(cx);
            r += (From.Source, cx.rsuids[source]??source);
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SelectRowSet)base._Relocate(wr);
            r += (From.Source, wr.Fix(source));
            return r;
        }
        public static SelectRowSet operator +(SelectRowSet rs, (long, object) x)
        {
            return (SelectRowSet)rs.New(rs.mem + x);
        }
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        internal override RowSet Build(Context cx)
        {
            for (var i=0;i<rt.Length;i++)
                cx.obs[rt[i]].Build(cx, this);
            return built?this:New(cx,needed,true);
        }
        protected override Cursor _First(Context _cx)
        {
            return SelectCursor.New(_cx, this);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Source: "); sb.Append(Uid(source));
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
                for (var b = srs.rt.First(); b != null; b = b.Next())
                {
                    var s = cx.obs[b.value()]; 
                    var v = s?.Eval(cx)??TNull.Value;
                    if (v == TNull.Value && bmk[b.value()] is TypedValue tv
                        && tv != TNull.Value)
                        v = tv;  // happens for SqlFormal e.g. in LogRowsRowSet 
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
                    for (var b = _srs.domain.representation.First(); b != null; b = b.Next())
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
		internal long source => (long)(mem[From.Source] ?? -1L);
        /// <summary>
        /// The having search condition for the aggregation
        /// </summary>
		internal BTree<long,bool> having =>
            (BTree<long,bool>)mem[TableExpression.Having]??BTree<long,bool>.Empty;
        internal TRow row => (TRow)mem[TrivialRowSet.Singleton];
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
            : base(q.defpos, cx, q.domain, rs.finder, null, null,
                  q.ordSpec, q.matches, q.matching,null,
                  BTree<long,object>.Empty+(From.Source,rs.defpos)
                  +(TableExpression.Having,q.where))
        { }
        protected EvalRowSet(Context cx,EvalRowSet rs,BTree<long,Finder> nd,TRow rw) 
            :base(cx,rs+ (TrivialRowSet.Singleton, rw), nd,true)
        {
            cx.values += (defpos, rw);
        }
        EvalRowSet(Context cx, EvalRowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {  } 
        protected EvalRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new EvalRowSet(defpos, m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new EvalRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new EvalRowSet(cx, this, nd, bt);
        }
        public static EvalRowSet operator +(EvalRowSet rs, (long, object) x)
        {
            return (EvalRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new EvalRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (EvalRowSet)base._Replace(cx, so, sv);
            r += (From.Source, cx.Replace(source, so, sv));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (EvalRowSet)base.Fix(cx);
            r += (From.Source, cx.rsuids[source]??source);
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (EvalRowSet)base._Relocate(wr);
            r += (From.Source, wr.Fix(source));
            return r;
        }
        protected override Cursor _First(Context _cx)
        {
            return new EvalBookmark(_cx,this);
        }

        internal override RowSet Build(Context _cx)
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
            return new EvalRowSet(_cx,this,needed,new TRow(domain,vs));
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
        internal long tabledefpos => (long)(mem[SqlInsert._Table]??-1L);
        /// <summary>
        /// Constructor: a rowset defined by a base table without a primary key.
        /// Independent of role, user, command.
        /// Context must have a suitable tr field
        /// </summary>
        internal TableRowSet(Context cx, long t,BTree<long,Finder>fi,BTree<long,bool> wh)
            : base(t, cx, cx.Inf(t).domain,fi,null,null,null,_Matches(cx,wh),null,
                  null,new BTree<long,object>(SqlInsert._Table,t))
        { }
        TableRowSet(Context cx, TableRowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {  }
        protected TableRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new TableRowSet(defpos,m);
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new TableRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new TableRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        public static TableRowSet operator+(TableRowSet rs,(long,object)x)
        {
            return (TableRowSet)rs.New(rs.mem + x);
        }
        internal override bool TableColsOk => true;
        public override Rvv _Rvv(Context cx)
        {
            return Rvv.Empty+cx.cursors[defpos].Rec();
        }
        internal override DBObject Relocate(long dp)
        {
            return new TableRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (TableRowSet)base._Replace(cx, so, sv);
            r += (SqlInsert._Table, cx.Replace(tabledefpos, so, sv));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (TableRowSet)base.Fix(cx);
            r += (SqlInsert._Table, cx.obuids[tabledefpos]??tabledefpos);
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (TableRowSet)base._Relocate(wr);
            r += (SqlInsert._Table, wr.Fix(tabledefpos));
            return r;
        }
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
                for (var b=trs.domain.representation.First();b!=null;b=b.Next())
                {
                    var p = b.key();
                    vs += (p, rw.vals[p]);
                }
                return new TRow(trs.domain, vs);
            }
            internal static TableCursor New(Context _cx, TableRowSet trs)
            {
                var table = _cx.db.objects[trs.tabledefpos] as Table;
                for (var b = table.tableRows.First(); b != null; b = b.Next())
                {
                    var rec = b.value();
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) &&
                        _cx.db.user != null && _cx.db.user.defpos != table.definer
                         && _cx.db._user != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new TableCursor(_cx, trs, table, 0, b);
                }
                return null;
            }
            protected override Cursor _Next(Context _cx)
            {
                var bmk = _bmk;
                var table = _table;
                for (;;) 
                {
                    if (bmk != null)
                        bmk = bmk.Next();
                    if (bmk == null)
                        return null;
                    var rec = bmk.value();
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) && 
                        _cx.db._user!=table.definer && _cx.db._user!=_cx.db.owner
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
        internal const long
            _Index = -410, // long Index
            IxTable = -409; // long Table
        internal long table =>(long)(mem[IxTable]??-1L);
        /// <summary>
        /// The Index to use
        /// </summary>
        internal long index => (long)(mem[_Index]??-1L);
        /// <summary>
        /// Constructor: A rowset for a table using a given index. 
        /// Unusally, the rowSet defpos is in the index's defining position,
        /// as the INRS is independent of role, user, command.
        /// Context must have a suitable tr field.
        /// </summary>
        /// <param name="f">the from part</param>
        /// <param name="x">the index</param>
        internal IndexRowSet(Context cx, Table tb, Index x, BTree<long,Finder>fi, 
            BTree<long,TypedValue> fl,long? dp=null, BTree<long,object>m=null) 
            : base(dp??x.defpos,cx,cx.Inf(tb.defpos).domain,fi,null,null,null,
                  fl,null,null,
                  (m??BTree<long,object>.Empty)+(IxTable,tb.defpos)+(_Index,x.defpos))
        { }
        protected IndexRowSet(Context cx, IndexRowSet irs, BTree<long, Finder> nd, bool bt)
            : base(cx, irs, nd, bt)
        { }
        protected IndexRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new IndexRowSet(defpos,m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new IndexRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new IndexRowSet(cx, this, nd, bt);
        }
        public static IndexRowSet operator+(IndexRowSet rs,(long,object)x)
        {
            return (IndexRowSet)rs.New(rs.mem + x);
        }
        internal override bool TableColsOk => true;
        public override Rvv _Rvv(Context cx)
        {
            var r = Rvv.Empty;
            if (cx.cursors[defpos]?.Rec() is TableRow t)
                r += t;
            return r;
        }
        internal override DBObject Relocate(long dp)
        {
            return new IndexRowSet(dp,mem);
        }
        internal override Basis Fix(Context cx)
        {
            var r = (IndexRowSet)base.Fix(cx);
            var ch = r != this;
            r += (_Index, cx.obuids[index]??index);
            r += (IxTable, cx.obuids[table]??table);
            if ((!ch) && r.index == index && r.table == table)
                r = this;
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (IndexRowSet)base._Replace(cx, so, sv);
            r += (_Index, cx.Replace(index, so, sv));
            r += (IxTable, cx.Replace(table, so, sv));
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (IndexRowSet)base._Relocate(wr);
            var ch = r != this;
            r += (_Index, wr.Fix(index));
            r += (IxTable, wr.Fix(table));
            if (ch || r.index != index || r.table != table)
                wr.cx.obs += (r.defpos, r);
            return r;
        }
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
            protected IndexCursor(Context _cx, IndexRowSet irs, int pos, MTreeBookmark bmk, TableRow trw,
                PRow key=null)
                : base(_cx, irs, pos, trw.defpos, new TRow(irs.domain, trw.vals))
            {
                _bmk = bmk; _irs = irs; _rec = trw; _key = key;
            }
            protected IndexCursor(IndexCursor cu,Context cx,long p,TypedValue v):base(cu,cx,p,v)
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
                var tb = (Table)_cx.db.objects[_irs.table];
                return new IndexCursor(_cx,_irs, _pos + 1, mb, tb.tableRows[mb.Value().Value]);
            }
            internal static IndexCursor New(Context _cx,IndexRowSet irs, PRow key = null)
            {
                var _irs = irs;
                var table = (Table)_cx.db.objects[_irs.table];
                var index = (Index)_cx.db.objects[_irs.index];
                for (var bmk = index.rows.PositionAt(key); bmk != null;
                    bmk = bmk.Next())
                {
                    var iq = bmk.Value();
                    if (!iq.HasValue)
                        continue;
                    var rec = table.tableRows[iq.Value];
                    if (rec == null || (table.enforcement.HasFlag(Grant.Privilege.Select)
                        && _cx.db._user != table.definer
                         && _cx.db._user != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification)))
                        continue;
                    return new IndexCursor(_cx,_irs, 0, bmk, rec, key);
                }
                return null;
            }
            protected override Cursor _Next(Context _cx)
            {
                var bmk = _bmk;
                var _table = (Table)_cx.db.objects[_irs.table];
                for (; ; )
                {
                    bmk = bmk.Next();
                    if (bmk == null)
                        return null;
                    if (!bmk.Value().HasValue)
                        continue;
                    var rec = _table.tableRows[bmk.Value().Value];
                    if (_table.enforcement.HasFlag(Grant.Privilege.Select)
                        && _cx.db._user != _table.definer
                         && _cx.db._user != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new IndexCursor(_cx, _irs, _pos + 1, bmk, rec);
                }
            }
        }
    }
    /// <summary>
    /// A RowSet defined by an Index (e.g. the primary key for a table)
    /// </summary>
    internal class FilterRowSet : IndexRowSet
    {
        internal const long
            IxFilter = -411; // PRow
        internal PRow filter => (PRow)mem[IxFilter];
        /// <summary>
        /// Constructor: A rowset for a table using a given index. 
        /// Unusally, the rowSet defpos is in the index's defining position,
        /// as the INRS is independent of role, user, command.
        /// Context must have a suitable tr field.
        /// </summary>
        /// <param name="f">the from part</param>
        /// <param name="x">the index</param>
        internal FilterRowSet(Context cx, Table tb, Index x, PRow filt, BTree<long, Finder> fi)
            : base(cx, tb, x, fi, null, cx.nextHeap++,BTree<long, object>.Empty + (IxFilter, filt))
        { }
        FilterRowSet(Context cx, FilterRowSet irs, BTree<long, Finder> nd, bool bt)
            : base(cx, irs, nd, bt)
        { }
        protected FilterRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        internal override Basis New(BTree<long, object> m)
        {
            return new FilterRowSet(defpos, m);
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new FilterRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new FilterRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        public static FilterRowSet operator +(FilterRowSet rs, (long, object) x)
        {
            return (FilterRowSet)rs.New(rs.mem + x);
        }
        internal override bool TableColsOk => true;
        internal override DBObject Relocate(long dp)
        {
            return new FilterRowSet(dp, mem);
        }
        internal override Basis Fix(Context cx)
        {
            var r = (FilterRowSet)base.Fix(cx);
            var f = r.filter.Fix(cx);
            if (f != filter)
                r += (IxFilter, f);
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (FilterRowSet)base._Relocate(wr);
            var f = wr.Fix(r.filter);
            if (f != filter)
                r += (IxFilter, f);
            return r;
        }
        protected override Cursor _First(Context _cx)
        {
            return FilterCursor.New(_cx, this);
        }
        /// <summary>
        /// We assume the key matches our key type, and that the filter is null
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public override Cursor PositionAt(Context _cx, PRow key)
        {
            return FilterCursor.New(_cx, this, key);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Filter ("); sb.Append(filter); sb.Append(")");
            return sb.ToString();
        }
        internal class FilterCursor : IndexCursor
        {
            internal readonly FilterRowSet _frs;
            FilterCursor(Context _cx, FilterRowSet frs, int pos, MTreeBookmark bmk, TableRow trw,
                PRow key = null)
                : base(_cx, frs, pos, bmk, trw, frs.filter)
            {
                _frs = frs;
            }
            FilterCursor(FilterCursor cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
            {
               _frs = cu._frs; 
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new FilterCursor(this, cx, p, v);
            }
            internal override TableRow Rec()
            {
                return base.Rec();
            }
            public override Cursor ResetToTiesStart(Context _cx, MTreeBookmark mb)
            {
                var tb = (Table)_cx.db.objects[_frs.table];
                return new FilterCursor(_cx, _frs, _pos + 1, mb, tb.tableRows[mb.Value().Value]);
            }
            internal static FilterCursor New(Context _cx, FilterRowSet irs, PRow key = null)
            {
                var _irs = irs;
                var table = (Table)_cx.db.objects[_irs.table];
                var index = (Index)_cx.db.objects[_irs.index];
                for (var bmk = index.rows.PositionAt(key ?? irs.filter); bmk != null;
                    bmk = bmk.Next())
                {
                    var iq = bmk.Value();
                    if (!iq.HasValue)
                        continue;
                    var rec = table.tableRows[iq.Value];
                    if (rec == null || (table.enforcement.HasFlag(Grant.Privilege.Select)
                        && _cx.db.user.defpos != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification)))
                        continue;
                    return new FilterCursor(_cx, _irs, 0, bmk, rec, key ?? irs.filter);
                }
                return null;
            }
            protected override Cursor _Next(Context _cx)
            {
                var bmk = _bmk;
                var _table = (Table)_cx.db.objects[_frs.table];
                for (; ; )
                {
                    bmk = bmk.Next();
                    if (bmk == null)
                        return null;
                    if (!bmk.Value().HasValue)
                        continue;
                    var rec = _table.tableRows[bmk.Value().Value];
                    if (_table.enforcement.HasFlag(Grant.Privilege.Select)
                        && _cx.db.user.defpos != _cx.db.owner
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new FilterCursor(_cx, _frs, _pos + 1, bmk, rec);
                }
            }
        }
    }
    /// <summary>
    /// A rowset for distinct values
    /// </summary>
    internal class DistinctRowSet : RowSet
    {
        internal MTree mtree => (MTree)mem[Index.Tree];
        internal long source => (long)(mem[From.Source]??-1L);
        /// <summary>
        /// constructor: a distinct rowset
        /// </summary>
        /// <param name="r">a source rowset</param>
        internal DistinctRowSet(Context _cx,RowSet r) 
            : base(_cx.GetUid(),_cx,r.domain,r.finder,r.keys,
                  r.where,null,null,null,null,new BTree<long,object>(From.Source,r.defpos))
        { }
        protected DistinctRowSet(Context cx,DistinctRowSet rs,BTree<long,Finder> nd,MTree mt) 
            :base(cx,rs+(Index.Tree,mt),nd,true)
        { }
        DistinctRowSet(Context cx, DistinctRowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {  }
        protected DistinctRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new DistinctRowSet(defpos,m);
        }
        internal override RowSet New(Context cx, BTree<long,Finder> nd, bool bt)
        {
            return new DistinctRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new DistinctRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        public static DistinctRowSet operator+(DistinctRowSet rs,(long,object)x)
        {
            return (DistinctRowSet)rs.New(rs.mem + x);
        }
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        internal override DBObject Relocate(long dp)
        {
            return new DistinctRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (DistinctRowSet)base._Replace(cx, so, sv);
            r += (Index.Tree, mtree.Replaced(cx,so,sv));
            r += (From.Source, cx.Replace(source, so, sv));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (DistinctRowSet)base.Fix(cx);
            r += (From.Source, cx.rsuids[source]??source);
            if (mtree!=null)
               r += (Index.Tree, new MTree(mtree.info.Fix(cx)));
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (DistinctRowSet)base._Relocate(wr);
            r += (From.Source, wr.Fix(source));
            if (mtree != null)
                r += (Index.Tree, new MTree(mtree.info.Relocate(wr)));
            return r;
        }
        internal override RowSet Build(Context cx)
        {
            var ds = BTree<long,DBObject>.Empty;
            var sce = cx.data[source];
            for (var b = sce.keys.First(); b != null; b = b.Next())
            {
                var sv = (SqlValue)cx.obs[b.value()];
                ds += (sv.defpos,sv);
            }
            var mt = new MTree(new TreeInfo(sce.keys, ds, TreeBehaviour.Ignore, TreeBehaviour.Ignore));
            var rs = BTree<int, TRow>.Empty;
            for (var a = sce.First(cx); a != null; a = a.Next(cx))
            {
                var vs = BList<TypedValue>.Empty;
                for (var ti = mt.info; ti != null; ti = ti.tail)
                    vs+= a[ti.head];
                MTree.Add(ref mt, new PRow(vs), 0);
                rs += ((int)rs.Count, a);
            }
            return new DistinctRowSet(cx,this,needed,mt);
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
                :base(cx,drs,pos,0,new TRow(drs.domain,bmk.key()))
            {
                _bmk = bmk;
                _drs = drs;
            }
            internal static DistinctCursor New(Context cx,DistinctRowSet drs)
            {
                var ox = cx.from;
                cx.from += cx.data[drs.source].finder;
                if (drs.mtree == null)
                    throw new DBException("20000", "Distinct RowSet not built?");
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
        internal const long
            _RTree = -412; // RTree
        internal RTree tree => (RTree)mem[_RTree];
        internal long source => (long)(mem[From.Source]??-1L);
        internal bool distinct => (bool)(mem[QuerySpecification.Distinct]??false);
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        public OrderedRowSet(Context _cx,RowSet r,CList<long> os,bool dct)
            :base(_cx.nextHeap++, _cx, r.domain,r.finder,os,r.where,
                 os,r.matches,null,null,new BTree<long,object>(From.Source,r.defpos)
                 +(QuerySpecification.Distinct,dct))
        { }
        protected OrderedRowSet(Context cx,OrderedRowSet rs,BTree<long,Finder> nd,RTree tr) 
            :base(cx,rs+(_RTree,tr),nd,true)
        { }
        OrderedRowSet(Context cx, OrderedRowSet rs, BTree<long, Finder> nd, bool bt)
            : base(cx, rs, nd, bt)
        { }
        protected OrderedRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new OrderedRowSet(defpos, m);
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new OrderedRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new OrderedRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        public static OrderedRowSet operator+(OrderedRowSet rs,(long,object)x)
        {
            return (OrderedRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new OrderedRowSet(dp, mem);
        }
        internal override Basis Fix(Context cx)
        {
            var r = (OrderedRowSet)base.Fix(cx);
            r += (From.Source, cx.rsuids[source]??source);
            if (tree != null)
                r += (_RTree, tree.Fix(cx));
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (OrderedRowSet)base._Relocate(wr);
            r += (From.Source, wr.Fix(source));
            if (tree != null)
                r += (_RTree, tree.Relocate(wr));
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (OrderedRowSet)base._Replace(cx, so, sv);
            if (tree != null)
                r += (_RTree, tree.Replace(cx, so, sv));
            r += (From.Source, cx.Replace(source, so, sv));
            return r;
        }
        internal override RowSet Build(Context cx)
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
            return new OrderedRowSet(cx,this,needed,tree);
        }
        protected override Cursor _First(Context _cx)
        {
            return OrderedCursor.New(_cx, this, RTreeBookmark.New(_cx, tree));
        }
        public override Rvv _Rvv(Context cx)
        {
            return Rvv.Empty+cx.cursors[defpos].Rec();
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
        internal EmptyRowSet(long dp, Context cx,Domain dm) 
            : base(dp, new BTree<long,object>(_Domain,dm)) 
        {
            cx.data += (dp, this);
        }
        EmptyRowSet(Context cx, RowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) { }
        protected EmptyRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new EmptyRowSet(defpos,m);
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new EmptyRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new EmptyRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        public static EmptyRowSet operator+(EmptyRowSet rs,(long,object)x)
        {
            return (EmptyRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new EmptyRowSet(dp,mem);
        }
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
        internal const long
            SqlRows = -413; // BList<long>  SqlRow
        internal BList<long> sqlRows =>
            (BList<long>)mem[SqlRows]??BList<long>.Empty;
        internal SqlRowSet(long dp,Context cx,Domain xp, BList<long> rs) 
            : base(dp, cx, xp,null, null,null,null,null,null,null,
                  new BTree<long,object>(SqlRows,rs))
        { }
        SqlRowSet(Context cx, SqlRowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        {  }
        protected SqlRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlRowSet(defpos,m);
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new SqlRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.TransPos)
                return (RowSet)New(m);
            var rs = new SqlRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        public static SqlRowSet operator +(SqlRowSet rs, (long, object) x)
        {
            return (SqlRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (SqlRowSet)base._Replace(cx, so, sv);
            r += (SqlRows, cx.Replaced(sqlRows));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (SqlRowSet)base.Fix(cx);
            r += (SqlRows, cx.Fix(sqlRows));
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (SqlRowSet)base._Relocate(wr);
            r += (SqlRows, wr.Fix(sqlRows));
            return r;
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
                : base(cx,rs,pos,0,_Row(cx,rs,bmk.key()))
            {
                _srs = rs;
                _bmk = bmk;
            }
            SqlCursor(SqlCursor cu,Context cx,long p,TypedValue v):base(cu,cx,p,v)
            {
                _srs = cu._srs;
            }
            static TRow _Row(Context cx,SqlRowSet rs,int p)
            {
                var rw = (TRow)cx.obs[rs.sqlRows[p]].Eval(cx);
                return new TRow(rs, rw);
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new SqlCursor(this, cx, p, v);
            }
            internal static SqlCursor New(Context _cx,SqlRowSet rs)
            {
                for (var b=rs.sqlRows.First();b!=null;b=b.Next())
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
        internal long source => (long)(mem[From.Source]??-1L);
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        internal TableExpRowSet(long dp, Context cx, CList<long> cs, CList<long> ks, 
            RowSet sc,BTree<long, bool> wh, BTree<long, TypedValue> ma,BTree<long,Finder> fi)
            : base(dp, cx, new Domain(Sqlx.ROW,cx,cs), _Fin(fi,sc), ks, 
                  wh, sc.rowOrder, ma,null,null,
                  new BTree<long,object>(From.Source,sc.defpos)) 
        { }
        TableExpRowSet(Context cx, TableExpRowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        { }
        protected TableExpRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new TableExpRowSet(defpos,m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new TableExpRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        public override Rvv _Rvv(Context cx)
        {
            return cx.data[from]._Rvv(cx);
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
        public static TableExpRowSet operator+(TableExpRowSet rs,(long,object)x)
        {
            return (TableExpRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TableExpRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (TableExpRowSet)base._Replace(cx, so, sv);
            r += (From.Source, cx.Replace(source, so, sv));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (TableExpRowSet)base.Fix(cx);
            r += (From.Source, cx.rsuids[source]??source);
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (TableExpRowSet)base._Relocate(wr);
            r += (From.Source, wr.Fix(source));
            return r;
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
    /// a row set for TRows.
    /// Each TRow has the same domain as the ExplicitRowSet.
    /// </summary>
    internal class ExplicitRowSet : RowSet
    {
        internal const long
            ExplRows = -414; // BList<(long,TRow)> SqlValue
        internal BList<(long,TRow)> explRows =>
            (BList<(long,TRow)>)mem[ExplRows]??BList<(long,TRow)>.Empty;
        /// <summary>
        /// constructor: a set of explicit rows
        /// </summary>
        /// <param name="rt">a row type</param>
        /// <param name="r">a a set of TRows from q</param>
        internal ExplicitRowSet(long dp,Context cx,Domain dt,BList<(long,TRow)>r)
            : base(dp,cx,dt,null,null,null,null,null,null,null,
                  new BTree<long,object>(ExplRows,r))
        { }
        ExplicitRowSet(Context cx, ExplicitRowSet rs, BTree<long, Finder> nd, bool bt) 
            : base(cx, rs, nd, bt) 
        { }
        protected ExplicitRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new ExplicitRowSet(defpos,m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new ExplicitRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd,bool bt)
        {
            return new ExplicitRowSet(cx, this, nd, bt);
        }
        public static ExplicitRowSet operator+(ExplicitRowSet rs,(long,object)x)
        {
            return (ExplicitRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new ExplicitRowSet(dp, mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (ExplicitRowSet)base._Replace(cx, so, sv);
            var s = BList<(long,TRow)>.Empty;
            for (var b=explRows.First();b!=null;b=b.Next())
            {
                var (p, q) = b.value();
                s += (p, (TRow)q.Replaced(cx));
            }
            r += (ExplRows, s);
            return r;
        }
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
                for (var b=explRows.First();b!=null;b=b.Next())
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
                for (var b=ers.explRows.First();b!=null;b=b.Next())
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
    internal class ProcRowSet : RowSet
    {
        internal SqlCall call => (SqlCall)mem[SqlCall.Call];
        internal ProcRowSet(long dp, SqlCall ca, Context cx) :base(dp,cx,ca.domain,null,null,
            null,null,null,null,null,BTree<long,object>.Empty+(_Needed,BTree<long,Finder>.Empty)
            +(Built,true)+(SqlCall.Call,ca))
        { }
        protected ProcRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        ProcRowSet(Context cx,ProcRowSet prs,BTree<long,Finder>nd,bool bt) :base(cx,prs,nd,bt)
        { }
        public static ProcRowSet operator+(ProcRowSet p,(long,object)x)
        {
            return (ProcRowSet)p.New(p.mem + x);
        }
        internal override RowSet Build(Context cx)
        {
            cx.values += (defpos,call.Eval(cx));
            return this;
        }
        protected override Cursor _First(Context cx)
        {
            var v = cx.values[defpos];
            return (v==TNull.Value)?null:ProcRowSetCursor.New(cx, this, (TArray)v);
        }

        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new ProcRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new ProcRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ProcRowSet(defpos, m);
        }

        internal override DBObject Relocate(long dp)
        {
            return new ProcRowSet(dp, mem+(SqlCall.Call,call.Relocate(dp)));
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (ProcRowSet)base._Replace(cx, so, sv);
            r += (SqlCall.Call, cx.done[call.defpos]??call);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Call: "); sb.Append(call);
            return sb.ToString();
        }
        internal class ProcRowSetCursor : Cursor
        {
            readonly ProcRowSet _prs;
            readonly ABookmark<int, TypedValue> _bmk;
            ProcRowSetCursor(Context cx,ProcRowSet prs,int pos,
                ABookmark<int,TypedValue>bmk, TRow rw) 
                : base(cx,prs,bmk.key(),-1,new TRow(prs,rw)) 
            { 
                _prs = prs; _bmk = bmk;
            }
            internal static ProcRowSetCursor New(Context cx,ProcRowSet prs,TArray ta)
            {
                for (var b = ta.list.First();b!=null;b=b.Next())
                {
                    var r = new ProcRowSetCursor(cx, prs, 0, b, (TRow)b.value());
                    if (r.Matches(cx))
                        return r;
                }
                return null;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new ProcRowSetCursor(cx, _prs, _pos, _bmk,
                    this + (p, v));
            }

            protected override Cursor _Next(Context cx)
            {
                for (var b = _bmk.Next(); b != null; b = b.Next())
                {
                    var r = new ProcRowSetCursor(cx, _prs, _pos+1, b, (TRow)b.value());
                    if (r.Matches(cx))
                        return r;
                }
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
    /// trigger actions may affect other tables.
    /// We need to ensure that the TransitionRowSet rowType matches the 
    /// current role's view of the target, although the supplied data may not match this
    /// if named column syntax is used. The rowset finder is used to construct
    /// these starting values for the TransitionCursor.
    /// During trigger processing, the OLD/NEW ROW/TABLE rowTypes will
    /// be taken from the current trigger definer's role. 
    /// The TargetCursor will show the columns as defined by the TargetInfo as defined 
    /// by the target owner's role, and at an early stage we construct mappings from 
    /// the TransitionRowSet rowType to and from this. 
    /// Another nuisance is that the TransitionRowSet must manage autokeys, 
    /// as it may be preparing many new rows in the transaction context.
    /// </summary>
    internal class TransitionRowSet : RowSet
    {
        internal const long
            _Adapters = -429, // Adapters
            _Data = -368, // RowSet
            Defaults = -415, // BTree<long,TypedValue>  SqlValue
            IxDefPos = -420, // long    Index
            Ra = -424,  // TriggerContext
            Rb = -422,  // TriggerContext
            Ri = -423,  // TriggerContext
            Ta = -427,  // TriggerContext
            TargetAc = -430,    // Activation
            TargetInfo = -417, // ObInfo
            TargetTrans = -418, // BTree<long,Finder>   TableColumn
            Tb = -425,  // TriggerContext
            Td = -428,  // TriggerContext
            Ti = -426,  // TriggerContext
            TransTarget = -419, // BTree<long,Finder>   SqlValue
            TriggerType = -421, // PTrigger.TrigType
            TrsFrom = -416; // From (SqlInsert, QuerySearch or UpdateSearch)
        internal BTree<long, TypedValue> defaults =>
            (BTree<long,TypedValue>)mem[Defaults]??BTree<long, TypedValue>.Empty; 
        internal new From from => (From)mem[TrsFrom]; // will be a SqlInsert, QuerySearch or UpdateSearch
        internal RowSet data => (RowSet)mem[_Data];
        internal ObInfo targetInfo => (ObInfo)mem[TargetInfo];
        internal Activation targetAc => (Activation)mem[TargetAc];
        internal BTree<long, Finder> targetTrans => 
            (BTree<long, Finder>)mem[TargetTrans];
        internal BTree<long, Finder> transTarget =>
            (BTree<long, Finder>)mem[TransTarget];
        internal long indexdefpos => (long)(mem[IxDefPos]?? -1L);
        internal PTrigger.TrigType _tgt => (PTrigger.TrigType)mem[TriggerType];
        /// <summary>
        /// There may be several triggers of any type, so we manage a set of transition activations for each.
        /// These are for table before, table instead, table after, row before, row instead, row after.
        /// </summary>
        internal TriggerContext rb => (TriggerContext)mem[Rb];
        internal TriggerContext ri => (TriggerContext)mem[Ri];
        internal TriggerContext ra => (TriggerContext)mem[Ra];
        internal TriggerContext tb => (TriggerContext)mem[Tb];
        internal TriggerContext ti => (TriggerContext)mem[Ti];
        internal TriggerContext ta => (TriggerContext)mem[Ta];
        internal TriggerContext td => (TriggerContext)mem[Td];
        internal Adapters _eqs => (Adapters)mem[_Adapters];
        internal TransitionRowSet(Context cx, From q, RowSet data, PTrigger.TrigType tg, Adapters eqs)
            : base(cx.nextHeap++, cx, q.domain,
                  data?.finder ?? BTree<long, Finder>.Empty, 
                  null, q.where, null, null, null, null, 
                  _Mem(cx.nextHeap-1,cx, q, tg, eqs)+(_Data,data))
        {
            cx.data += (defpos, this);
            targetAc.data = cx.data;
        }
        static BTree<long,object> _Mem(long defpos,Context cx,From from,
            PTrigger.TrigType tg, Adapters eqs)
        {
            var m = BTree<long, object>.Empty;
            m += (TrsFrom, from);
            m += (_Adapters,eqs);
            var tr = cx.db;
            var t = tr.objects[from.target] as Table;
            m += (IxDefPos, t.FindPrimaryIndex(tr)?.defpos ?? -1L);
            // check now about conflict with generated columns
            if (t.Denied(cx, Grant.Privilege.Insert))
                throw new DBException("42105", from);
            var rt = from.rowType; // data rowType
            var targetInfo = (ObInfo)tr.schema.infos[t.defpos];
            var tt = targetInfo.domain.rowType;
            m += (TargetInfo,targetInfo);
            var tgTn = BTree<long, Finder>.Empty;
            var tnTg = BTree<long, Finder>.Empty;
            for (var b = rt.First(); b != null; b = b.Next()) 
            {
                var p = b.value();
                var s = cx.obs[p];
                var c = (s is SqlCopy sc) ? sc.copyFrom : s.defpos;
                tgTn += (c,new Finder(p,defpos));
                tnTg += (p,new Finder(c,from.defpos));
            }
            m += (TargetTrans,tgTn);
            m += (TransTarget,tnTg);
            m += (TargetAc,new Activation(cx, from.name));
            var dfs = BTree<long, TypedValue>.Empty;
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
                        dfs += (tc.defpos, tv);
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
            m += (Defaults, dfs);
            m += (TriggerType,tg);
            // Get the trigger sets 
            if (t.triggers[tg | PTrigger.TrigType.EachStatement
                    | PTrigger.TrigType.Before] is BTree<long,bool> ttb)
                m+=(Tb,Setup(cx, t, defpos, ttb));
            if (t.triggers[tg | PTrigger.TrigType.EachStatement
                    | PTrigger.TrigType.Instead] is BTree<long, bool> tti)
                m += (Ti, Setup(cx, t, defpos, tti)); 
            if (t.triggers[tg | PTrigger.TrigType.EachStatement
                    | PTrigger.TrigType.After] is BTree<long, bool> tta)
                m += (Tb, Setup(cx, t, defpos, tta));
            if (t.triggers[tg | PTrigger.TrigType.Deferred]
                     is BTree<long, bool> ttd)
                m += (Td, Setup(cx, t, defpos, ttd));
            if (t.triggers[tg | PTrigger.TrigType.EachRow
                    | PTrigger.TrigType.Before] is BTree<long, bool> trb)
                m += (Rb, Setup(cx, t, defpos, trb));
            if (t.triggers[tg | PTrigger.TrigType.EachRow
                    | PTrigger.TrigType.Instead] is BTree<long, bool> tri)
                m += (Ri, Setup(cx, t, defpos, tri));
            if (t.triggers[tg | PTrigger.TrigType.EachRow
                    | PTrigger.TrigType.After] is BTree<long, bool> tra)
                m += (Ra, Setup(cx, t, defpos, tra));
            return m;
        }
        protected TransitionRowSet(Context cx,TransitionRowSet rs,BTree<long,Finder> nd,
            bool bt) :base(cx,rs,nd,bt)
        { }
        protected TransitionRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new TransitionRowSet(defpos, m);
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd,bool bt)
        {
            return new TransitionRowSet(cx,this,nd,bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new TransitionRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        internal override bool TableColsOk => true;
        internal override RowSet Source(Context cx)
        {
            return cx.data[from.defpos];
        }
        internal override DBObject Relocate(long dp)
        {
            return new TransitionRowSet(dp, mem);
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
        TargetCursor CheckPrimaryKey(Context cx,TargetCursor tgc,TransitionCursor trc)
        {
            // cx is an Activation
            var ix = (Index)cx.next.db.objects[indexdefpos];
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
                    var c = targetTrans[tc.defpos].col;
                    trc += (cx.next,c,v);
                    cx.next.cursors += (from.defpos, trc);
                    cx.next.values += (c, v);
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
        static TriggerContext Setup(Context _cx,Table tb,long trs,
            BTree<long, bool> tgs)
        {
            var cx = new TriggerContext(_cx,trs,tgs);
            cx.nextHeap = _cx.nextHeap;
            cx.obs = _cx.obs;
            cx.data = _cx.data;
            cx.obs += (tb.defpos,tb);
            for (var b = tb.tblCols.First(); b != null; b = b.Next())
            {
                var tc = (DBObject)_cx.tr.objects[b.key()];
                cx.obs += (tc.defpos,tc);
            }
            return cx;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" From: "); sb.Append(Uid(from.defpos));
            if (data!=null)
            { sb.Append(" Data: ");sb.Append(Uid(data.defpos)); }
            return sb.ToString();
        }
        internal bool? InsertSA(Context _cx)
        { return ta?.Exec(_cx,this); }
        internal bool? InsertSB(Context _cx)
        { return tb?.Exec(_cx, this); }
        internal bool? UpdateSA(Context _cx)
        { return ta?.Exec(_cx,this); }
        internal bool? UpdateSB(Context _cx)
        { return tb?.Exec(_cx, this)==true || ti?.Exec(_cx, this)==true; }
        internal bool? DeleteSB(Context _cx)
        { return tb?.Exec(_cx,this) == true || ti?.Exec(_cx,this)==true; }
        internal bool? DeleteSA(Context _cx)
        { return td?.Exec(_cx, this); }
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
            TransitionCursor(TransitionCursor cu,Context cx,long p,TypedValue v)
                :base(cu,cx,cu._trs.finder[p].col,v)
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
                for (var b=fbm.columns.First(); b!=null;b=b.Next())
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
                                // tv = fn(fbm[j]);
                                var pr = cx.db.objects[m.Value] as Procedure;
                                var ac = new CalledActivation(cx, pr, Domain.Null);
                                tv = cx.obs[pr.body].Eval(ac);
                            }
                        }
                        trc += (cx, b.value(), tv);
                    }
                }
                trc = TargetCursor.New(trs.targetAc, trc, pos)._trsCu;
                return trc;
            }
            internal static TransitionCursor New(Context _cx,TransitionRowSet trs)
            {
                var ox = _cx.from;
                var sce = trs.data; //_cx.data[trs.from.defpos];
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
            internal bool? InsertRA(Context _cx)
            { return _trs.ra?.Exec(_cx,_trs); }
            internal bool? InsertRB(Context _cx)
            { return _trs.rb?.Exec(_cx, _trs)==true ||
                    _trs.ri?.Exec(_cx,_trs)==true; }
            internal bool? UpdateRA(Context _cx)
            { return _trs.ra?.Exec(_cx,_trs); }
            internal bool? UpdateRB(Context _cx)
            { return _trs.rb?.Exec(_cx,_trs) ==true || _trs.ri?.Exec(_cx,_trs)==true; }
            internal bool? DeleteRB(Context _cx)
            { return _trs.rb?.Exec(_cx, _trs)==true|| _trs.ri?.Exec(_cx,_trs)==true; }

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
                    tgc = trs.CheckPrimaryKey(cx, tgc, trc);
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
        internal const long
            Data = -432,        // BTree<long,TableRow> RecPos
            Map = -433,         // BTree<long,Finder>   TableColumn
            RMap = -434,        // BTree<long,Finder>   TableColumn
            Trs = -431;         // TransitionRowSet

        internal TransitionRowSet _trs => (TransitionRowSet)mem[Trs];
        internal BTree<long,TableRow> data => 
            (BTree<long,TableRow>)mem[Data]??BTree<long,TableRow>.Empty;
        internal BTree<long, Finder> map =>
            (BTree<long, Finder>)mem[Map];
        internal BTree<long,Finder> rmap =>
            (BTree<long, Finder>)mem[RMap];
        /// <summary>
        /// Old table: compute the rows that will be in the update/delete
        /// </summary>
        /// <param name="dp"></param>
        /// <param name="cx"></param>
        /// <param name="trs"></param>
        internal TransitionTableRowSet(long dp, Context cx, TransitionRowSet trs)
            : base(dp, cx, trs.domain,trs.finder,null,null,null,
                  null,null,null, _Mem(cx,trs))
        { }
        /// <summary>
        /// New table: Get the new rows from the context
        /// </summary>
        /// <param name="dp"></param>
        /// <param name="cx"></param>
        /// <param name="trs"></param>
        internal TransitionTableRowSet(long dp, Context cx, long trs)
            : base(dp, cx, cx.data[trs].domain,null,null,null,null,
                  null,null,null, _Mem(cx, trs))
        { }
        static BTree<long,object> _Mem(Context cx,TransitionRowSet trs)
        {
            var dat = BTree<long, TableRow>.Empty;
            for (var b = trs.First(cx); b != null; b = b.Next(cx))
                 dat += (b._defpos, b.Rec());
            return BTree<long, object>.Empty
                + (Trs, trs) + (Data, dat) + (Map, trs.targetTrans)
                + (RMap, trs.transTarget);
        }
        static BTree<long,object> _Mem(Context cx,long t)
        {
            var trs = (TransitionRowSet)cx.data[t];
            var dat = BTree<long, TableRow>.Empty;
            if (cx.newTables.Contains(t))
                dat = cx.newTables[t];
            else
                for (var b = trs.First(cx); b != null; b = b.Next(cx))
                    dat += (b._defpos, b.Rec());
            return BTree<long,object>.Empty
                + (Trs,trs) + (Data,dat) + (Map,trs.targetTrans)
                +(RMap,trs.transTarget);
        }
        internal TransitionTableRowSet(Context cx,TransitionTableRowSet rs,
            BTree<long,Finder>nd,bool bt) :base(cx,rs,nd,bt)
        { }
        protected TransitionTableRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new TransitionTableRowSet(defpos,m);
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new TransitionTableRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new TransitionTableRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        public static TransitionTableRowSet operator+(TransitionTableRowSet rs,(long,object)x)
        {
            return (TransitionTableRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TransitionTableRowSet(dp,mem);
        }
        internal override RowSet Source(Context cx)
        {
            return _trs.Source(cx);
        }
        protected override Cursor _First(Context _cx)
        {
            return TransitionTableCursor.New(_cx,this);
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
        internal const long
            Actuals = -435, // BList<long>  SqlValue
            Proc = -436,    // Procedure
            Result = -437;  // RowSet
        internal Procedure proc => (Procedure)mem[Proc];
        internal BList<long> actuals => (BList<long>)mem[Actuals];
        internal RowSet result => (RowSet)mem[Result];
        internal RoutineCallRowSet(Context cx,From f,Procedure p, BList<long> r) 
            :base(cx.GetUid(),cx,f.domain,null,null,f.where,null,null,null,null,
                 BTree<long,object>.Empty+(Proc,p)+(Actuals,r))
        { }
        protected RoutineCallRowSet(Context cx,RoutineCallRowSet rs,
            BTree<long,Finder> nd,RowSet r) 
            :base(cx,rs+(Result,r),nd,true)
        { }
        protected RoutineCallRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new RoutineCallRowSet(defpos,m);
        }
        internal override RowSet New(Context cx, BTree<long,Finder> nd,bool bt)
        {
            throw new NotImplementedException();
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new RoutineCallRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        public static RoutineCallRowSet operator+(RoutineCallRowSet rs,(long,object) x)
        {
            return (RoutineCallRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new RoutineCallRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (RoutineCallRowSet)base._Replace(cx, so, sv);
            r += (Actuals, cx.Replaced(actuals));
            r += (Proc, cx.done[proc.defpos] ?? proc);
            r += (Result, cx.data[result.defpos]); // hmm
            return r;
        }
        internal override RowSet Build(Context cx)
        {
            var _cx = new Context(cx);
            cx = proc.Exec(_cx, actuals);
            return new RoutineCallRowSet(cx, this, needed, cx.result);
        }
        protected override Cursor _First(Context cx)
        {
            return result.First(cx);
        }
    }
    internal class RowSetSection : RowSet
    {
        internal const long
            Offset = -438, // int
            Size = -439; // int
        internal long source => (long)(mem[From.Source]??-1L);
        internal int offset => (int)(mem[Offset]??0);
        internal int size => (int)(mem[Size] ?? 0);
        internal RowSetSection(Context _cx,RowSet s, int o, int c)
            : base(_cx.nextHeap++,s.mem+(Offset,o)+(Size,c)+(From.Source,s.defpos))
        {
            _cx.data += (defpos, this);
        }
        protected RowSetSection(Context cx, RowSetSection rs, BTree<long,Finder> nd,bool bt)
            :base(cx,rs,nd,bt)
        { }
        protected RowSetSection(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new RowSetSection(defpos,m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new RowSetSection(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        internal override RowSet New(Context cx, BTree<long,Finder>nd,bool bt)
        {
            return new RowSetSection(cx, this, nd, bt);
        }
        public static RowSetSection operator+(RowSetSection rs,(long,object)x)
        {
            return (RowSetSection)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new RowSetSection(dp,mem);
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
                if (_pos+1 >= _rss.size)
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
        internal const long
            Docs = -440; // BList<SqlValue>
        internal BList<SqlValue> vals => (BList<SqlValue>)mem[Docs];
        internal DocArrayRowSet(Context cx, Query q, SqlRowArray d)
            : base(cx.GetUid(), cx, q.domain, null, null, q.where,
                  null, null, null, null, _Mem(cx, d))
        { }
        static BTree<long,object> _Mem(Context cx,SqlRowArray d)
        {
            var vs = BList<SqlValue>.Empty;
            if (d != null)
                for(int i=0;i<d.rows.Count;i++)
                    vs += (SqlValue)cx.obs[d.rows[i]];
            return new BTree<long,object>(Docs,vs);
        }
        DocArrayRowSet(Context cx,DocArrayRowSet ds, BTree<long,Finder> nd,bool bt) 
            :base(cx,ds,nd,bt)
        { }
        protected DocArrayRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new DocArrayRowSet(defpos,m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new DocArrayRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        internal override RowSet New(Context cx, BTree<long,Finder> nd, bool bt)
        {
            return new DocArrayRowSet(cx,this, nd, bt);
        }
        public static DocArrayRowSet operator+(DocArrayRowSet rs,(long,object)x)
        {
            return (DocArrayRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new DocArrayRowSet(dp, mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (DocArrayRowSet)base._Replace(cx, so, sv);
            var ds = BList<SqlValue>.Empty;
            for (var b = vals.First(); b != null; b = b.Next())
                ds += (SqlValue)cx.done[b.value().defpos] ?? b.value();
            r += (Docs, ds);
            return r;
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
    internal class ValueRowSet : RowSet
    {
        internal long source => (long)(mem[From.Source] ?? -1L);
        public ValueRowSet(long dp, Context cx, Domain dm, From f, RowSet r)
            : base(dp, BTree<long,object>.Empty+(From.Source,r.defpos)
                  +(_Domain,dm)+ (_Finder,_Fin(cx, f, r))
                  +(Query.Where, r.where) 
                  +(RowOrder,r.rowOrder)+(Query._Matches, r.matches)
                  + (Query.Matching,r.matching))
        { }
        protected ValueRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        protected ValueRowSet(Context cx, RowSet rs, BTree<long, Finder> nd,bool bt)
            : base(cx, rs, nd, bt) { }
        static BTree<long,Finder> _Fin(Context cx,From f,RowSet s)
        {
            var fi = BTree<long,Finder>.Empty;
            var sb = s.rt.First();
            for (var b = f.rowType.First(); b != null && sb != null;
                b = b.Next(), sb = sb.Next())
                fi += (b.value(), s.finder[sb.value()]);
            return fi;
        }
        public static ValueRowSet operator+(ValueRowSet v,(long,object)x)
        {
            return (ValueRowSet)v.New(v.mem + x);
        }
        internal override RowSet Source(Context cx)
        {
            return cx.data[source];
        }
        public override Cursor First(Context _cx) // NB not _First
        {
            return ValueCursor.New(_cx,this,0);
        }
        protected override Cursor _First(Context _cx)
        {
            throw new NotImplementedException();
        }

        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new ValueRowSet(cx,this,nd,bt);
        }

        internal override Basis New(BTree<long, object> m)
        {
            return new ValueRowSet(defpos,m);
        }

        internal override DBObject Relocate(long dp)
        {
            return new ValueRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (ValueRowSet)base._Replace(cx, so, sv);
            r += (From.Source, cx.Replace(source, so, sv));
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = (ValueRowSet)base._Relocate(wr);
            var s = (RowSet)wr.cx.data[source]._Relocate(wr);
            if (s.defpos != source)
                r += (From.Source, s.defpos);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (ValueRowSet)base.Fix(cx);
            r += (From.Source, cx.rsuids[source]??source);
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Source: ");sb.Append(Uid(source));
            return sb.ToString();
        }
        internal class ValueCursor : Cursor
        {
            ValueRowSet _vrs;
            Cursor _sc;
            ValueCursor(Context cx, ValueRowSet rs, int pos, long defpos, Cursor sc)
                : base(cx, rs, pos, defpos, _Row(rs,sc)) 
            {
                _vrs = rs; _sc = sc;
            }
            ValueCursor(ValueCursor cu, Context cx, long p, TypedValue v) 
                : base(cu, cx, p, v) 
            { }
            static TRow _Row(ValueRowSet rs,Cursor sc)
            {
                var vs = BTree<long, TypedValue>.Empty;
                for (var b = rs.rt.First(); b != null; b = b.Next())
                {
                    var p = b.value();
                    var fi = rs.finder[p];
                    vs += (p, sc[fi.col]);
                }
                return new TRow(rs.domain, vs);
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new ValueCursor(this,cx,p,v);
            }
            internal static ValueCursor New(Context cx,ValueRowSet vrs,int pos)
            {
                var sce = vrs.Source(cx);
                var ox = cx.from;
                cx.from += sce.finder;
                for (var b = sce.First(cx);b!=null;b=b.Next(cx))
                {
                    var rb = new ValueCursor(cx, vrs, pos, b._defpos, b);
                    if (rb.Matches(cx) && Query.Eval(vrs.where, cx))
                    {
                        cx.from = ox;
                        return rb;
                    }
                }
                return null;
            }
            protected override Cursor _Next(Context cx)
            {
                var sce = _vrs.Source(cx);
                var ox = cx.from;
                cx.from += sce.finder;
                for (var b = _sc.Next(cx); b != null; b = b.Next(cx))
                {
                    var rb = new ValueCursor(cx, _vrs, _pos+1, b._defpos, b);
                    if (rb.Matches(cx) && Query.Eval(_vrs.where, cx))
                    {
                        cx.from = ox;
                        return rb;
                    }
                }
                return null;
            }

            internal override TableRow Rec()
            {
                return _sc.Rec();
            }
        }
    } 
    /// <summary>
    /// WindowRowSet is built repeatedly during traversal of the source rowset.
    /// </summary>
    internal class WindowRowSet : RowSet
    {
        internal const long
            Multi = -441, // TMultiset
            Window = -442; // SqlFunction
        internal RTree tree => (RTree)mem[OrderedRowSet._RTree];
        internal TMultiset values => (TMultiset)mem[Multi];
        internal long source => (long)(mem[From.Source]??-1L);
        internal SqlFunction wf=> (SqlFunction)mem[Window];
        internal WindowRowSet(Context cx,RowSet sc, SqlFunction f)
            :base(cx.nextHeap++,cx,sc.domain,null,null,sc.where,null,
                 null,null,null,BTree<long,object>.Empty+(From.Source,sc.defpos)
                 +(Window,f))
        { }
        protected WindowRowSet(Context cx,WindowRowSet rs,BTree<long,Finder> nd,
            RTree tr,TMultiset vs) :base(cx,rs,nd,true)
        { }
        protected WindowRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new WindowRowSet(defpos, m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new WindowRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        protected override Cursor _First(Context _cx)
        {
            return WindowCursor.New(_cx,this);
        }
        public static WindowRowSet operator+(WindowRowSet rs,(long,object)x)
        {
            return (WindowRowSet)rs.New(rs.mem+x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new WindowRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (WindowRowSet)base._Replace(cx, so, sv);
            r += (Window, cx.done[wf.defpos] ?? wf);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            return ((WindowRowSet)base.Fix(cx))+(Multi,values.Fix(cx))
                +(OrderedRowSet._RTree,tree.Fix(cx))+(Window,wf.Fix(cx));
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = (WindowRowSet)base._Relocate(wr);
            r += (Window, wf._Relocate(wr));
            return r;
        }
        internal override RowSet Build(Context cx)
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
            var values = new TMultiset(new Domain(Sqlx.MULTISET,wf.domain));
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
    internal class RestRowSet : RowSet
    {
        internal const long
            JoinCols = -383, // BTree<string,int>
            RemoteAggregates = -384, // bool
            RemoteCols = -373, // BList<string>
            RemoteGroups = -374, // GroupSpecification
            RestView = -459,    // long
            RestValue = -457,   // TArray
            UsingCols = -259, // BTree<string,long> TableColumns
            UsingTable = -260; // long Table
        internal TArray aVal => (TArray)mem[RestValue];
        internal long restView => (long)(mem[RestView] ?? -1L);
   //     internal long globalFrom => (long)(mem[GlobalFrom] ?? -1L);
        internal BList<string> remoteCols => 
            (BList<string>)mem[RemoteCols] ?? BList<string>.Empty;
        internal BTree<string, long> usingCols =>
            (BTree<string, long>)mem[UsingCols] ?? BTree<string, long>.Empty;
        internal long usingTable => (long)(mem[UsingTable] ?? -1L);
        internal BTree<string, int> joinCols =>
            (BTree<string, int>)mem[JoinCols] ?? BTree<string, int>.Empty;
        internal bool remoteAggregates => (bool)(mem[RemoteAggregates] ?? false);
        internal GroupSpecification remoteGroups =>(GroupSpecification)mem[RemoteGroups];
        public RestRowSet(Context cx, From f, RestView vw)
            : base(f.defpos, cx, f.domain, null, null, null, null, null, null, null,
                 BTree<long,object>.Empty +(RestView,vw.defpos)+(RemoteCols,vw.viewCols))
        { }
        protected RestRowSet(Context cx, RestRowSet rs, BTree<long, Finder> nd, bool bt)
            : base(cx, rs, nd, bt) { } 
        protected RestRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        public static RestRowSet operator+(RestRowSet rs,(long,object)x)
        {
            return (RestRowSet)rs.New(rs.mem + x);
        }
        protected override Cursor _First(Context _cx)
        {
            return RestCursor.New(_cx, this);
        }

        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new RestRowSet(cx, this, nd, bt);
        }

        internal override Basis New(BTree<long, object> m)
        {
            return new RestRowSet(defpos,m);
        }

        internal override DBObject Relocate(long dp)
        {
            return new RestRowSet(dp, mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (RestRowSet)base._Replace(cx, so, sv);
            r += (RestView, cx.Replace(restView, so, sv));
            var rg = remoteGroups._Replace(cx, so, sv);
            if (rg != remoteGroups)
                r += (RemoteGroups, rg);
            if (usingTable >= 0)
                r += (UsingTable, cx.Replace(UsingTable, so, sv));
            var ch = false;
            var uc = BTree<string, long>.Empty;
            for (var b=usingCols.First();b!=null;b=b.Next())
            {
                var p = cx.Replace(b.value(), so, sv);
                uc += (b.key(), p);
                ch = ch || p != b.value();
            }
            if (ch)
                r += (UsingCols, uc);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (RestRowSet)base.Fix(cx);
            r += (RestView, cx.Fix(restView));
            var rg = remoteGroups?.Fix(cx);
            if (rg != remoteGroups)
                r += (RemoteGroups, rg);
            if (usingTable>=0)
            {
                r += (UsingTable, cx.Fix(usingTable));
                r += (UsingCols, cx.Fix(usingCols));
            }
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = (RestRowSet)base._Relocate(wr);
            r += (RestView, wr.Fix(restView));
            var rg = remoteGroups?._Relocate(wr);
            if (rg != remoteGroups)
                r += (RemoteGroups, rg);
            if (usingTable >= 0)
            {
                r += (UsingTable, wr.Fix(usingTable));
                r += (UsingCols, wr.Fix(usingCols));
            }
            return r;
        }
        public HttpWebRequest GetRequest(Context cx)
        {
            var rv = (RestView)cx.obs[restView];
            string url = rv.description;
            string user = rv.clientName??cx.user.name, password = rv.clientPassword;
            var ss = url.Split('/');
            if (ss.Length > 3)
            {
                var st = ss[2].Split('@');
                if (st.Length > 1)
                {
                    var su = st[0].Split(':');
                    user = su[0];
                    if (su.Length > 1)
                        password = su[1];
                }
            }
            var rq = WebRequest.Create(url) as HttpWebRequest;
            rq.UserAgent = "Pyrrho";
            rq.UserAgent = "Pyrrho " + PyrrhoStart.Version[1];
            if (user == null)
                rq.UseDefaultCredentials = true;
            else
            {
                var cr = user + ":" + password;
                var d = Convert.ToBase64String(Encoding.UTF8.GetBytes(cr));
                rq.Headers.Add("Authorization: Basic " + d);
            }
            return rq;
        }
        public static HttpWebResponse GetResponse(WebRequest rq)
        {
            HttpWebResponse wr;
            try
            {
                wr = rq.GetResponse() as HttpWebResponse;
            }
            catch (WebException e)
            {
                wr = e.Response as HttpWebResponse;
                if (wr == null)
                    throw new DBException("3D003");
                if (wr.StatusCode == HttpStatusCode.Unauthorized)
                    throw new DBException("42105");
                if (wr.StatusCode == HttpStatusCode.Forbidden)
                    throw new DBException("42105");
            }
            catch (Exception e)
            {
                throw new DBException(e.Message);
            }
            return wr;
        }
        internal override RowSet Build(Context cx)
        {
            var ut = (Table)cx.obs[usingTable]; // non-null for get using
            var u = ut?.domain.rowType.Last().value() ?? -1L;
            var vw = (RestView)cx.obs[restView];
            var url = (string)(((SqlValue)cx.obs[u])?.Eval(cx).Val()
                ?? vw.description);
            var rx = url.LastIndexOf("/");
            var globalFrom = (From)cx.obs[defpos];
            string targetName = "";
            if (globalFrom != null)
            {
                targetName = url.Substring(rx + 1);
                url = url.Substring(0, rx);
            }
            var rq = GetRequest(cx);
            rq.Method = "POST";
            rq.Accept = vw.mime ?? "application/json";
            var sql = new StringBuilder("select ");
            sql.Append(remoteCols);
            sql.Append(" from "); sql.Append(targetName);
            var cs = cx.obs[globalFrom.source] as CursorSpecification;
            var cm = " group by ";
            if ((remoteGroups != null && remoteGroups.sets.Count > 0)
                || globalFrom.aggregates(cx))
            {
                var ids = BTree<long, bool>.Empty;
                for (var b = cs.restGroups.First(); b != null; b = b.Next())
                {
                    var n = b.key();
                    if (!ids.Contains(n))
                    {
                        ids += (n, true);
                        sql.Append(cm); cm = ",";
                        sql.Append(n);
                    }
                }
                if (remoteGroups != null)
                    for (var b = remoteGroups.sets.First(); b != null; b = b.Next())
                    {
                        var gs = (Grouping)cx.obs[b.value()];
                        Grouped(cx, gs, sql, ref cm, ids, globalFrom);
                    }
                for (var b = needed.First(); b != null; b = b.Next())
                    if (!ids.Contains(b.key()))
                        {
                        ids += (b.key(), true);
                        sql.Append(cm); cm = ",";
                        sql.Append(b.key());
                    }
                if (cs?.where.Count > 0 || cs?.matches.Count > 0)
                {
                    var sw = globalFrom.WhereString(cs.where, cs.matches, null);
                    if (sw.Length > 0)
                    {
                        sql.Append((ids.Count > 0) ? " having " : " where ");
                        sql.Append(sw);
                    }
                }
            }
            else
            if (cs?.where.Count > 0 || cs?.matches.Count > 0)
            {
                var sw = globalFrom.WhereString(cs.where, cs.matches, null);
                if (sw.Length > 0)
                {
                    sql.Append(" where ");
                    sql.Append(sw);
                }
            }
            if (PyrrhoStart.HTTPFeedbackMode)
                Console.WriteLine(url + " " + sql.ToString());
            if (globalFrom != null)
            {
                var bs = Encoding.UTF8.GetBytes(sql.ToString());
                rq.ContentType = "text/plain";
                rq.ContentLength = bs.Length;
                try
                {
                    var rqs = rq.GetRequestStream();
                    rqs.Write(bs, 0, bs.Length);
                    rqs.Close();
                }
                catch (WebException)
                {
                    throw new DBException("3D002", url);
                }
            }
            var wr = GetResponse(rq);
            if (wr == null)
                throw new DBException("2E201", url);
            var et = wr.GetResponseHeader("ETag");
            var mime = wr.GetResponseHeader("Content-Type")??"text/plain";
            if (et != null)
            {
                //              cx.etags.Add(et);
                if (PyrrhoStart.DebugMode)
                    Console.WriteLine("Response ETag: " + et);
            }
            var s = wr.GetResponseStream();
            TypedValue a = null;
            if (s != null)
                a = domain.Parse(0, new StreamReader(s).ReadToEnd(),mime,cx.role);
            if (PyrrhoStart.HTTPFeedbackMode)
            {
                if (a is TArray)
                    Console.WriteLine("--> " + ((TArray)a).list.Count + " rows");
                else
                    Console.WriteLine("--> " + (a?.ToString() ?? "null"));
            }
            s.Close();
            return this+(RestValue,a);
        }
        BTree<long,bool> Grouped(Context cx, Grouping gs, StringBuilder sql, ref string cm, 
            BTree<long,bool> ids, Query gf)
        {
            for (var b = gs.members.First(); b!=null; b=b.Next())
            {
                var gi = (Grouping)cx.obs[b.key()];
                if (gf.Knows(cx,gi.defpos) && !ids.Contains(gi.defpos))
                {
                    ids+=(gi.defpos,true);
                    sql.Append(cm); cm = ",";
                    sql.Append(gi.alias);
                }
            }
            for (var b=gs.groups.First();b!=null;b=b.Next())
                ids = Grouped(cx, b.value(), sql, ref cm, ids, gf);
            return ids;
        }
        internal override string ToString(Context cx, int n)
        {
            var sb = new StringBuilder(base.ToString(cx,n));
            var oi = (ObInfo)cx.db.role.infos[defpos];
            sb.Append(oi.Metadata());
            return sb.ToString();
        }
        internal class RestCursor : Cursor
        {
            readonly RestRowSet _rrs;
            readonly int _ix;
            RestCursor(Context cx,RestRowSet rrs,int pos,int ix)
                :base(cx,rrs.defpos,pos,ix,(TRow)rrs.aVal[pos])
            {
                _rrs = rrs; _ix = ix;
            }
            RestCursor(Context cx,RestCursor rb,long p,TypedValue v)
                :base(cx,rb._rrs,rb._pos,rb._ix,rb+(p,v))
            { }
            internal static RestCursor New(Context cx,RestRowSet rrs)
            {
                for (var i = 0; i < rrs.aVal.Length; i++)
                {
                    var rb = new RestCursor(cx, rrs, 0, i);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new RestCursor(cx, this, p, v);
            }

            protected override Cursor _Next(Context cx)
            {
                for (var i = _ix+1; i < _rrs.aVal.Length; i++)
                {
                    var rb = new RestCursor(cx, _rrs, _pos+1, i);
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            internal override TableRow Rec()
            {
                return null;
            }
        }
    }

    // An ad-hoc RowSet for a row history: the work is mostly done by
    // LogRowsCursor
    internal class LogRowsRowSet : RowSet
    {
        internal const long
            TargetTable = -369; // long Table
        public long targetTable => (long)(mem[TargetTable] ?? -1L);
        public LogRowsRowSet(long dp, Context cx, long td)
            : base(dp, _Mem(cx, dp, td)+(Built,true))
        { }
        protected LogRowsRowSet(long dp,BTree<long,object> m) :base(dp,m)
        { }
        protected LogRowsRowSet(Context cx, RowSet rs, BTree<long, Finder> nd, bool bt)
            : base(cx, rs, nd, bt)
        { }
        static BTree<long, object> _Mem(Context cx, long dp, long td)
        {
            var tb = cx.db.objects[td] as Table ??
                throw new DBException("42131", "" + td).Mix();
            cx.Add(tb);
            var r = new BTree<long, object>(TargetTable, tb.defpos);
            var rt = BList<SqlValue>.Empty;
            var fi = BTree<long, Finder>.Empty;
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "Pos", Domain.Position);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "Action", Domain.Char);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "DefPos", Domain.Position);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "Transaction", Domain.Position);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "Timestamp", Domain.Timestamp);
            return r + (_Domain, new Domain(Sqlx.TABLE,rt)) + (_Finder, fi);
        }
        public static LogRowsRowSet operator+(LogRowsRowSet r,(long,object)x)
        {
            return (LogRowsRowSet)r.New(r.mem + x);
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new LogRowsRowSet(cx, this, nd, bt);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new LogRowsRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        protected override Cursor _First(Context _cx)
        {
            return LogRowsCursor.New(_cx, this,0,targetTable);
        }

        internal override DBObject Relocate(long dp)
        {
            return new LogRowsRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (LogRowsRowSet) base._Replace(cx, so, sv);
            r += (TargetTable, cx.Replace(targetTable, so, sv));
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new LogRowsRowSet(defpos, m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" for "); sb.Append(Uid(targetTable));
            return sb.ToString();
        }
        internal class LogRowsCursor : Cursor
        {
            readonly LogRowsRowSet _lrs;
            readonly TRow _row;
            LogRowsCursor(Context cx,LogRowsRowSet rs,int pos,long defpos,TRow rw)
                :base(cx,rs,pos,defpos,rw)
            {
                _lrs = rs; _row = rw;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new LogRowsCursor(cx, _lrs, _pos, _defpos, _row + (p, v));
            }
            internal static LogRowsCursor New(Context cx,LogRowsRowSet lrs,int pos,long p)
            {
                Physical ph;
                PTransaction pt = null;
                for (var b = cx.db.log.PositionAt(p); b != null; b = b.Next())
                {
                    var vs = BTree<long, TypedValue>.Empty;
                    p = b.key();
                    switch (b.value())
                    {
                        case Physical.Type.Record:
                        case Physical.Type.Record1:
                        case Physical.Type.Record2:
                        case Physical.Type.Record3:
                            {
                                (ph, _) = cx.db._NextPhysical(b.key(),pt);
                                var rc = ph as Record;
                                if (rc.tabledefpos != lrs.targetTable)
                                    continue;
                                for (var c = lrs.rt.First(); c != null; c = c.Next())
                                {
                                    var cp = c.value();
                                    switch (c.key())
                                    {
                                        case 0: vs += (cp, new TPosition(p)); break;
                                        case 1: vs += (cp, new TChar("Insert")); break;
                                        case 2: vs += (cp, new TPosition(rc.defpos)); break;
                                        case 3: vs += (cp, new TPosition(rc.trans)); break;
                                        case 4:
                                            vs += (cp, new TDateTime(new DateTime(rc.time)));
                                            break;
                                    }
                                }
                                return new LogRowsCursor(cx, lrs, pos, p, new TRow(lrs.domain, vs));
                            }
                        case Physical.Type.Update:
                        case Physical.Type.Update1:
                            {
                                (ph, _) = cx.db._NextPhysical(b.key(),pt);
                                var rc = ph as Record;
                                if (rc.tabledefpos != lrs.targetTable)
                                    continue;
                                for (var c = lrs.rt.First(); c != null; c = c.Next())
                                {
                                    var cp = c.value();
                                    switch (c.key())
                                    {
                                        case 0: vs += (cp, new TPosition(p)); break;
                                        case 1: vs += (cp, new TChar("Update")); break;
                                        case 2: vs += (cp, new TPosition(rc.defpos)); break;
                                        case 3: vs += (cp, new TPosition(rc.trans)); break;
                                        case 4:
                                            vs += (cp, new TDateTime(new DateTime(rc.time)));
                                            break;
                                    }
                                }
                                return new LogRowsCursor(cx, lrs, pos, p, new TRow(lrs.domain, vs));
                            }
                        case Physical.Type.Delete:
                        case Physical.Type.Delete1:
                            {
                                (ph, _) = cx.db._NextPhysical(b.key(),pt);
                                var rc = ph as Delete;
                                if (rc.tabledefpos != lrs.targetTable)
                                    continue;
                                for (var c = lrs.rt.First(); c != null; c = c.Next())
                                {
                                    var cp = c.value();
                                    switch (c.key())
                                    {
                                        case 0: vs += (cp, new TPosition(p)); break;
                                        case 1: vs += (cp, new TChar("Delete")); break;
                                        case 2: vs += (cp, new TPosition(rc.ppos)); break;
                                        case 3: vs += (cp, new TPosition(rc.trans)); break;
                                        case 4:
                                            vs += (cp, new TDateTime(new DateTime(rc.time)));
                                            break;
                                        default: vs += (cp, TNull.Value); break;
                                    }
                                }
                                return new LogRowsCursor(cx, lrs, pos, p, new TRow(lrs.domain, vs));
                            }
                        case Physical.Type.PTransaction:
                        case Physical.Type.PTransaction2:
                            {
                                (ph,_) = cx.db._NextPhysical(b.key(), pt);
                                pt = (PTransaction)ph;
                                break;
                            }
                    }
                }
                return null;
            }
            protected override Cursor _Next(Context cx)
            {
                return New(cx,_lrs,_pos+1,_defpos+1);
            }
            internal override TableRow Rec()
            {
                return null;
            }
        }
    }
    /// <summary>
    /// An Ad-hoc RowSet for a row,column history: the work is mostly done by
    /// LogRowColCursor
    /// </summary>
    internal class LogRowColRowSet : RowSet
    {
        internal const long
            LogCol = -337, // long TableColumn
            LogRow = -339; // long TableRow
        public long targetTable => (long)(mem[LogRowsRowSet.TargetTable] ?? -1L);
        public long logCol => (long)(mem[LogCol] ?? -1L);
        public long logRow => (long)(mem[LogRow] ?? -1L);
        public LogRowColRowSet(long dp, Context cx, long r, long c)
        : base(dp, _Mem(cx, dp, c) + (LogRow, r) + (LogCol, c)+(Built,true))
        { }
        protected LogRowColRowSet(long dp,BTree<long,object>m) :base(dp,m)
        { }
        protected LogRowColRowSet(Context cx, RowSet rs, BTree<long, Finder> nd, bool bt)
            : base(cx, rs, nd, bt)
        { }
        static BTree<long, object> _Mem(Context cx, long dp, long cd)
        {
            var tc = cx.db.objects[cd] as TableColumn ??
                throw new DBException("42131", "" + cd).Mix();
            cx.Add(tc);
            var tb = cx.db.objects[tc.tabledefpos] as Table;
            cx.Add(tb);
            var rt = BList<SqlValue>.Empty;
            var fi = BTree<long, Finder>.Empty;
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "Pos", Domain.Char);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "Value", Domain.Char);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "StartTransaction", Domain.Char);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "StartTimestamp", Domain.Timestamp);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "EndTransaction", Domain.Char);
            fi += (cx.nextHeap, new Finder(cx.nextHeap, dp));
            rt += new SqlFormal(cx, "EndTimestamp", Domain.Timestamp);
            return BTree<long, object>.Empty + (_Domain,new Domain(Sqlx.TABLE,rt))+
                (LogRowsRowSet.TargetTable, tb.defpos) + (_Finder,fi);
        }
        public static LogRowColRowSet operator+(LogRowColRowSet r,(long,object)x)
        {
            return (LogRowColRowSet)r.New(r.mem + x);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" for "); sb.Append(targetTable);
            sb.Append("(");  sb.Append(Uid(logRow)); 
            sb.Append(",");  sb.Append(Uid(logCol)); 
            sb.Append(")"); 
            return sb.ToString();
        }
        internal override Basis New(BTree<long,object> m)
        {
            return new LogRowColRowSet(defpos, m);
        }
        /// <summary>
        /// We need to change some properties, but if it has come from a framing
        /// it will be shareable and so we must create a new copy first
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (m == mem)
                return this;
            if (defpos >= Transaction.Analysing)
                return (RowSet)New(m);
            var rs = new LogRowColRowSet(cx.nextHeap++, m);
            Fixup(cx, rs);
            return rs;
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd, bool bt)
        {
            return new LogRowColRowSet(cx, this, nd, bt);
        }
        protected override Cursor _First(Context _cx)
        {
            return LogRowColCursor.New(_cx,this,0,(null,logRow));
        }

        internal override DBObject Relocate(long dp)
        {
            return new LogRowColRowSet(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (LogRowColRowSet) base._Replace(cx, so, sv);
            r += (LogRowsRowSet.TargetTable, cx.Replace(targetTable, so, sv));
            r += (LogCol, cx.Replace(logCol, so, sv));
            return r;
        }
        internal class LogRowColCursor : Cursor
        {
            readonly LogRowColRowSet _lrs;
            readonly (Physical,long) _next;
            LogRowColCursor(Context cx, LogRowColRowSet lrs, int pos, long defpos, 
                (Physical,long) next, TRow rw) 
                : base(cx, lrs, pos, defpos, rw) 
            {
                _lrs = lrs; _next = next;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new LogRowColCursor(cx, _lrs, _pos, _defpos, _next, this + (p, v));
            }
            internal static LogRowColCursor New(Context cx,LogRowColRowSet lrs,int pos,
                (Physical,long) nx,PTransaction trans=null)
            {
                var vs = BTree<long, TypedValue>.Empty;
                var tb = lrs.targetTable;
                var tc = lrs.logCol;
                var rp = nx.Item2;
                if (trans == null)
                    for (var b = cx.db.log.PositionAt(nx.Item2); trans == null && b != null;
                        b = b.Previous())
                        if (b.value() is Physical.Type.PTransaction ||
                            b.value() is Physical.Type.PTransaction2)
                            trans = (PTransaction)cx.db._NextPhysical(b.key()).Item1;
                if (rp < 0)
                    return null;
                if (nx.Item1==null)
                    nx = cx.db._NextPhysical(rp,trans);
                for (; nx.Item1 != null;)
                {
                    var rc = nx.Item1 as Record; // may be an Update 
                    if (rc==null || lrs.logRow != rc.defpos || !rc.fields.Contains(tc))
                    {
                        nx = cx.db._NextPhysical(nx.Item2, trans);
                        if (nx.Item1 is PTransaction nt)
                            trans = nt;
                        continue;
                    }
                    var b = lrs.rt.First(); vs += (b.value(), new TPosition(rc.ppos));
                    b = b.Next(); vs += (b.value(), rc.fields[tc] ?? TNull.Value);
                    b = b.Next(); vs += (b.value(), new TPosition(rc.trans));
                    b = b.Next(); vs += (b.value(), new TDateTime(new DateTime(rc.time)));
                    var done = false;
                    for (nx = cx.db._NextPhysical(nx.Item2, trans);
                        nx.Item1 != null && nx.Item2 >= 0;
                        nx = cx.db._NextPhysical(nx.Item2, trans))
                    {
                        var (ph, _) = nx;
                        switch (ph.type)
                        {
                            case Physical.Type.Delete:
                            case Physical.Type.Delete1:
                                done = ((Delete)ph).delpos == rp;
                                    break;
                            case Physical.Type.Update:
                            case Physical.Type.Update1:
                                {
                                    var up = (Update)ph;
                                    done = up.defpos == rp && up.fields.Contains(tc);
                                    break;
                                }
                            case Physical.Type.PTransaction:
                            case Physical.Type.PTransaction2:
                                trans = (PTransaction)ph;
                                break;
                        }
                        if (done)
                            break;
                    }
                    if (done)
                    {
                        b = b.Next(); vs += (b.value(), new TPosition(trans.ppos));
                        b = b.Next(); vs += (b.value(), new TDateTime(new DateTime(trans.pttime)));
                    }
                    var rb = new LogRowColCursor(cx, lrs, pos, rc.ppos,
                                        nx, new TRow(lrs.domain, vs));
                    if (rb.Matches(cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor _Next(Context cx)
            {
                return New(cx,_lrs,_pos+1,_next);
            }

            internal override TableRow Rec()
            {
                return null;
            }
        }
    }
}