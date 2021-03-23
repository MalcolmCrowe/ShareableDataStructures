using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using System;
using System.Configuration;
using System.Runtime.ExceptionServices;
using System.Security.Authentication.ExtendedProtection;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
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
    /// A row set for a Join operation.
    /// </summary>
	internal class JoinRowSet : RowSet
	{
        internal const long
            JFirst = -447, // long RowSet
            JSecond = -448; // long RowSet
        /// <summary>
        /// The two row sets being joined
        /// </summary>
		internal long first => (long)mem[JFirst];
        internal long second => (long)mem[JSecond];
        internal Sqlx joinKind => (Sqlx) mem[JoinPart.JoinKind];
        internal CTree<long, bool> joinCond =>
            (CTree<long, bool>)mem[JoinPart.JoinCond] ?? CTree<long, bool>.Empty;
        internal FDJoinPart fdInfo => (FDJoinPart)mem[JoinPart._FDInfo];
        internal CTree<long, CTree<long,bool>> matching =>
            (CTree<long, CTree<long,bool>>)mem[JoinPart.Matching]??CTree<long,CTree<long,bool>>.Empty;
        /// <summary>
        /// Constructor: build the rowset for the Join
        /// </summary>
        /// <param name="j">The Join part</param>
		public JoinRowSet(Context _cx, JoinPart j, RowSet lr, RowSet rr) :
            base(j.defpos, _cx, j.domain, _Fin(j,lr,rr), null, j.where, j.ordSpec, j.matches,
                _Last(lr, rr) + (JFirst, lr.defpos) + (JSecond, rr.defpos)
                +(RSTargets,lr.rsTargets+rr.rsTargets)
                + (JoinPart._FDInfo, j.FDInfo) + (JoinPart.Matching,j.matching)
                +(JoinPart.JoinCond,j.joinCond) + (JoinPart.JoinKind,j.kind))
		{ }
        JoinRowSet(Context cx,JoinRowSet jrs, CTree<long,Finder> nd,bool bt)
            :base(cx,jrs,nd,bt)
        { }
        static CTree<long,Finder> _Fin(JoinPart j,RowSet lr,RowSet rr)
        {
            var r = lr.finder + rr.finder;
            if (j.joinUsing == BTree<long,long>.Empty)
                return r;
            for (var b = j.joinUsing.First(); b != null; b = b.Next())
                r += (b.value(), new Finder(b.key(), lr.defpos));
            return r;
        }
        static BTree<long,object> _Last(RowSet lr,RowSet rr)
        {
            var r = BTree<long, object>.Empty;
            var ld = lr.lastData;
            var rd = rr.lastData;
            if (ld != 0 && rd != 0) 
                r+=(Table.LastData,Math.Max(ld, rd));
            return r;
        }
        internal override bool Knows(Context cx, long rp)
        {
            return rp==first || rp==second || base.Knows(cx, rp);
        }
        JoinRowSet(Context cx,JoinRowSet rs,Sqlx k) 
            :base(cx,rs+(JoinPart.JoinKind,k),rs.needed,rs.built)
        { }
        protected JoinRowSet(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        internal override Basis New(BTree<long, object> m)
        {
            return new JoinRowSet(defpos, m);
        }
        internal RowSet New(Context cx,Sqlx k)
        {
            return new JoinRowSet(cx, this, k);
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
            var rs = new JoinRowSet(cx.GetUid(), m);
            cx.data += (rs.defpos, rs);
            return rs;
        }
        internal int Compare(Context cx)
        {
            var oc = cx.finder;
            cx.finder += finder;
            for (var b = joinCond.First(); b != null; b = b.Next())
            {
                var se = cx.obs[b.key()] as SqlValueExpr;
                var c = cx.obs[se.left].Eval(cx)?.CompareTo(cx.obs[se.right].Eval(cx)) ?? -1;
                if (c != 0)
                {
                    cx.finder = oc;
                    return c;
                }
            }
            cx.finder = oc;
            return 0;
        }
        internal override RowSet New(Context cx, CTree<long, Finder> nd,bool bt)
        {
            return new JoinRowSet(cx, this, nd, bt);
        }
        public static JoinRowSet operator+(JoinRowSet rs,(long,object)x)
        {
            return (JoinRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new JoinRowSet(dp, mem);
        }
        public override Rvv _Rvv(Context cx)
        {
            return cx.data[first]._Rvv(cx) + cx.data[second]._Rvv(cx);
        }
        internal override Basis Fix(Context cx)
        {
            var r = (JoinRowSet)base.Fix(cx);
            var nc = cx.Fix(joinCond);
            if (nc != joinCond)
                r += (JoinPart.JoinCond, nc);
            var ni = fdInfo?.Fix(cx);
            if (ni != fdInfo)
                r += (JoinPart._FDInfo, ni);
            var nf = cx.rsuids[first] ?? first;
            if (nf != first)
                r += (JFirst, nf);
            var ns = cx.rsuids[second] ?? second;
            if (ns != second)
                r += (JSecond, ns);
            var ma = cx.Fix(matching);
            if (ma != matching)
                r += (JoinPart.Matching, ma);
            return r;
        }
        internal override BList<long> Sources(Context cx)
        {
            return new BList<long>(first) + second;
        }
        internal override BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            t = Scan(t, first, VIC.RK|VIC.RV);
            t = Scan(t, second, VIC.RK|VIC.RV);
            return base.Scan(t);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (JoinRowSet)base._Relocate(wr);
            r += (JoinPart.JoinCond, wr.Fix(joinCond));
            r += (JFirst, wr.Fix(first));
            r += (JSecond, wr.Fix(second));
            r += (JoinPart.Matching, wr.Fix(matching));
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (RowSet)base._Replace(cx, so, sv);
            r += (JFirst, cx.Replace(first, so, sv));
            r += (JSecond, cx.Replace(second, so, sv));
            return r;
        }
        internal override CTree<long, Finder> AllWheres(Context cx,CTree<long,Finder> nd)
        {
            nd = cx.Needs(nd,this,where);
            nd = cx.Needs(nd,this,cx.data[first].AllWheres(cx, nd));
            nd = cx.Needs(nd,this,cx.data[second].AllWheres(cx, nd));
            return nd;
        }
        internal override CTree<long, Finder> AllMatches(Context cx,CTree<long,Finder> nd)
        {
            nd = cx.Needs(nd,this,matches);
            nd = cx.Needs(nd,this,cx.data[first].AllMatches(cx,nd));
            nd = cx.Needs(nd,this,cx.data[second].AllMatches(cx,nd));
            return nd;
        }
        internal override DBObject AddMatch(Context cx, SqlValue sv, TypedValue v)
        {
            var r = (RowSet)base.AddMatch(cx, sv, v);
            if (r.matches.Contains(sv.defpos))
                return this;
            if (sv is SqlCopy sc)
                for (var b = joinCond.First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is SqlValueExpr se && se.kind == Sqlx.EQL)
                    {
                        if (se.left == sc.defpos && cx.obs[se.right] is SqlCopy sm)
                            r = (RowSet)r.AddMatch(cx, sm, v);
                        if (se.right == sc.defpos && cx.obs[se.left] is SqlCopy sl)
                            r = (RowSet)r.AddMatch(cx, sl, v);
                    }
            return r;
        }
        internal override DBObject AddCondition(Context cx, long prop, long cond)
        {
            var cs = (CTree<long, bool>)mem[prop] ?? CTree<long, bool>.Empty;
            if (cs.Contains(cond))
                return this;
            var sv = (SqlValue)cx.obs[cond];
            if (!sv.KnownBy(cx, this))
                return this;
            var done = false;
            var r = this;
            var lf = cx.data[first];
            if (lf.defpos > Transaction.TransPos && sv.KnownBy(cx, lf))
            {
                var nl = (RowSet)lf.AddCondition(cx, prop, cond);
                if (nl != lf)
                {
                    cx.data += (nl.defpos, nl);
                    r += (JFirst, nl.defpos);
                    done = true;
                }
            }
            var rg = cx.data[second];
            if (rg.defpos > Transaction.TransPos && sv.KnownBy(cx, rg))
            {
                var nr = (RowSet)rg.AddCondition(cx, prop, cond);
                if (nr != rg)
                {
                    cx.data += (nr.defpos, nr);
                    r += (JSecond, nr.defpos);
                    done = true;
                }
            }
            if (done)
                return r + (prop, cs + (cond, true));
            return new SelectRowSet(cx, this, CTree<long, bool>.Empty + (cond, true));
        }
        /// <summary>
        /// Set up a bookmark for the rows of this join
        /// </summary>
        /// <param name="matches">matching information</param>
        /// <returns>the enumerator</returns>
        protected override Cursor _First(Context _cx)
        {
            JoinBookmark r;
            switch (joinKind)
            {
                case Sqlx.LATERAL: r = LateralJoinBookmark.New(_cx, this); break;
                case Sqlx.CROSS: r= CrossJoinBookmark.New(_cx,this); break;
                case Sqlx.INNER: r= InnerJoinBookmark.New(_cx,this); break;
                case Sqlx.LEFT: r = LeftJoinBookmark.New(_cx, this); break;
                case Sqlx.RIGHT: r = RightJoinBookmark.New(_cx, this); break;
                case Sqlx.FULL: r = FullJoinBookmark.New(_cx,this); break;
                case Sqlx.NO: r = FDJoinBookmark.New(_cx,this); break;
                default:
                    throw new PEException("PE57");
            }
            var b = r?.MoveToMatch(_cx);
            return b;
        }
        protected override Cursor _Last(Context cx)
        {
            JoinBookmark r;
            switch (joinKind)
            {
                case Sqlx.LATERAL: r = LateralJoinBookmark.New(this, cx); break;
                case Sqlx.CROSS: r = CrossJoinBookmark.New(this, cx); break;
                case Sqlx.INNER: r = InnerJoinBookmark.New(this, cx); break;
                case Sqlx.LEFT: r = LeftJoinBookmark.New(this, cx); break;
                case Sqlx.RIGHT: r = RightJoinBookmark.New(this, cx); break;
                case Sqlx.FULL: r = FullJoinBookmark.New(this, cx); break;
                case Sqlx.NO: r = FDJoinBookmark.New(this, cx); break;
                default:
                    throw new PEException("PE57");
            }
            var b = r?.MoveToMatch(cx);
            return b;
        }
        public override Cursor First(Context cx)
        {
            var r = ((JoinBookmark)base.First(cx))?.MoveToMatch(cx);
            if (r == null)
                return null;
            if (cx.data[second].needed != CTree<long, Finder>.Empty)
            {
                var jrs = (JoinRowSet)cx.data[defpos];
                cx.data += (defpos, jrs.New(cx, Sqlx.LATERAL));
                r = new LateralJoinBookmark(cx,(JoinBookmark)r);
            }
            return r;
        }
        public override Cursor Last(Context cx)
        {
            var r = ((JoinBookmark)base.Last(cx))?.PrevToMatch(cx);
            if (r == null)
                return null;
            if (cx.data[second].needed != CTree<long, Finder>.Empty)
            {
                var jrs = (JoinRowSet)cx.data[defpos];
                cx.data += (defpos, jrs.New(cx, Sqlx.LATERAL));
                r = new LateralJoinBookmark(cx,(JoinBookmark)r);
            }
            return r;
        }
        (RowSet,RowSet) Split(Context cx,RowSet fm)
        {
            var f = cx.data[first];
            var a = BList<(long, TRow)>.Empty;
            var s = cx.data[second];
            var b = BList<(long, TRow)>.Empty;
            for (var c = First(cx);c!=null;c=c.Next(cx))
            {
                var fc = cx.cursors[f.defpos];
                a += (fc._defpos, fc);
                var sc = cx.cursors[s.defpos];
                b += (sc._defpos, sc);
            }
            return (f + (ExplicitRowSet.ExplRows, a),
                s + (ExplicitRowSet.ExplRows, b));
        }
        internal override Context Insert(Context cx, RowSet fm, string prov, Level cl)
        {
            var (a, b) = Split(cx, fm);
            cx.data[first].Insert(cx, a, prov, cl);
            cx.data[second].Insert(cx, b, prov, cl);
            return cx;
        }
        internal override Context Delete(Context cx, RowSet fm)
        {
            var (a, b) = Split(cx, fm);
            cx = cx.data[first].Delete(cx, a);
            cx = cx.data[second].Delete(cx, b);
            return cx;
        }
        internal override Context Update(Context cx, RowSet fm)
        {
            var (a, b) = Split(cx, fm);
            cx = cx.data[first].Update(cx, a);
            cx = cx.data[second].Update(cx, b);
            return cx;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (joinCond!=CTree<long,bool>.Empty)
            { 
                sb.Append(" JoinCond: ");
                var cm = "(";
                for (var b=joinCond.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                sb.Append(")");
            }
            if (matching != CTree<long, CTree<long, bool>>.Empty)
            {
                sb.Append(" matching");
                for (var b = matching.First(); b != null; b = b.Next())
                    for (var c = b.value().First(); c != null; c = c.Next())
                    {
                        sb.Append(" "); sb.Append(Uid(b.key()));
                        sb.Append("="); sb.Append(Uid(c.key()));
                    }
            }
            sb.Append(" First: ");sb.Append(Uid(first));
            sb.Append(" Second: "); sb.Append(Uid(second));
            return sb.ToString();
        }
    }
    /// <summary>
    /// A base class for join bookmarks. A join bookmark is composite: it contains bookmarks for left and right.
    /// If there are no ties, all is simple.
    /// But if there are ties on both first and second we need to ensure that
    /// all tying values of right must be used with each tying value of left.
    /// We discover there is a tie when the MTreeBookmark is over-long or has pmk nonnull.
    /// With joins we always have MTree for both left and right (from Indexes or from Ordering)
    /// </summary>
	internal abstract class JoinBookmark : Cursor
	{
        /// <summary>
        /// The associated join row set
        /// </summary>
		internal readonly JoinRowSet _jrs;
        protected readonly Cursor _left, _right;
        protected readonly bool _useLeft, _useRight;
        internal JoinBookmark(Context _cx, JoinRowSet jrs, Cursor left, bool ul, Cursor right,
            bool ur, int pos) : base(_cx, jrs, pos, 0, 0, _Vals(jrs, left, ul, right, ur))
        {
            _jrs = jrs;
            _left = left;
            _useLeft = ul;
            _right = right;
            _useRight = ur;
        }
        internal JoinBookmark(Context _cx, JoinRowSet jrs, Cursor left, bool ul, Cursor right,
    bool ur, int pos,TRow rw) : base(_cx, jrs, pos, 0, 0, rw)
        {
            _jrs = jrs;
            _left = left;
            _useLeft = ul;
            _right = right;
            _useRight = ur; 
        }
        protected JoinBookmark(JoinBookmark cu, Context cx, long p, TypedValue v) 
            : base(cu, cx, p, v)
        {
            _jrs = cu._jrs;
            _left = cu._left;
            _useLeft = cu._useLeft;
            _right = cu._right;
            _useRight = cu._useRight;
        }
        protected JoinBookmark(Context cx,JoinBookmark cu) 
            :base(cx,cx.data[cu._jrs.defpos],cu._pos,0,0,cu) 
        {
            _jrs = cu._jrs;
            _left = cu._left;
            _useLeft = cu._useLeft;
            _right = cu._right;
            _useRight = cu._useRight;
        }
        static TRow _Vals(JoinRowSet jrs, Cursor left, bool ul, Cursor right, bool ur)
        {
            var vs = CTree<long, TypedValue>.Empty;
            for (var b = jrs.rt.First(); b != null; b = b.Next())
            {
                var p = b.value();
                vs += (p, (ul?left[p]:null) ?? (ur?right[p]:null)??TNull.Value);
            }
            return new TRow(jrs.domain, vs);
        }
        public override Cursor Next(Context _cx)
        {
            return ((JoinBookmark)_Next(_cx))?.MoveToMatch(_cx);
        }
        public override Cursor Previous(Context cx)
        {
            return ((JoinBookmark)_Previous(cx))?.PrevToMatch(cx);
        }
        internal Cursor MoveToMatch(Context _cx)
        {
            JoinBookmark r = this;
            while (r != null && !DBObject.Eval(_jrs.where, _cx))
                r = (JoinBookmark)r.Next(_cx);
            return r;
        }
        internal Cursor PrevToMatch(Context _cx)
        {
            JoinBookmark r = this;
            while (r != null && !DBObject.Eval(_jrs.where, _cx))
                r = (JoinBookmark)r.Previous(_cx);
            return r;
        }
        internal override BList<TableRow> Rec()
        {
            var r = BList<TableRow>.Empty;
            if (_useLeft)
                r += _left.Rec();
            if (_useRight)
                r += _right.Rec();
            return r;
        }
    }
    /// <summary>
    /// An enumerator for an inner join rowset
    /// Key for left and right is given by the JoinCondition
    /// </summary>
    internal class InnerJoinBookmark : JoinBookmark
    {
        /// <summary>
        /// Constructor: a new Inner Join enumerator
        /// </summary>
        /// <param name="j">The part row set</param>
        InnerJoinBookmark(Context _cx,JoinRowSet j, Cursor left, Cursor right,int pos=0) 
            : base(_cx,j,left,true,right,true,pos)
        {
            // warning: now check the joinCondition using AdvanceToMatch
        }
        InnerJoinBookmark(InnerJoinBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
        { }
        InnerJoinBookmark(Context cx, InnerJoinBookmark cu) : base(cx, cu) { }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new InnerJoinBookmark(this, cx, p, v);
        }
        internal static InnerJoinBookmark New(Context cx,JoinRowSet j)
        {
            var left = cx.data[j.first].First(cx);
            var right = cx.data[j.second].First(cx);
            if (left == null || right == null)
                return null;
            for (;;)
            {
                var bm = new InnerJoinBookmark(cx,j, left, right);
                int c = j.Compare(cx);
                if (c == 0)
                    return bm;
                if (c < 0)
                {
                    if ((left = left.Next(cx)) == null)
                        return null;
                }
                else if ((right = right.Next(cx)) == null)
                    return null;
            }
        }
        internal static InnerJoinBookmark New(JoinRowSet j, Context cx)
        {
            var left = cx.data[j.first].Last(cx);
            var right = cx.data[j.second].Last(cx);
            if (left == null || right == null)
                return null;
            for (; ; )
            {
                var bm = new InnerJoinBookmark(cx, j, left, right);
                int c = j.Compare(cx);
                if (c == 0)
                    return bm;
                if (c > 0)
                {
                    if ((left = left.Previous(cx)) == null)
                        return null;
                }
                else if ((right = right.Previous(cx)) == null)
                    return null;
            }
        }
        /// <summary>
        /// Move to the next row in the inner join
        /// </summary>
        /// <returns>whether there is a next row</returns>
        protected override Cursor _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (right.Mb() is MTreeBookmark mb0 && mb0.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Next(_cx);
                return new InnerJoinBookmark(_cx,_jrs, left, right, _pos + 1);
            }
            left = left.Next(_cx);
            if (left == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                right.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
            if (mb != null)
                right = right.ResetToTiesStart(_cx,mb);
            else
                right = right.Next(_cx);
            if (right == null)
                return null;
            for (; ; )
            {
                var ret = new InnerJoinBookmark(_cx,_jrs, left, right, _pos + 1);
                int c = _jrs.Compare( _cx);
                if (c == 0)
                    return ret;
                if (c < 0)
                {
                    if ((left = left.Next(_cx)) == null)
                        return null;
                }
                else
                    if ((right = right.Next(_cx)) == null)
                        return null;
            }
        }
        protected override Cursor _Previous(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (right.Mb() is MTreeBookmark mb0 && mb0.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Previous(_cx);
                return new InnerJoinBookmark(_cx, _jrs, left, right, _pos + 1);
            }
            left = left.Previous(_cx);
            if (left == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                right.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
            if (mb != null)
                right = right.ResetToTiesStart(_cx, mb);
            else
                right = right.Previous(_cx);
            if (right == null)
                return null;
            for (; ; )
            {
                var ret = new InnerJoinBookmark(_cx, _jrs, left, right, _pos + 1);
                int c = _jrs.Compare(_cx);
                if (c == 0)
                    return ret;
                if (c > 0)
                {
                    if ((left = left.Previous(_cx)) == null)
                        return null;
                }
                else
                    if ((right = right.Previous(_cx)) == null)
                    return null;
            }
        }
        internal override Cursor _Fix(Context cx)
        {
            return new InnerJoinBookmark(cx, this);
        }
    }
    /// <summary>
    /// An enumerator for a functional-dependent part.
    /// In such a part, there is a condition that uniquely determines
    /// an operand of the part.
    /// So we enumerate the other operand and position for the determined
    /// operand.
    /// </summary>
    internal class FDJoinBookmark : JoinBookmark
    {
        /// <summary>
        /// Constructor for the functional-dependent join
        /// </summary>
        /// <param name="rs">The Join RowSet</param>
        FDJoinBookmark(Context _cx,JoinRowSet rs, IndexRowSet.IndexCursor left, 
            IndexRowSet.IndexCursor right,int pos) 
            : base(_cx,rs,left,true,right,true,pos,_Row(_cx,rs,left,right))
        { }
        FDJoinBookmark(FDJoinBookmark cu,Context cx, long p,TypedValue v):base(cu,cx,p,v)
        { }
        FDJoinBookmark(Context cx,FDJoinBookmark cu):base(cx,cu)
        { }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new FDJoinBookmark(this,cx,p,v);
        }
        static TRow _Row(Context cx,JoinRowSet rs,Cursor left,Cursor right)
        {
            var vs = CTree<long, TypedValue>.Empty;
            var lf = cx.data[rs.first];
            var rg = cx.data[rs.second];
            for (var b=lf.rt.First();b!=null;b=b.Next())
            {
                var s = cx.obs[b.value()];
                var p = (s is SqlCopy sc) ? sc.copyFrom : -1L;
                vs += (s.defpos, left[p]);
            }
            for (var b = rg.rt.First(); b != null; b = b.Next())
            {
                var s = cx.obs[b.value()];
                var p = (s is SqlCopy sc) ? sc.copyFrom : -1L;
                vs += (s.defpos, right[p]);
            }
            return new TRow(rs.domain, vs);
        }
        /// <summary>
        /// A new FD bookmark
        /// </summary>
        /// <param name="j">the join rowset</param>
        /// <returns>the bookmark</returns>
        internal static FDJoinBookmark New(Context cx, JoinRowSet j)
        {
            var info = j.fdInfo;
            IndexRowSet.IndexCursor left, right;
            var ox = cx.finder;
            cx.finder += j.finder;
            var fi = cx.data[j.first];
            var se = cx.data[j.second];
            if (info.reverse)
            {
                left = For(fi.First(cx));
                if (left != null)
                {
                    right = For(se.PositionAt(cx, left._bmk.key()));
                    var r = new FDJoinBookmark(cx, j, left, right, 0);
                    cx.finder = ox;
                    return r;
                }
            }
            else
            {
                right = For(se.First(cx));
                if (right!= null)
                {
                    left = For(fi.PositionAt(cx,right._bmk.key()));
                    var r = new FDJoinBookmark(cx, j, left, right, 0);
                    cx.finder = ox;
                    return r;
                }
            }
            cx.finder = ox;
            return null;
        }
        internal static FDJoinBookmark New(JoinRowSet j, Context cx)
        {
            var info = j.fdInfo;
            IndexRowSet.IndexCursor left, right;
            var ox = cx.finder;
            cx.finder += j.finder;
            var fi = cx.data[j.first];
            var se = cx.data[j.second];
            if (info.reverse)
            {
                left = For(fi.Last(cx));
                if (left != null)
                {
                    right = For(se.PositionAt(cx, left._bmk.key()));
                    var r = new FDJoinBookmark(cx, j, left, right, 0);
                    cx.finder = ox;
                    return r;
                }
            }
            else
            {
                right = For(se.Last(cx));
                if (right != null)
                {
                    left = For(fi.PositionAt(cx, right._bmk.key()));
                    var r = new FDJoinBookmark(cx, j, left, right, 0);
                    cx.finder = ox;
                    return r;
                }
            }
            cx.finder = ox;
            return null;
        }
        static IndexRowSet.IndexCursor For(Cursor c)
        {
            var sc = (SelectedRowSet.SelectedCursor)c;
            return (IndexRowSet.IndexCursor)sc._bmk;
        }
        /// <summary>
        /// Move to the next row in a functional-dependent rowset
        /// </summary>
        /// <returns>a bookmark for the next row or null</returns>
        protected override Cursor _Next(Context cx)
        {
            var left = (IndexRowSet.IndexCursor)_left;
            var right = (IndexRowSet.IndexCursor)_right;
            var ox = cx.finder;
            cx.finder += _jrs.finder;
            if (_jrs.fdInfo.reverse)
            {
                left = (IndexRowSet.IndexCursor)left.Next(cx);
                if (left != null)
                {
                    right = For(cx.data[_jrs.second].PositionAt(cx,left._bmk.key()));
                    var r = new FDJoinBookmark(cx, _jrs, left, right, _pos + 1);
                    cx.finder = ox;
                    return r;
                }
            }
            else
            {
                right = (IndexRowSet.IndexCursor)right.Next(cx);
                if (right!=null)
                {
                    left = For(cx.data[_jrs.first].PositionAt(cx,right._bmk.key()));
                    var r = new FDJoinBookmark(cx, _jrs, left, right, _pos + 1);
                    cx.finder = ox;
                    return r;
                }
            }
            cx.finder = ox;
            return null;
        }
        protected override Cursor _Previous(Context cx)
        {
            var left = (IndexRowSet.IndexCursor)_left;
            var right = (IndexRowSet.IndexCursor)_right;
            var ox = cx.finder;
            cx.finder += _jrs.finder;
            if (_jrs.fdInfo.reverse)
            {
                left = (IndexRowSet.IndexCursor)left.Previous(cx);
                if (left != null)
                {
                    right = For(cx.data[_jrs.second].PositionAt(cx, left._bmk.key()));
                    var r = new FDJoinBookmark(cx, _jrs, left, right, _pos + 1);
                    cx.finder = ox;
                    return r;
                }
            }
            else
            {
                right = (IndexRowSet.IndexCursor)right.Previous(cx);
                if (right != null)
                {
                    left = For(cx.data[_jrs.first].PositionAt(cx, right._bmk.key()));
                    var r = new FDJoinBookmark(cx, _jrs, left, right, _pos + 1);
                    cx.finder = ox;
                    return r;
                }
            }
            cx.finder = ox;
            return null;
        }
        internal override Cursor _Fix(Context cx)
        {
            return new FDJoinBookmark(cx, this);
        }
    }
    /// <summary>
    /// Enumerator for a left join
    /// </summary>
    internal class LeftJoinBookmark : JoinBookmark
    {
        readonly Cursor hideRight = null;
        /// <summary>
        /// Constructor: a left join enumerator for a join rowset
        /// </summary>
        /// <param name="j">The join rowset</param>
        LeftJoinBookmark(Context _cx,JoinRowSet j, Cursor left, Cursor right,bool ur,int pos) 
            : base(_cx,j,left,true,right,ur,pos)
        {
            // care: ensure you AdvanceToMatch
            hideRight = right;
        }
        LeftJoinBookmark(LeftJoinBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
        { }
        LeftJoinBookmark(Context cx,LeftJoinBookmark cu):base(cx,cu)
        {
            hideRight = cu.hideRight?._Fix(cx);
        }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new LeftJoinBookmark(this, cx, p, v);
        }
        /// <summary>
        /// A new leftjoin bookmark
        /// </summary>
        /// <param name="j">the join row set</param>
        /// <returns>a bookmark for the first entry or null if there is none</returns>
        internal static LeftJoinBookmark New(Context cx,JoinRowSet j)
        {
            var left = cx.data[j.first].First(cx);
            var right = cx.data[j.second].First(cx);
            if (left == null)
                return null;
            for (;;)
            {
                if (right == null)
                    return new LeftJoinBookmark(cx,j, left, null, false, 0);
                var bm = new LeftJoinBookmark(cx,j, left, right,true,0);
                int c = j.Compare(cx);
                if (c == 0)
                    return bm;
                if (c < 0)
                    return new LeftJoinBookmark(cx,j, left, right, false, 0);
                else
                    right = right.Next(cx);
            }
        }
        internal static LeftJoinBookmark New(JoinRowSet j, Context cx)
        {
            var left = cx.data[j.first].Last(cx);
            var right = cx.data[j.second].Last(cx);
            if (left == null)
                return null;
            for (; ; )
            {
                if (right == null)
                    return new LeftJoinBookmark(cx, j, left, null, false, 0);
                var bm = new LeftJoinBookmark(cx, j, left, right, true, 0);
                int c = j.Compare(cx);
                if (c == 0)
                    return bm;
                if (c > 0)
                    return new LeftJoinBookmark(cx, j, left, right, false, 0);
                else
                    right = right.Previous(cx);
            }
        }
        /// <summary>
        /// Move to the next row in a left join
        /// </summary>
        /// <returns>a bookmark for the next row or null if none</returns>
        protected override Cursor _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            if ((_left != null && left == null) || (_right != null && right == null))
                throw new PEException("PE388");
            right = hideRight;
            if (_useRight && right.Mb() is MTreeBookmark mr && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Next(_cx);
                return new LeftJoinBookmark(_cx,_jrs, left, right, true, _pos + 1);
            }
            left = left.Next(_cx);
            if (left == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useRight)
            {
                var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                    right.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                if (mb != null && left.ResetToTiesStart(_cx, mb) is LeftJoinBookmark rel)
                    left = rel;
                else
                    right = right.Next(_cx);
            }
            for (;;)
            {
                if (left == null)
                    return null;
                if (right == null)
                    return new LeftJoinBookmark(_cx,_jrs, left, null, false, _pos + 1);
                var ret = new LeftJoinBookmark(_cx,_jrs, left, right, true, _pos + 1);
                int c = _jrs.Compare(_cx);
                if (c == 0)
                    return ret;
                if (c < 0)
                    return new LeftJoinBookmark(_cx,_jrs, left, right, false, _pos + 1);
                else
                    right = right.Next(_cx);
            }
        }
        protected override Cursor _Previous(Context _cx)
        {
            var left = _left;
            var right = _right;
            if ((_left != null && left == null) || (_right != null && right == null))
                throw new PEException("PE388");
            right = hideRight;
            if (_useRight && right.Mb() is MTreeBookmark mr && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Previous(_cx);
                return new LeftJoinBookmark(_cx, _jrs, left, right, true, _pos + 1);
            }
            left = left.Previous(_cx);
            if (left == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useRight)
            {
                var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                    right.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                if (mb != null && left.ResetToTiesStart(_cx, mb) is LeftJoinBookmark rel)
                    left = rel;
                else
                    right = right.Previous(_cx);
            }
            for (; ; )
            {
                if (left == null)
                    return null;
                if (right == null)
                    return new LeftJoinBookmark(_cx, _jrs, left, null, false, _pos + 1);
                var ret = new LeftJoinBookmark(_cx, _jrs, left, right, true, _pos + 1);
                int c = _jrs.Compare(_cx);
                if (c == 0)
                    return ret;
                if (c > 0)
                    return new LeftJoinBookmark(_cx, _jrs, left, right, false, _pos + 1);
                else
                    right = right.Previous(_cx);
            }
        }
        internal override Cursor _Fix(Context cx)
        {
            return new LeftJoinBookmark(cx,this);
        }
    }
    internal class RightJoinBookmark : JoinBookmark
    {
        /// <summary>
        /// Constructor: a right join enumerator for a join rowset
        /// </summary>
        /// <param name="j">The join rowset</param>
        RightJoinBookmark(Context _cx,JoinRowSet j, Cursor left, bool ul, Cursor right,int pos) 
            : base(_cx,j,left,ul,right,true,pos)
        {
            // care: ensure you AdvanceToMatch
        }
        RightJoinBookmark(RightJoinBookmark cu,Context cx,long p,TypedValue v):base(cu, cx, p, v) 
        { }
        RightJoinBookmark(Context cx, RightJoinBookmark cu) : base(cx, cu) { }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new RightJoinBookmark(this, cx, p, v);
        }
        /// <summary>
        /// a bookmark for the right join
        /// </summary>
        /// <param name="j">the join row set</param>
        /// <returns>the bookmark for the first row or null if none</returns>
        internal static RightJoinBookmark New(Context cx,JoinRowSet j)
        {
            var left = cx.data[j.first].First(cx);
            var right = cx.data[j.second].First(cx);
            if (right == null)
                return null;
            for (;;)
            {
                if (left == null)
                    return new RightJoinBookmark(cx,j, null, false, right, 0);
                var bm = new RightJoinBookmark(cx,j, left, true, right,0);
                int c = j.Compare( cx);
                if (c == 0)
                    return bm;
                if (c < 0)
                    left = left.Next(cx);
                else
                    return new RightJoinBookmark(cx,j, left, false, right, 0);
            }
        }
        internal static RightJoinBookmark New(JoinRowSet j,Context cx)
        {
            var left = cx.data[j.first].Last(cx);
            var right = cx.data[j.second].Last(cx);
            if (right == null)
                return null;
            for (; ; )
            {
                if (left == null)
                    return new RightJoinBookmark(cx, j, null, false, right, 0);
                var bm = new RightJoinBookmark(cx, j, left, true, right, 0);
                int c = j.Compare(cx);
                if (c == 0)
                    return bm;
                if (c > 0)
                    left = left.Previous(cx);
                else
                    return new RightJoinBookmark(cx, j, left, false, right, 0);
            }
        }
        /// <summary>
        /// Move to the next row in a right join
        /// </summary>
        /// <returns>whether there is a next row</returns>
        protected override Cursor _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (_useLeft && right?.Mb() is MTreeBookmark mr && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Next(_cx);
                return new RightJoinBookmark(_cx,_jrs, left, true, right, _pos + 1);
            }
            right = right.Next(_cx);
            if (right == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useLeft)
            {
                var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                    left.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                if (mb != null && right.ResetToTiesStart(_cx, mb) is RightJoinBookmark rer)
                    right = rer;
                else
                    left = left.Next(_cx);
            }
            for (;;)
            {
                if (right == null)
                    return null;
                if (left == null)
                    return new RightJoinBookmark(_cx,_jrs, null, false, right, _pos + 1);
                var ret = new RightJoinBookmark(_cx,_jrs, left, true, right, _pos + 1);
                int c = _jrs.Compare(_cx);
                if (c == 0)
                    return ret;
                if (c < 0)
                    left = left.Next(_cx);
                else
                    return new RightJoinBookmark(_cx,_jrs, left, false, right, _pos + 1);
            }
        }
        protected override Cursor _Previous(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (_useLeft && right?.Mb() is MTreeBookmark mr && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Previous(_cx);
                return new RightJoinBookmark(_cx, _jrs, left, true, right, _pos + 1);
            }
            right = right.Previous(_cx);
            if (right == null)
                return null;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useLeft)
            {
                var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)_jrs.joinCond.Count)) ? null :
                    left.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                if (mb != null && right.ResetToTiesStart(_cx, mb) is RightJoinBookmark rer)
                    right = rer;
                else
                    left = left.Previous(_cx);
            }
            for (; ; )
            {
                if (right == null)
                    return null;
                if (left == null)
                    return new RightJoinBookmark(_cx, _jrs, null, false, right, _pos + 1);
                var ret = new RightJoinBookmark(_cx, _jrs, left, true, right, _pos + 1);
                int c = _jrs.Compare(_cx);
                if (c == 0)
                    return ret;
                if (c > 0)
                    left = left.Previous(_cx);
                else
                    return new RightJoinBookmark(_cx, _jrs, left, false, right, _pos + 1);
            }
        }
        internal override Cursor _Fix(Context cx)
        {
            return new RightJoinBookmark(cx, this);
        }
    }
    /// <summary>
    /// A full join bookmark for a join row set
    /// </summary>
    internal class FullJoinBookmark : JoinBookmark
    {
        /// <summary>
        /// Constructor: a full join bookmark for a join rowset
        /// </summary>
        /// <param name="j">The join rowset</param>
        FullJoinBookmark(Context _cx,JoinRowSet j, Cursor left, bool ul, Cursor right, 
            bool ur, int pos)
            : base(_cx,j, left, ul, right, ur, pos)
        { }
        FullJoinBookmark(FullJoinBookmark cu,Context cx, long p, TypedValue v):base(cu,cx,p,v)
        { }
        FullJoinBookmark(Context cx, FullJoinBookmark cu) : base(cx, cu) { }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new FullJoinBookmark(this, cx, p, v);
        }
        /// <summary>
        /// A new bookmark for a full join
        /// </summary>
        /// <param name="j">the join row set</param>
        /// <returns>a bookmark for the first row or null if none</returns>
        internal static FullJoinBookmark New(Context cx,JoinRowSet j)
        {
            var left = cx.data[j.first].First(cx);
            var right = cx.data[j.second].First(cx);
            if (left == null && right == null)
                return null;
            var bm = new FullJoinBookmark(cx,j, left, true, right, true, 0);
            int c = j.Compare(cx);
            if (c == 0)
                return bm;
            if (c < 0)
                return new FullJoinBookmark(cx,j, left, true, right, false, 0);
            return new FullJoinBookmark(cx,j, left, false, right, true, 0);
        }
        internal static FullJoinBookmark New(JoinRowSet j, Context cx)
        {
            var left = cx.data[j.first].Last(cx);
            var right = cx.data[j.second].Last(cx);
            if (left == null && right == null)
                return null;
            var bm = new FullJoinBookmark(cx, j, left, true, right, true, 0);
            int c = j.Compare(cx);
            if (c == 0)
                return bm;
            if (c > 0)
                return new FullJoinBookmark(cx, j, left, true, right, false, 0);
            return new FullJoinBookmark(cx, j, left, false, right, true, 0);
        }
        /// <summary>
        /// Move to the next row in a full join
        /// </summary>
        /// <returns>a bookmark for the next row or null if none</returns>
        protected override Cursor _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (_useLeft && _useRight && right.Mb() is MTreeBookmark mr 
                && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Next(_cx);
                return new FullJoinBookmark(_cx,_jrs, left, true, right, true, _pos + 1);
            }
            if (_useLeft)
                left = left.Next(_cx);
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useRight)
            {
                if (_useLeft)
                {
                    var mb = (left == null || (left.Mb() is MTreeBookmark ml 
                        && ml.changed((int)_jrs.joinCond.Count))) ? null :
                        right.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                    if (mb != null)
                        right = right.ResetToTiesStart(_cx,mb);
                    else
                        right = right.Next(_cx);
                }
                else
                    right = right.Next(_cx);
            }
            if (left == null && right == null)
                return null;
            if (left == null)
                return new FullJoinBookmark(_cx,_jrs, null, false, right, true, _pos + 1);
            if (right == null)
                return new FullJoinBookmark(_cx,_jrs, left, true, right, false, _pos + 1);
            new FullJoinBookmark(_cx,_jrs, left, true, right, true, _pos + 1);
            int c = _jrs.Compare(_cx);
            return new FullJoinBookmark(_cx,_jrs, left, c <= 0, right, c >= 0, _pos + 1);
        }
        protected override Cursor _Previous(Context _cx)
        {
            var left = _left;
            var right = _right;
            if (_useLeft && _useRight && right.Mb() is MTreeBookmark mr
                && mr.hasMore((int)_jrs.joinCond.Count))
            {
                right = right.Previous(_cx);
                return new FullJoinBookmark(_cx, _jrs, left, true, right, true, _pos + 1);
            }
            if (_useLeft)
                left = left.Previous(_cx);
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useRight)
            {
                if (_useLeft)
                {
                    var mb = (left == null || (left.Mb() is MTreeBookmark ml
                        && ml.changed((int)_jrs.joinCond.Count))) ? null :
                        right.Mb()?.ResetToTiesStart((int)_jrs.joinCond.Count);
                    if (mb != null)
                        right = right.ResetToTiesStart(_cx, mb);
                    else
                        right = right.Previous(_cx);
                }
                else
                    right = right.Previous(_cx);
            }
            if (left == null && right == null)
                return null;
            if (left == null)
                return new FullJoinBookmark(_cx, _jrs, null, false, right, true, _pos + 1);
            if (right == null)
                return new FullJoinBookmark(_cx, _jrs, left, true, right, false, _pos + 1);
            new FullJoinBookmark(_cx, _jrs, left, true, right, true, _pos + 1);
            int c = _jrs.Compare(_cx);
            return new FullJoinBookmark(_cx, _jrs, left, c >= 0, right, c <= 0, _pos + 1);
        }
        internal override Cursor _Fix(Context cx)
        {
            return new FullJoinBookmark(cx, this);
        }
    }
    /// <summary>
    /// A cross join bookmark for a join row set
    /// </summary>
    internal class CrossJoinBookmark : JoinBookmark
    {
        /// <summary>
        /// Constructor: a cross join bookmark for a join row set
        /// </summary>
        /// <param name="j">a join row set</param>
        CrossJoinBookmark(Context _cx,JoinRowSet j, Cursor left = null, Cursor right = null,
            int pos=0) : base(_cx,j,left,true,right,true,pos)
        { }
        CrossJoinBookmark(CrossJoinBookmark cu, Context cx, long p, TypedValue v) 
            : base(cu, cx, p, v) { }
        CrossJoinBookmark(Context cx,CrossJoinBookmark cu):base(cx,cu)
        { }
        public static CrossJoinBookmark New(Context cx,JoinRowSet j)
        {
            var f = cx.data[j.first].First(cx);
            var s = cx.data[j.second].First(cx);
            if (f == null) // don't test s (possible lateral dependency)
                return null;
            for (;; )
            {
                if (s != null)
                {
                    var rb = new CrossJoinBookmark(cx, j, f, s);
                    if (DBObject.Eval(j.joinCond, cx))
                        return rb;
                    s = s.Next(cx);
                }
                if (s == null)
                {
                    f = f.Next(cx);
                    if (f == null)
                        return null;
                    s = cx.data[j.second].First(cx);
                }
            }
        }
        public static CrossJoinBookmark New(JoinRowSet j,Context cx)
        {
            var f = cx.data[j.first].Last(cx);
            var s = cx.data[j.second].Last(cx);
            if (f == null || s == null)
                return null;
            for (; ; )
            {
                if (s != null)
                {
                    var rb = new CrossJoinBookmark(cx, j, f, s);
                    if (DBObject.Eval(j.joinCond, cx))
                        return rb;
                    s = s.Previous(cx);
                }
                if (s == null)
                {
                    f = f.Previous(cx);
                    if (f == null)
                        return null;
                    s = cx.data[j.second].Last(cx);
                }
            }
        }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new CrossJoinBookmark(this, cx, p, v);
        }
        /// <summary>
        /// Move to the next row in the cross join
        /// </summary>
        /// <returns>a bookmark for the next row or null if none</returns>
        protected override Cursor _Next(Context cx)
        {
            var left = _left;
            var right = _right;
            right = right.Next(cx);
            for (; ; )
            {
                if (right != null)
                {
                    var rb = new CrossJoinBookmark(cx, _jrs, left, right, _pos + 1);
                    if (DBObject.Eval(_jrs.joinCond, cx))
                        return rb;
                    right = right.Next(cx);
                }
                if (right == null)
                {
                    left = left.Next(cx);
                    if (left == null)
                        return null;
                    right = cx.data[_jrs.second].First(cx);
                }
            }
        }
        protected override Cursor _Previous(Context cx)
        {
            var left = _left;
            var right = _right;
            right = right.Previous(cx);
            for (; ; )
            {
                if (right != null)
                {
                    var rb = new CrossJoinBookmark(cx, _jrs, left, right, _pos + 1);
                    if (DBObject.Eval(_jrs.joinCond, cx))
                        return rb;
                    right = right.Previous(cx);
                }
                if (right == null)
                {
                    left = left.Previous(cx);
                    if (left == null)
                        return null;
                    right = cx.data[_jrs.second].Last(cx);
                }
            }
        }
        internal override Cursor _Fix(Context cx)
        {
            return new CrossJoinBookmark(cx, this);
        }
    }
    /// <summary>
    /// A join bookmark for a lateral join row set
    /// </summary>
    internal class LateralJoinBookmark : JoinBookmark
    {
        /// <summary>
        /// Constructor: a cross join bookmark for a join row set
        /// </summary>
        /// <param name="j">a join row set</param>
        LateralJoinBookmark(Context _cx, JoinRowSet j, Cursor left = null, Cursor right = null,
            int pos = 0) : base(_cx, j, left, true, right, true, pos)
        { }
        LateralJoinBookmark(LateralJoinBookmark cu, Context cx, long p, TypedValue v) 
            : base(cu, cx, p, v) { }
        internal LateralJoinBookmark(Context cx, JoinBookmark cu)
            : base(cx, cu) { }
        LateralJoinBookmark(Context cx, LateralJoinBookmark cu) : base(cx, cu) { }
        public static LateralJoinBookmark New(Context cx, JoinRowSet j)
        {
            var f = cx.data[j.first].First(cx);
            var s = cx.data[j.second].First(cx);
            if (f == null || s == null)
                return null;
            Console.WriteLine("LJB " + f.ToString() + "|" + s.ToString());
            return new LateralJoinBookmark(cx, j, f, s);
        }
        public static LateralJoinBookmark New(JoinRowSet j,Context cx)
        {
            var f = cx.data[j.first].Last(cx);
            var s = cx.data[j.second].Last(cx);
            if (f == null || s == null)
                return null;
            Console.WriteLine("LJB " + f.ToString() + "|" + s.ToString());
            return new LateralJoinBookmark(cx, j, f, s);
        }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new LateralJoinBookmark(this, cx, p, v);
        }
        /// <summary>
        /// Move to the next row in the lateral join
        /// </summary>
        /// <returns>a bookmark for the next row or null if none</returns>
        protected override Cursor _Next(Context cx)
        {
            var left = _left;
            var right = _right;
            var was = right._needed;
            right = right.Next(cx);
            for (; ; )
            {
                if (right != null)
                {
                    var rb = new LateralJoinBookmark(cx, _jrs, left, right, _pos + 1);
                    if (_jrs.Compare(cx) == 0)
                        return rb;
                    right = right.Next(cx);
                    continue;
                }
                left = left.Next(cx);
                if (left == null)
                    return null;
                var se = cx.data[_jrs.second];
                var second = se.MaybeBuild(cx, was);
                right = second.First(cx);
            }
        }
        protected override Cursor _Previous(Context cx)
        {
            var left = _left;
            var right = _right;
            var was = right._needed;
            right = right.Previous(cx);
            for (; ; )
            {
                if (right != null)
                {
                    var rb = new LateralJoinBookmark(cx, _jrs, left, right, _pos + 1);
                    if (_jrs.Compare(cx) == 0)
                        return rb;
                    right = right.Previous(cx);
                    continue;
                }
                left = left.Next(cx);
                if (left == null)
                    return null;
                var se = cx.data[_jrs.second];
                var second = se.MaybeBuild(cx, was);
                right = second.Last(cx);
            }
        }
        internal override Cursor _Fix(Context cx)
        {
            return new LateralJoinBookmark(cx, this);
        }
    }

}

