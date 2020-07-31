using Pyrrho.Common;
using Pyrrho.Level3;
using System;
using System.Configuration;
using System.Runtime.ExceptionServices;
using System.Security.Authentication.ExtendedProtection;
using System.Text;
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
    /// A row set for a Join operation.
    /// </summary>
	internal class JoinRowSet : RowSet
	{
        internal readonly JoinPart join;
        /// <summary>
        /// The two row sets being joined
        /// </summary>
		internal readonly long first,second;
        /// <summary>
        /// Constructor: build the rowset for the Join
        /// </summary>
        /// <param name="j">The Join part</param>
		public JoinRowSet(Context _cx, JoinPart j,RowSet lr,RowSet rr) : 
            base(j.defpos,_cx,j.domain,j.display,_Finder(lr,rr),null,j.where,j.ordSpec,j.matches,
                j.matching)
		{
            join = j;
            first = lr.defpos;
            second = rr.defpos;
        }
        JoinRowSet(Context cx,JoinRowSet jrs, BTree<long,Finder> nd,bool bt)
            :base(cx,jrs,nd,bt)
        {
            join = jrs.join;
            first = jrs.first;
            second = jrs.second;
        }
        static BTree<long,Finder> _Finder(RowSet lr,RowSet rr)
        {
            var r = lr.finder;
            for (var b=rr.finder.First();b!=null;b=b.Next())
                r += (b.key(),b.value());
            return r;
        }
        internal override bool Knows(Context cx, long rp)
        {
            return rp==first || rp==second || base.Knows(cx, rp);
        }
        protected JoinRowSet(JoinRowSet rs, long a, long b) : base(rs, a, b)
        {
            join = rs.join;
            first = rs.first;
            second=rs.second;
        }
        JoinRowSet(Context cx,JoinRowSet rs,Sqlx k) :base(cx,rs,rs.needed,rs.built)
        {
            join = rs.join + (JoinPart.JoinKind, k);
            first = rs.first;
            second = rs.second;
        }
        internal RowSet New(Context cx,Sqlx k)
        {
            return new JoinRowSet(cx, this, k);
        }
        internal override RowSet New(long a, long b)
        {
            return new JoinRowSet(this, a, b);
        }
        internal override RowSet New(Context cx, BTree<long, Finder> nd,bool bt)
        {
            return new JoinRowSet(cx, this, nd, bt);
        }
        internal override BTree<long, Finder> AllWheres(Context cx,BTree<long,Finder> nd)
        {
            nd = cx.Needs(nd,this,where);
            nd = cx.Needs(nd,this,cx.data[first].AllWheres(cx, nd));
            nd = cx.Needs(nd,this,cx.data[second].AllWheres(cx, nd));
            return nd;
        }
        internal override BTree<long, Finder> AllMatches(Context cx,BTree<long,Finder> nd)
        {
            nd = cx.Needs(nd,this,matches);
            nd = cx.Needs(nd,this,cx.data[first].AllMatches(cx,nd));
            nd = cx.Needs(nd,this,cx.data[second].AllMatches(cx,nd));
            return nd;
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            var j = join;
            sb.Append("Join ");
            sb.Append((j.kind == Sqlx.NO) ? "FD" : j.kind.ToString());
            Conds(sb, j.joinCond, " ON ");
            sb.Append(' ');
            var fr = j.FDInfo?.reverse;
            var rx = j.FDInfo?.rindex;
            var ft = (rx!=null)? " foreign " : " primary ";
            base._Strategy(sb, indent);
        }
        /// <summary>
        /// Set up a bookmark for the rows of this join
        /// </summary>
        /// <param name="matches">matching information</param>
        /// <returns>the enumerator</returns>
        protected override Cursor _First(Context _cx)
        {
            JoinPart j = join;
            JoinBookmark r;
            switch (j.kind)
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
        public override Cursor First(Context cx)
        {
            var r = ((JoinBookmark)base.First(cx))?.MoveToMatch(cx);
            if (cx.data[second].needed != BTree<long, Finder>.Empty)
            {
                var jrs = (JoinRowSet)cx.data[defpos];
                cx.data += (defpos, jrs.New(cx, Sqlx.LATERAL));
                r = new LateralJoinBookmark(cx,(JoinBookmark)r);
            }
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" First: ");sb.Append(DBObject.Uid(first));
            sb.Append(" Second: "); sb.Append(DBObject.Uid(second));
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
            bool ur, int pos) : base(_cx, jrs, pos, 0, _Vals(jrs, left, ul, right, ur))
        {
            _jrs = jrs;
            _left = left;
            _useLeft = ul;
            _right = right;
            _useRight = ur;
        }
        internal JoinBookmark(Context _cx, JoinRowSet jrs, Cursor left, bool ul, Cursor right,
    bool ur, int pos,TRow rw) : base(_cx, jrs, pos, 0, rw)
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
            :base(cx,cx.data[cu._jrs.defpos],cu._pos,0,cu) 
        {
            _jrs = (JoinRowSet)cx.data[cu._jrs.defpos];
            _left = cu._left;
            _useLeft = cu._useLeft;
            _right = cu._right;
            _useRight = cu._useRight;
        }
        static TRow _Vals(JoinRowSet jrs, Cursor left, bool ul, Cursor right, bool ur)
        {
            var vs = BTree<long, TypedValue>.Empty;
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
        internal Cursor MoveToMatch(Context _cx)
        {
            Cursor r = this;
            while (r != null && !Query.Eval(_jrs.where, _cx))
                r = r.Next(_cx);
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
            var join = j.join;
            for (;;)
            {
                var bm = new InnerJoinBookmark(cx,j, left, right);
                int c = join.Compare(cx);
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
        /// <summary>
        /// Move to the next row in the inner join
        /// </summary>
        /// <returns>whether there is a next row</returns>
        protected override Cursor _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            var join = _jrs.join;
            if (right.Mb() is MTreeBookmark mb0 && mb0.hasMore((int)join.joinCond.Count))
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
            var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)join.joinCond.Count)) ? null :
                right.Mb()?.ResetToTiesStart((int)join.joinCond.Count);
            if (mb != null)
                right = right.ResetToTiesStart(_cx,mb);
            else
                right = right.Next(_cx);
            if (right == null)
                return null;
            for (; ; )
            {
                var ret = new InnerJoinBookmark(_cx,_jrs, left, right, _pos + 1);
                int c = join.Compare( _cx);
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

        internal override TableRow Rec()
        {
            return null;
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
        /// Functional-dependency information collected during JoinPart.Conditions
        /// </summary>
        readonly FDJoinPart info;
        readonly JoinPart join;
        /// <summary>
        /// Constructor for the functional-dependent join
        /// </summary>
        /// <param name="rs">The Join RowSet</param>
        FDJoinBookmark(Context _cx,JoinRowSet rs, IndexRowSet.IndexCursor left, 
            IndexRowSet.IndexCursor right,int pos) 
            : base(_cx,rs,left,true,right,true,pos,_Row(_cx,rs,left,right))
        {
            join = rs.join;
            info = join.FDInfo;
        }
        FDJoinBookmark(FDJoinBookmark cu,Context cx, long p,TypedValue v):base(cu,cx,p,v)
        {
            join = cu.join;
            info = cu.info;
        }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new FDJoinBookmark(this,cx,p,v);
        }
        static TRow _Row(Context cx,JoinRowSet rs,Cursor left,Cursor right)
        {
            var vs = BTree<long, TypedValue>.Empty;
            var lf = (Query)cx.obs[rs.join.left];
            var rg = (Query)cx.obs[rs.join.right];
            for (var b=lf.rowType.First();b!=null;b=b.Next())
            {
                var s = cx.obs[b.value()];
                var p = (s is SqlCopy sc) ? sc.copyFrom : -1L;
                vs += (s.defpos, left[p]);
            }
            for (var b = rg.rowType.First(); b != null; b = b.Next())
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
            var join = j.join;
            var info = join.FDInfo;
            IndexRowSet.IndexCursor left, right;
            var ox = cx.from;
            cx.from += j.finder;
            var fi = cx.data[j.first];
            var se = cx.data[j.second];
            if (info.reverse)
            {
                left = For(fi.First(cx));
                if (left != null)
                {
                    right = For(se.PositionAt(cx, left._bmk.key()));
                    var r = new FDJoinBookmark(cx, j, left, right, 0);
                    cx.from = ox;
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
                    cx.from = ox;
                    return r;
                }
            }
            cx.from = ox;
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
            var ox = cx.from;
            cx.from += _jrs.finder;
            if (info.reverse)
            {
                left = (IndexRowSet.IndexCursor)left.Next(cx);
                if (left != null)
                {
                    right = For(cx.data[_jrs.second].PositionAt(cx,left._bmk.key()));
                    var r = new FDJoinBookmark(cx, _jrs, left, right, _pos + 1);
                    cx.from = ox;
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
                    cx.from = ox;
                    return r;
                }
            }
            cx.from = ox;
            return null;
        }

        internal override TableRow Rec()
        {
            return null;
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
            var join = j.join;
            for (;;)
            {
                if (right == null)
                    return new LeftJoinBookmark(cx,j, left, null, false, 0);
                var bm = new LeftJoinBookmark(cx,j, left, right,true,0);
                int c = join.Compare(cx);
                if (c == 0)
                    return bm;
                if (c < 0)
                    return new LeftJoinBookmark(cx,j, left, right, false, 0);
                else
                    right = right.Next(cx);
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
            var join = _jrs.join;
            right = hideRight;
            if (_useRight && right.Mb() is MTreeBookmark mr && mr.hasMore((int)join.joinCond.Count))
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
                var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)join.joinCond.Count)) ? null :
                    right.Mb()?.ResetToTiesStart((int)join.joinCond.Count);
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
                int c = join.Compare(_cx);
                if (c == 0)
                    return ret;
                if (c < 0)
                    return new LeftJoinBookmark(_cx,_jrs, left, right, false, _pos + 1);
                else
                    right = right.Next(_cx);
            }
        }

        internal override TableRow Rec()
        {
            return null;
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
            var join = j.join;
            for (;;)
            {
                if (left == null)
                    return new RightJoinBookmark(cx,j, null, false, right, 0);
                var bm = new RightJoinBookmark(cx,j, left, true, right,0);
                int c = join.Compare( cx);
                if (c == 0)
                    return bm;
                if (c < 0)
                    left = left.Next(cx);
                else
                    return new RightJoinBookmark(cx,j, left, false, right, 0);
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
            var join = _jrs.join;
            if (_useLeft && right?.Mb() is MTreeBookmark mr && mr.hasMore((int)join.joinCond.Count))
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
                var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)join.joinCond.Count)) ? null :
                    left.Mb()?.ResetToTiesStart((int)join.joinCond.Count);
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
                int c = join.Compare(_cx);
                if (c == 0)
                    return ret;
                if (c < 0)
                    left = left.Next(_cx);
                else
                    return new RightJoinBookmark(_cx,_jrs, left, false, right, _pos + 1);
            }
        }

        internal override TableRow Rec()
        {
            return null;
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
            var join = j.join;
            var bm = new FullJoinBookmark(cx,j, left, true, right, true, 0);
            int c = join.Compare(cx);
            if (c == 0)
                return bm;
            if (c < 0)
                return new FullJoinBookmark(cx,j, left, true, right, false, 0);
            return new FullJoinBookmark(cx,j, left, false, right, true, 0);
        }
        /// <summary>
        /// Move to the next row in a full join
        /// </summary>
        /// <returns>a bookmark for the next row or null if none</returns>
        protected override Cursor _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            var join = _jrs.join;
            if (_useLeft && _useRight && right.Mb() is MTreeBookmark mr 
                && mr.hasMore((int)join.joinCond.Count))
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
                    var mb = (left == null || (left.Mb() is MTreeBookmark ml && ml.changed((int)join.joinCond.Count))) ? null :
                        right.Mb()?.ResetToTiesStart((int)join.joinCond.Count);
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
            int c = join.Compare(_cx);
            return new FullJoinBookmark(_cx,_jrs, left, c <= 0, right, c >= 0, _pos + 1);
        }

        internal override TableRow Rec()
        {
            return null;
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
        CrossJoinBookmark(CrossJoinBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v) { }
        public static CrossJoinBookmark New(Context cx,JoinRowSet j)
        {
            var f = cx.data[j.first].First(cx);
            var s = cx.data[j.second].First(cx);
            if (f == null || s == null)
                return null;
            return new CrossJoinBookmark(cx,j, f, s);
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
                    break;
                left = left.Next(cx);
                if (left == null)
                    return null;
                right = cx.data[_jrs.second].First(cx);
            }
            return new CrossJoinBookmark(cx,_jrs, left, right, _pos + 1);
        }

        internal override TableRow Rec()
        {
            return null;
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
        public static LateralJoinBookmark New(Context cx, JoinRowSet j)
        {
            var f = cx.data[j.first].First(cx);
            var s = cx.data[j.second].First(cx);
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
            var se = cx.data[_jrs.second];
            var second = se.MaybeBuild(cx,right._needed);
            if (second != se)
                right = cx.cursors[second.defpos];
            else
                right = right.Next(cx);
            for (; ; )
            {
                if (right != null)
                    break;
                left = left.Next(cx);
                if (left == null)
                    return null;
                right = second.First(cx);
            }
            return new LateralJoinBookmark(cx, _jrs, left, right, _pos + 1);
        }

        internal override TableRow Rec()
        {
            return null;
        }
    }

}

