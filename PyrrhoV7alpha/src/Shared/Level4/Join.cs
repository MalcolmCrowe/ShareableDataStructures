using System;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

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
		internal readonly RowSet first,second;
        /// <summary>
        /// Constructor: build the rowset for the Join
        /// </summary>
        /// <param name="j">The Join part</param>
		public JoinRowSet(Context _cx, JoinPart j,RowSet lr,RowSet rr) : 
            base(j.defpos,_cx,j.rowType.info,_Finder(lr,rr),null,j.where,j.ordSpec,j.matches,
                Context.Copy(j.matching))
		{
            join = j;
            first = lr;
            second = rr;
        }
        JoinRowSet(long dp,Context cx,JoinRowSet jrs):base(dp,cx,jrs)
        {
            join = jrs.join;
            first = jrs.first;
            second = jrs.second;

        }
        static BTree<long,long> _Finder(RowSet lr,RowSet rr)
        {
            var r = lr.finder;
            for (var b=rr.finder.First();b!=null;b=b.Next())
                r += (b.key(),b.value());
            return r;
        }
        protected JoinRowSet(JoinRowSet rs, long a, long b) : base(rs, a, b)
        {
            join = rs.join;
            first = rs.first;
            second=rs.second;
        }
        internal override RowSet New(long a, long b)
        {
            return new JoinRowSet(this, a, b);
        }
        internal override RowSet New(long dp, Context cx)
        {
            return new JoinRowSet(dp, cx, this);
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
            if (rx!=null)
            {
                if (fr==true)
                {
                    sb.Append(ft);
                    sb.Append(" " + j.left.ToString());
                    first.Matches(sb);
                    Conds(sb, j.left.where, " where ");
                    sb.Append(" "+j.right.ToString());
                    second.Matches(sb);
                    Conds(sb, j.right.where, " where ");
                } else
                {
                    sb.Append(ft);
                    sb.Append(" " + j.right.ToString());
                    second.Matches(sb);
                    Conds(sb, j.right.where, " where ");
                    sb.Append(" "+j.left.ToString());
                    first.Matches(sb);
                    Conds(sb, j.left.where, " where ");
                }
            }
            else if (fr == false)
            {
                sb.Append(ft + j.left.ToString());
                first.Matches(sb);
                sb.Append(" "+j.right.ToString());
                second.Matches(sb);
                Conds(sb, j.right.where, " where ");
            }
            else if (fr == true)
            {
                sb.Append(" "+j.left.ToString());
                first.Matches(sb);
                sb.Append(ft+j.right.ToString());
                second.Matches(sb);
                Conds(sb, j.right.where," where ");
            }
            base._Strategy(sb, indent);
            if (fr != false)
                first?.Strategy(indent);
            if (fr != true)
                second?.Strategy(indent);
        }
        /// <summary>
        /// Set up a bookmark for the rows of this join
        /// </summary>
        /// <param name="matches">matching information</param>
        /// <returns>the enumerator</returns>
        public override Cursor First(Context _cx)
        {
            JoinPart j = join;
            var ox = _cx.from;
            _cx.from = _finder;
            JoinBookmark r;
            switch (j.kind)
            {
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
            _cx.from = ox;
            return b;
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
        static TRow _Vals(JoinRowSet jrs, Cursor left, bool ul, Cursor right, bool ur)
        {
            var vs = BTree<long, TypedValue>.Empty;
            for (var b = jrs.info.columns.First(); b != null; b = b.Next())
            {
                var p = b.value().defpos;
                vs += (p, (ul?left[p]:null) ?? (ur?right[p]:null)??TNull.Value);
            }
            return new TRow(jrs.info.domain, vs);
        }
        protected abstract JoinBookmark _Next(Context _cx);
        public override Cursor Next(Context _cx)
        {
            return _Next(_cx)?.MoveToMatch(_cx);
        }
        internal Cursor MoveToMatch(Context _cx)
        {
            var r = this;
            while (r != null && !Query.Eval(_jrs.where, _cx))
                r = r._Next(_cx);
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
        internal static InnerJoinBookmark New(Context _cx,JoinRowSet j)
        {
            var left = j.first.First(_cx);
            var right = j.second.First(_cx);
            if (left == null || right == null)
                return null;
            var join = j.join;
            for (;;)
            {
                var bm = new InnerJoinBookmark(_cx,j, left, right);
                int c = join.Compare(_cx);
                if (c == 0)
                    return bm;
                if (c < 0)
                {
                    if ((left = left.Next(_cx)) == null)
                        return null;
                }
                else if ((right = right.Next(_cx)) == null)
                    return null;
            }
        }
        /// <summary>
        /// Move to the next row in the inner join
        /// </summary>
        /// <returns>whether there is a next row</returns>
        protected override JoinBookmark _Next(Context _cx)
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
            : base(_cx,rs,left,true,right,true,pos,_Row(rs,left,right))
        {
            join = rs.join;
            info = join.FDInfo;
        }
        static TRow _Row(JoinRowSet rs,Cursor left,Cursor right)
        {
            var vs = BTree<long, TypedValue>.Empty;
            for (var b=rs.join.left.rowType.First();b!=null;b=b.Next())
            {
                var s = b.value();
                var p = (s is SqlCopy sc) ? sc.copyFrom : 
                    (s is SqlTableCol st) ? st.tableCol.defpos : -1L;
                vs += (s.defpos, left[p]);
            }
            for (var b = rs.join.right.rowType.First(); b != null; b = b.Next())
            {
                var s = b.value();
                var p = (s is SqlCopy sc) ? sc.copyFrom :
                    (s is SqlTableCol st) ? st.tableCol.defpos : -1L;
                vs += (s.defpos, right[p]);
            }
            return new TRow(rs.info.domain, vs);
        }
        /// <summary>
        /// A new FD bookmark
        /// </summary>
        /// <param name="j">the join rowset</param>
        /// <returns>the bookmark</returns>
        internal static FDJoinBookmark New(Context _cx, JoinRowSet j)
        {
            var join = j.join;
            var info = join.FDInfo;
            IndexRowSet.IndexCursor left, right;
            var ox = _cx.from;
            _cx.from = j.finder;
            if (info.reverse)
            {
                left = For(j.first.First(_cx));
                if (left != null)
                {
                    right = For(j.second.PositionAt(_cx, left._bmk.key()));
                    var r = new FDJoinBookmark(_cx, j, left, right, 0);
                    _cx.from = ox;
                    return r;
                }
            }
            else
            {
                right = For(j.second.First(_cx));
                if (right!= null)
                {
                    left = For(j.first.PositionAt(_cx,right._bmk.key()));
                    var r = new FDJoinBookmark(_cx, j, left, right, 0);
                    _cx.from = ox;
                    return r;
                }
            }
            _cx.from = ox;
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
        protected override JoinBookmark _Next(Context _cx)
        {
            var left = (IndexRowSet.IndexCursor)_left;
            var right = (IndexRowSet.IndexCursor)_right;
            var ox = _cx.from;
            _cx.from = _jrs.finder;
            if (info.reverse)
            {
                left = (IndexRowSet.IndexCursor)left.Next(_cx);
                if (left != null)
                {
                    right = For(_jrs.second.PositionAt(_cx,left._bmk.key()));
                    var r = new FDJoinBookmark(_cx, _jrs, left, right, _pos + 1);
                    _cx.from = ox;
                    return r;
                }
            }
            else
            {
                right = (IndexRowSet.IndexCursor)right.Next(_cx);
                if (right!=null)
                {
                    left = For(_jrs.first.PositionAt(_cx,right._bmk.key()));
                    var r = new FDJoinBookmark(_cx, _jrs, left, right, _pos + 1);
                    _cx.from = ox;
                    return r;
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
        /// <summary>
        /// A new leftjoin bookmark
        /// </summary>
        /// <param name="j">the join row set</param>
        /// <returns>a bookmark for the first entry or null if there is none</returns>
        internal static LeftJoinBookmark New(Context _cx,JoinRowSet j)
        {
            var left = j.first.First(_cx);
            var right = j.second.First(_cx);
            if (left == null)
                return null;
            var join = j.join;
            for (;;)
            {
                if (right == null)
                    return new LeftJoinBookmark(_cx,j, left, null, false, 0);
                var bm = new LeftJoinBookmark(_cx,j, left, right,true,0);
                int c = join.Compare(_cx);
                if (c == 0)
                    return bm;
                if (c < 0)
                    return new LeftJoinBookmark(_cx,j, left, right, false, 0);
                else
                    right = right.Next(_cx);
            }
        }
        /// <summary>
        /// Move to the next row in a left join
        /// </summary>
        /// <returns>a bookmark for the next row or null if none</returns>
        protected override JoinBookmark _Next(Context _cx)
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
        /// <summary>
        /// a bookmark for the right join
        /// </summary>
        /// <param name="j">the join row set</param>
        /// <returns>the bookmark for the first row or null if none</returns>
        internal static RightJoinBookmark New(Context _cx,JoinRowSet j)
        {
            var left = j.first.First(_cx);
            var right = j.second.First(_cx);
            if (right == null)
                return null;
            var join = j.join;
            for (;;)
            {
                if (left == null)
                    return new RightJoinBookmark(_cx,j, null, false, right, 0);
                var bm = new RightJoinBookmark(_cx,j, left, true, right,0);
                int c = join.Compare( _cx);
                if (c == 0)
                    return bm;
                if (c < 0)
                    left = left.Next(_cx);
                else
                    return new RightJoinBookmark(_cx,j, left, false, right, 0);
            }
        }
        /// <summary>
        /// Move to the next row in a right join
        /// </summary>
        /// <returns>whether there is a next row</returns>
        protected override JoinBookmark _Next(Context _cx)
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
        {
        }
        /// <summary>
        /// A new bookmark for a full join
        /// </summary>
        /// <param name="j">the join row set</param>
        /// <returns>a bookmark for the first row or null if none</returns>
        internal static FullJoinBookmark New(Context _cx,JoinRowSet j)
        {
            var left = j.first.First(_cx);
            var right = j.second.First(_cx);
            if (left == null && right == null)
                return null;
            var join = j.join;
            var bm = new FullJoinBookmark(_cx,j, left, true, right, true, 0);
            int c = join.Compare(_cx);
            if (c == 0)
                return bm;
            if (c < 0)
                return new FullJoinBookmark(_cx,j, left, true, right, false, 0);
            return new FullJoinBookmark(_cx,j, left, false, right, true, 0);
        }
        /// <summary>
        /// Move to the next row in a full join
        /// </summary>
        /// <returns>a bookmark for the next row or null if none</returns>
        protected override JoinBookmark _Next(Context _cx)
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
        public static CrossJoinBookmark New(Context _cx,JoinRowSet j)
        {
            var f = j.first.First(_cx);
            var s = j.second.First(_cx);
            if (f == null || s == null)
                return null;
            return new CrossJoinBookmark(_cx,j, f, s);
        }
        /// <summary>
        /// Move to the next row in the cross join
        /// </summary>
        /// <returns>a bookmark for the next row or null if none</returns>
        protected override JoinBookmark _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            right = right.Next(_cx);
            for (; ; )
            {
                if (right != null)
                    break;
                left = left.Next(_cx);
                if (left == null)
                    return null;
                right = _jrs.second.First(_cx);
            }
            return new CrossJoinBookmark(_cx,_jrs, left, right, _pos + 1);
        }

        internal override TableRow Rec()
        {
            return null;
        }
    }
}

