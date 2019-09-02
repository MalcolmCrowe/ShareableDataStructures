using System;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level4
{
    /// <summary>
    /// A row set for a Join part
    /// </summary>
	internal class JoinRowSet : RowSet
	{
        public override string keywd()
        {
            return " Join ";
        }
        /// <summary>
        /// The two row sets being joined
        /// </summary>
		internal RowSet first,second;
        /// <summary>
        /// Constructor: build the rowset for the Join
        /// </summary>
        /// <param name="j">The Join part</param>
		public JoinRowSet(Context _cx, JoinPart j,RowSet lr,RowSet rr) : base(lr._tr,_cx,j)
		{
            first = lr;
            second = rr;
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            var j = qry as JoinPart;
            sb.Append("Join ");
            sb.Append((j.kind == Sqlx.NO) ? "FD" : j.kind.ToString());
            Conds(sb, j.joinCond, " ON ");
            sb.Append(' ');
            var fr = j.FDInfo?.reverse;
            var rx = j.FDInfo?.rindexDefPos;
            var ft = (rx!=null)? " foreign " : " primary ";
            var index = _tr.schemaRole.objects[j.FDInfo.indexDefPos] as Index;
            if (rx!=null)
            {
                if (fr==true)
                {
                    sb.Append(ft);
                    Cols(sb, index);
                    sb.Append(" " + j.left.ToString());
                    first.Matches(sb);
                    Conds(sb, j.left.where, " where ");
                    sb.Append(" "+j.right.ToString());
                    second.Matches(sb);
                    Conds(sb, j.right.where, " where ");
                } else
                {
                    sb.Append(ft);
                    Cols(sb, index);
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
        protected override RowBookmark _First(Context _cx)
        {
            JoinPart j = qry as JoinPart;
            switch (j.kind)
            {
                case Sqlx.CROSS: return CrossJoinBookmark.New(_cx,this)?.MoveToMatch(_cx);
                case Sqlx.INNER: return InnerJoinBookmark.New(_cx,this)?.MoveToMatch(_cx);
                case Sqlx.LEFT: return LeftJoinBookmark.New(_cx,this)?.MoveToMatch(_cx);
                case Sqlx.RIGHT: return RightJoinBookmark.New(_cx,this)?.MoveToMatch(_cx);
                case Sqlx.FULL: return FullJoinBookmark.New(_cx,this)?.MoveToMatch(_cx);
                case Sqlx.NO: return FDJoinBookmark.New(_cx,this)?.MoveToMatch(_cx);
                default:
                    throw new PEException("PE57");
            }
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
	internal abstract class JoinBookmark : RowBookmark
	{
        /// <summary>
        /// The associated part row set
        /// </summary>
		internal readonly JoinRowSet _jrs;
        protected readonly RowBookmark _left, _right;
        protected readonly bool _useLeft, _useRight;
        internal JoinBookmark(Context _cx, JoinRowSet jrs,RowBookmark left,bool ul,RowBookmark right,
            bool ur,int pos) :base(_cx,jrs,pos,0)
        {
            _jrs = jrs;
            _left = left;
            _useLeft = ul;
            _right = right;
            _useRight = ur;
        }
        protected BTree<long,TypedValue> Value(Context _cx)
        {
            var r = BTree<long, TypedValue>.Empty;
            for (var b = _jrs.rowType.columns.First(); b != null; b = b.Next())
            {
                var p = b.value().defpos;
                r += (p, _cx.values[p]);
            }
            return r;
        }
        protected TypedValue Get(Context _cx, string n)
        {
            TypedValue v = null;
            if (_useLeft)
            {
                var t = _jrs.first.rowType;
                var s = t.names[n];
                var j = s.seq;
                if (j >= 0)
                {
                    if (_left._rs.qry is SelectQuery qs && j < qs.cols.Count)
                        v = qs.cols[j].Eval(_rs._tr, _cx);
                    if (v == null)
                        v = _left?.row[s.defpos] ?? TNull.Value; // allow for outer joins
                }
            }
            if (v == null && _useRight)
            {
                var t = _jrs.second.rowType;
                var s = t.names[n];
                var j = s.seq;
                if (j >= 0)
                {
                    if (_right._rs.qry is SelectQuery qs && j < qs.cols.Count)
                        v = qs.cols[j].Eval(_rs._tr, _cx);
                    if (v == null)
                        v = _right?.row[s.defpos] ?? TNull.Value;
                }
            }
            return v ?? TNull.Value;
        }
        protected abstract JoinBookmark _Next(Context _cx);
        public override RowBookmark Next(Context _cx)
        {
            return _Next(_cx)?.MoveToMatch(_cx);
        }
        internal RowBookmark MoveToMatch(Context _cx)
        {
            var r = this;
            while (r != null && !Query.Eval(_jrs.qry.where,_jrs._tr,_cx))
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
        TRow _row, _key;
        /// <summary>
        /// Constructor: a new Inner Join enumerator
        /// </summary>
        /// <param name="j">The part row set</param>
        InnerJoinBookmark(Context _cx,JoinRowSet j, RowBookmark left, RowBookmark right,int pos=0) 
            : base(_cx,j,left,true,right,true,pos)
        {
            // warning: now check the joinCondition using AdvanceToMatch
            var vs = Value(_cx);
            _row = new TRow(j.rowType, vs);
            _key = new TRow(j.keyType, vs);
        }

        public override TRow row => _row;

        public override TRow key => _key;

        internal static InnerJoinBookmark New(Context _cx,JoinRowSet j)
        {
            var left = j.first.First(_cx);
            var right = j.second.First(_cx);
            if (left == null || right == null)
                return null;
            var join = j.qry as JoinPart;
            for (;;)
            {
                var bm = new InnerJoinBookmark(_cx,j, left, right);
                int c = join.Compare(j._tr,_cx);
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
            var join = _jrs.qry as JoinPart;
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
                int c = join.Compare(_jrs._tr,_cx);
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
            throw new NotImplementedException();
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
        readonly TRow _row, _key;
        /// <summary>
        /// Constructor for the functional-dependent join
        /// </summary>
        /// <param name="rs">The Join RowSet</param>
        FDJoinBookmark(Context _cx,JoinRowSet rs, RowBookmark left, RowBookmark right,int pos) 
            : base(_cx,rs,left,true,right,true,pos)
        {
            join = rs.qry as JoinPart;
            info = join.FDInfo;
            // warning: now check the joinCondition using AdvanceToMatch
            var vs = Value(_cx);
            _row = new TRow(rs.rowType, vs);
            _key = new TRow(rs.keyType, vs);
        }

        public override TRow row => _row;

        public override TRow key => _key;

        /// <summary>
        /// A new FD bookmark
        /// </summary>
        /// <param name="j">the join rowset</param>
        /// <returns>the bookmark</returns>
        internal static FDJoinBookmark New(Context _cx,JoinRowSet j)
        {
            var join = j.qry as JoinPart;
            var info = join.FDInfo;
            RowBookmark left = null, right = null;
            if (info.reverse)
                for (left = j.first.First(_cx); left != null; left = left.Next(_cx))
                {
                    if (j._tr.role.objects[info.rindexDefPos] is Index rx && !(left.Matches() &&
                            Query.Eval(join.left.where, j._tr,_cx)))
                        continue;
                    PRow key = null;
                    for (var b = info.conds.First(); b != null; b = b.Next())
                        if (b.value() is SqlValueExpr se)
                            key = new PRow(se.left.Eval(j._tr, _cx), key);
                    right = j.second.PositionAt(_cx,key.Reverse());
                    if (right?.Matches()==true &&
                        Query.Eval(join.right.where, j._tr, _cx))
                        return new FDJoinBookmark(_cx,j, left, right, 0);
                }
            else
                for (right = j.second.First(_cx); right != null; right = right.Next(_cx))
                {
                    if (j._tr.role.objects[info.rindexDefPos] is Index rx && !(right.Matches() &&
                            Query.Eval(join.right.where, j._tr, _cx)))
                        continue;
                    PRow key = null;
                    for (var b = info.conds.First(); b != null; b = b.Next())
                        if (b.value() is SqlValueExpr se)
                            key = new PRow(se.right.Eval(j._tr, _cx), key);
                    left = j.first.PositionAt(_cx,key.Reverse());
                    if (left?.Matches()==true &&
                        Query.Eval(join.left.where, j._tr, _cx))
                        return new FDJoinBookmark(_cx,j, left, right, 0);
                }
            return null;
        }
        /// <summary>
        /// Move to the next row in a functional-dependent rowset
        /// </summary>
        /// <returns>a bookmark for the next row or null</returns>
        protected override JoinBookmark _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            var join = _jrs.qry as JoinPart;
            var rindex = _jrs._tr.role.objects[info.rindexDefPos] as Index;
            if (info.reverse)
            {
                for (left = left.Next(_cx); left != null; left = left.Next(_cx))
                {
                    if (rindex != null && !(left.Matches() &&
                         Query.Eval(join.left.where, _rs._tr, _cx)))
                        continue;
                    PRow ks = null;
                    for (var b = info.conds.First(); b != null; b = b.Next())
                        if (b.value() is SqlValueExpr c)
                            ks = new PRow(c.left.Eval(_jrs._tr, _cx), ks);
                    right = _jrs.second.PositionAt(_cx,ks.Reverse());
                    if (right?.Matches()==true &&
                        Query.Eval(join.right.where, _rs._tr, _cx))
                        return new FDJoinBookmark(_cx,_jrs, left, right, _pos + 1);
                }
                return null;
            }
            else
            {
                for (right = right.Next(_cx);right != null;right = right.Next(_cx))
                {
                    if (rindex != null && !(right.Matches() &&
                            Query.Eval(join.right.where, _rs._tr, _cx)))
                        continue;
                    PRow ks = null;
                    for (var b = info.conds.First(); b!=null; b=b.Next())
                        if (b.value() is SqlValueExpr c)
                            ks = new PRow(c.right.Eval(_jrs._tr,_cx), ks);
                    left = _jrs.first.PositionAt(_cx,ks.Reverse());
                   if (left?.Matches()==true && 
                        Query.Eval(join.left.where,_rs._tr,_cx))
                    return new FDJoinBookmark(_cx,_jrs, left, right, _pos + 1);
                }
                return null;
            }
        }

        internal override TableRow Rec()
        {
            throw new NotImplementedException();
        }
    }
    /// <summary>
    /// Enumerator for a left join
    /// </summary>
    internal class LeftJoinBookmark : JoinBookmark
    {
        RowBookmark hideRight = null;
        readonly TRow _row, _key;
        /// <summary>
        /// Constructor: a left join enumerator for a join rowset
        /// </summary>
        /// <param name="j">The join rowset</param>
        LeftJoinBookmark(Context _cx,JoinRowSet j, RowBookmark left, RowBookmark right,bool ur,int pos) 
            : base(_cx,j,left,true,right,ur,pos)
        {
            // care: ensure you AdvanceToMatch
            var jp = j.qry as JoinPart;
            hideRight = right;
            var vs = Value(_cx);
            _row = new TRow(j.rowType, vs);
            _key = new TRow(j.keyType, vs);
        }

        public override TRow row => _row;

        public override TRow key => _key;

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
            var join = j.qry as JoinPart;
            for (;;)
            {
                if (right == null)
                    return new LeftJoinBookmark(_cx,j, left, null, false, 0);
                var bm = new LeftJoinBookmark(_cx,j, left, right,true,0);
                int c = join.Compare(j._tr,_cx);
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
            var join = _jrs.qry as JoinPart;
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
                if (mb != null)
                    right = right.ResetToTiesStart(_cx,mb);
                else
                    right = right.Next(_cx);
            }
            for (;;)
            {
                if (right == null)
                    return new LeftJoinBookmark(_cx,_jrs, left, null, false, _pos + 1);
                var ret = new LeftJoinBookmark(_cx,_jrs, left, right, true, _pos + 1);
                int c = join.Compare(_jrs._tr,_cx);
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
            throw new NotImplementedException();
        }
    }
    internal class RightJoinBookmark : JoinBookmark
    {
        RowBookmark hideLeft = null;
        TRow _row, _key;
        /// <summary>
        /// Constructor: a right join enumerator for a join rowset
        /// </summary>
        /// <param name="j">The join rowset</param>
        RightJoinBookmark(Context _cx,JoinRowSet j, RowBookmark left, bool ul, RowBookmark right,int pos) 
            : base(_cx,j,left,ul,right,true,pos)
        {
            // care: ensure you AdvanceToMatch
            var jp = j.qry as JoinPart;
            hideLeft = left;
            var vs = Value(_cx);
            _row = new TRow(j.rowType, vs);
            _key = new TRow(j.keyType, vs);
        }

        public override TRow row => _row;

        public override TRow key => _key;

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
            var join = j.qry as JoinPart;
            for (;;)
            {
                if (left == null)
                    return new RightJoinBookmark(_cx,j, null, false, right, 0);
                var bm = new RightJoinBookmark(_cx,j, left, true, right,0);
                int c = join.Compare(j._tr,_cx);
                if (c == 0)
                    return bm;
                if (c < 0)
                    left = left.Next(_cx);
                else
                    return new RightJoinBookmark(_cx,j, left, false, right, 0);
            }
        }
        /// <summary>
        /// Move to the next row in a left join
        /// </summary>
        /// <returns>whether there is a next row</returns>
        protected override JoinBookmark _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            var join = _jrs.qry as JoinPart;
            if (_useLeft && right.Mb() is MTreeBookmark mr && mr.hasMore((int)join.joinCond.Count))
            {
                right = right.Next(_cx);
                return new RightJoinBookmark(_cx,_jrs, left, true, right, _pos + 1);
            }
            right = right.Next(_cx) as RTreeBookmark;
            // if both left and right have multiple rows for a join key
            // we need to reset the right bookmark to ensure that all 
            // combinations of these matching rows have been used
            if (_useLeft)
            {
                var mb = (left.Mb() is MTreeBookmark ml && ml.changed((int)join.joinCond.Count)) ? null :
                    left.Mb()?.ResetToTiesStart((int)join.joinCond.Count);
                if (mb != null)
                    right = right.ResetToTiesStart(_cx,mb);
                else
                    right = right.Next(_cx);
            }
            for (;;)
            {
                if (left == null)
                    return new RightJoinBookmark(_cx,_jrs, null, false, right, _pos + 1);
                var ret = new RightJoinBookmark(_cx,_jrs, left, true, right, _pos + 1);
                int c = join.Compare(_jrs._tr,_cx);
                if (c == 0)
                    return ret;
                if (c < 0)
                    left = left.Next(_cx) as RTreeBookmark;
                else
                    return new RightJoinBookmark(_cx,_jrs, left, false, right, _pos + 1);
            }
        }

        internal override TableRow Rec()
        {
            throw new NotImplementedException();
        }
    }
    /// <summary>
    /// A full join bookmark for a join row set
    /// </summary>
    internal class FullJoinBookmark : JoinBookmark
    {
        RowBookmark hideLeft = null, hideRight = null;
        TRow _row, _key;
        /// <summary>
        /// Constructor: a full join bookmark for a join rowset
        /// </summary>
        /// <param name="j">The join rowset</param>
        FullJoinBookmark(Context _cx,JoinRowSet j, RowBookmark left, bool ul, RowBookmark right, 
            bool ur, int pos)
            : base(_cx,j, left, ul, right, ur, pos)
        {
            // care: ensure you AdvanceToMatch
            var jp = j.qry as JoinPart;
            hideLeft = left;
            hideRight = right;
            var vs = Value(_cx);
            _row = new TRow(j.rowType, vs);
            _key = new TRow(j.keyType, vs);
        }

        public override TRow row => _row;

        public override TRow key => _key;

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
            var join = j.qry as JoinPart;
            var bm = new FullJoinBookmark(_cx,j, left, true, right, true, 0);
            int c = join.Compare(j._tr,_cx);
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
            var join = _jrs.qry as JoinPart;
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
            int c = join.Compare(_jrs._tr,_cx);
            return new FullJoinBookmark(_cx,_jrs, left, c <= 0, right, c >= 0, _pos + 1);
        }

        internal override TableRow Rec()
        {
            throw new NotImplementedException();
        }
    }
    /// <summary>
    /// A cross join bookmark for a join row set
    /// </summary>
    internal class CrossJoinBookmark : JoinBookmark
    {
        TRow _row, _key;
        /// <summary>
        /// Constructor: a cross join bookmark for a join row set
        /// </summary>
        /// <param name="j">a join row set</param>
        CrossJoinBookmark(Context _cx,JoinRowSet j, RowBookmark left = null, RowBookmark right = null,
            int pos=0) : base(_cx,j,left,true,right,true,pos)
        {
            var vs = Value(_cx);
            _row = new TRow(j.rowType, vs);
            _key = new TRow(j.keyType, vs);
        }

        public override TRow row => _row;

        public override TRow key => _key;

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

