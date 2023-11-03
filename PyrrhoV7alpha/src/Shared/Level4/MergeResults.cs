using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level4
{
    /// <summary>
    /// A MergeRowSet is for handling UNION, INTERSECT, EXCEPT
    ///     /
    /// </summary>
    internal class MergeRowSet : RowSet
    {
        internal const long
            _Left = -244, // long RowSet
            _Right = -246; // long RowSet
        /// <summary>
        /// The first operand of the merge operation
        /// </summary>
        internal long left => (long)(mem[_Left]??-1L);
        /// <summary>
        /// The second operand of the merge operation
        /// </summary>
        internal long right => (long)(mem[_Right]??-1L);
        /// <summary>
        /// UNION/INTERSECT/EXCEPT
        /// </summary>
        internal Sqlx oper => (Sqlx)(mem[Domain.Kind]??Sqlx.NONE);
        /// <summary>
        /// Constructor: a merge rowset from two queries, whose rowsets have been constructed
        /// </summary>
        /// <param name="a">the left operand</param>
        /// <param name="b">the right operand</param>
        /// <param name="q">true if DISTINCT specified</param>
        internal MergeRowSet(long dp,Context cx, Domain q, RowSet a,RowSet b, bool d, Sqlx op)
            : base(dp,cx,_Mem(q,a,b)+(Distinct,d)+(Domain.Kind,op)
                  +(_Left,a.defpos)+(_Right,b.defpos))
        {
            cx.Add(this);
        }
        protected MergeRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Domain dm,RowSet a,RowSet b)
        {
            var m = BTree<long, object>.Empty + (_Domain, dm);
            var la = a.lastData;
            var lb = b.lastData;
            if (la != 0L && lb != 0L)
                m += (Table.LastData, Math.Max(la, lb));
            return m;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new MergeRowSet(defpos, m);
        }
        public static MergeRowSet operator+(MergeRowSet rs,(long,object)x)
        {
            var (dp, ob) = x;
            if (rs.mem[dp] == ob)
                return rs;
            return (MergeRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new MergeRowSet(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nl = cx.Fix(left);
            if (nl!=left)
            r += (_Left, nl);
            var nr = cx.Fix(right);
            if (nr!=right)
            r += (_Right, nr);
            return r;
        }
        internal override bool Knows(Context cx, long rp, bool ambient=false)
        {
            return rp==left || rp==right || base.Knows(cx,rp,ambient);
        }
        internal override CTree<long, bool> Sources(Context cx)
        {
            return new CTree<long, bool>(left, true) + (right, true);
        }
        internal override BTree<long, RowSet> AggSources(Context cx)
        {
            var l = left;
            var r = right;
            return new BTree<long, RowSet>(l,(RowSet?)cx.obs[l]??throw new PEException("PE2200")) 
                + (r, (RowSet?)cx.obs[r] ?? throw new PEException("PE2201"));
        }
        protected override Cursor? _First(Context cx)
        {
            return oper switch
            {
                Sqlx.UNION => UnionBookmark.New(cx, this, 0,
                                    ((RowSet?)cx.obs[left])?.First(cx), ((RowSet?)cx.obs[right])?.First(cx)),
                Sqlx.INTERSECT => IntersectBookmark.New(cx, this),
                Sqlx.EXCEPT => ExceptBookmark.New(cx, this),
                _ => throw new PEException("PE899"),
            };
        }
        protected override Cursor? _Last(Context cx)
        {
            return oper switch
            {
                Sqlx.UNION => UnionBookmark.New(cx, this, 0,
                                        ((RowSet?)cx.obs[left])?.Last(cx), ((RowSet?)cx.obs[right])?.Last(cx)),
                Sqlx.INTERSECT => IntersectBookmark.New(this, cx),
                Sqlx.EXCEPT => ExceptBookmark.New(this, cx),
                _ => throw new PEException("PE899"),
            };
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Left: "); sb.Append(Uid(left));
            sb.Append(" Right: "); sb.Append(Uid(right));
            return sb.ToString();
        }
    }
    /// <summary>
    /// An enumerator for a mergerowset
    /// A class for shared MergeEnumerator machinery. Supports IntersectionEnumerator, ExceptEnumerator and UnionEnumerator
    ///     /
    /// </summary>
    internal abstract class MergeBookmark : Cursor
    {
        /// <summary>
        /// The associated merge rowset
        /// </summary>
        internal readonly MergeRowSet rowSet;
        internal readonly Cursor? _left, _right;
        internal readonly bool _useLeft;
        internal override BList<TableRow>? Rec()
        {
            return _useLeft ? _left?.Rec() : _right?.Rec();
        }
        /// <summary>
        /// Constructor: a merge enumerator for a mergerowset
        /// </summary>
        /// <param name="r">the rowset</param>
        protected MergeBookmark(Context _cx, MergeRowSet r,int pos,Cursor? left=null,
            Cursor? right=null,bool ul=false) :base(_cx,r,pos,
                 (left?._ds??CTree<long,(long,long)>.Empty) +(right?._ds??CTree<long,(long,long)>.Empty),
                 (ul?left:right)??Empty)
        {
            rowSet = r;
            _left = left; _right = right;
            _useLeft = ul;
        }
        protected MergeBookmark(MergeBookmark cu,Context cx,long p,TypedValue v):base(cu,cx,p,v)
        {
            rowSet = cu.rowSet;
            _left = cu._left; 
            _right = cu._right;
            _useLeft = cu._useLeft;
        }
        protected MergeBookmark(Context cx, MergeBookmark cu) : base(cx, cu)
        {
            rowSet = (MergeRowSet)(cx.obs[cx.Fix(cu._rowsetpos)] ?? throw new PEException("PE2300"));
            rowSet = (MergeRowSet)(rowSet?.Fix(cx) ?? throw new PEException("PE2301"));
            _left = (Cursor?)cu._left?.Fix(cx);
            _right = (Cursor?)cu._right?.Fix(cx);
            _useLeft = cu._useLeft;
        }
        protected static int _compare(RowSet r, Cursor? left, Cursor? right)
        {
            if (left == null)
                return -1;
            if (right == null)
                return 1;
            var dt = r.rowType;
            for (var i = 0; i < dt.Length; i++)
                if (dt[i] is long n)
                {
                    var c = left[n].CompareTo(right[n]);
                    if (c != 0)
                        return c;
                }
            return 0;
        }
    }
    /// <summary>
    /// A Union enumerator for merge rowset
    ///     /
    /// </summary>
    internal class UnionBookmark : MergeBookmark
    {
        /// <summary>
        /// Constructor: a bookmark for the merge rowset
        /// </summary>
        /// <param name="r">the merge rowset</param>
        UnionBookmark(Context _cx, MergeRowSet r,int pos,Cursor? left,Cursor? right,bool ul) : 
            base(_cx,r,pos,left,right,ul)
        {
        }
        UnionBookmark(UnionBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
        { }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new UnionBookmark(this, cx, p, v);
        }
        internal static UnionBookmark? New(Context _cx, MergeRowSet r, int pos = 0, 
            Cursor? left = null,Cursor? right=null)
        {
            if (left == null && right == null)
                return null;
            return new UnionBookmark(_cx,r, pos, left, right, right==null 
                || _compare(r,left,right)<0);
        }
        /// <summary>
        /// Move to the next row in the union
        /// </summary>
        /// <returns>whether there is a next row</returns>
        protected override Cursor? _Next(Context _cx)
        {
            var left = _left;
            var right = _right;
            // Next on left if we've just used it
            // Next on right if we've just used it OR there is a match && distinct
            if (right != null && ((!_useLeft) || (rowSet.distinct 
                && _compare(rowSet, left, right) == 0)))
                right = right.Next(_cx);
            if (left != null && _useLeft)
                left = left.Next(_cx);
            return New(_cx,rowSet, _pos + 1, left, right);
        }
        protected override Cursor? _Previous(Context _cx)
        {
            var left = _left;
            var right = _right;
            // Next on left if we've just used it
            // Next on right if we've just used it OR there is a match && distinct
            if (right != null && ((!_useLeft) || (rowSet.distinct
                && _compare(rowSet, left, right) == 0)))
                right = right.Previous(_cx);
            if (left != null && _useLeft)
                left = left.Previous(_cx);
            return New(_cx, rowSet, _pos + 1, left, right);
        }
    }
    /// <summary>
    /// An except enumerator for the merge rowset
    ///     /
    /// </summary>
    internal class ExceptBookmark : MergeBookmark
    {
        /// <summary>
        /// Constructor: an except enumerator for the merge rowset
        /// </summary>
        /// <param name="r">the merge rowset</param>
        ExceptBookmark(Context _cx, MergeRowSet r,int pos=0,Cursor? left=null,Cursor? right=null)
            : base(_cx,r,pos,left,right,true)
        {
            // we assume MovetoNonMatch has been done
            _cx.values += (_rowsetpos, this);
        }
        ExceptBookmark(ExceptBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
        {
            cx.values += (_rowsetpos, this);
        }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new ExceptBookmark(this, cx, p, v);
        }
        static void MoveToNonMatch(Context _cx, MergeRowSet r, ref Cursor? left,ref Cursor? right)
        {
            for (;;)
            {
                if (left == null || right == null)
                    break;
                var c = _compare(r, left, right);
                if (c == 0)
                    left = left.Next(_cx);
                else if (c > 0)
                    right = right.Next(_cx);
            }
        }
        static void PrevToNonMatch(Context _cx, MergeRowSet r, ref Cursor? left, ref Cursor? right)
        {
            for (; ; )
            {
                if (left == null || right == null)
                    break;
                var c = _compare(r, left, right);
                if (c == 0)
                    left = left.Previous(_cx);
                else if (c < 0)
                    right = right.Previous(_cx);
            }
        }
        internal static ExceptBookmark? New(Context cx, MergeRowSet r)
        {
            var left = ((RowSet?)cx.obs[r.left])?.First(cx);
            var right = ((RowSet?)cx.obs[r.right])?.First(cx);
            MoveToNonMatch(cx,r, ref left, ref right);
            if (left == null)
                return null;
            return new ExceptBookmark(cx,r, 0, left, right);
        }
        internal static ExceptBookmark? New(MergeRowSet r, Context cx)
        {
            var left = ((RowSet?)cx.obs[r.left])?.Last(cx);
            var right = ((RowSet?)cx.obs[r.right])?.Last(cx);
            MoveToNonMatch(cx, r, ref left, ref right);
            if (left == null)
                return null;
            return new ExceptBookmark(cx, r, 0, left, right);
        }
        /// <summary>
        /// Move to the next row of the except rowset
        /// </summary>
        /// <returns>whether there is a next row</returns>
        protected override Cursor? _Next(Context _cx)
        {
            var left = _left?.Next(_cx);
            var right = _right;
            MoveToNonMatch(_cx,rowSet, ref left, ref right);
            if (left == null)
                return null;
            return new ExceptBookmark(_cx,rowSet, _pos + 1, left, right);
        }
        protected override Cursor? _Previous(Context _cx)
        {
            var left = _left?.Previous(_cx);
            var right = _right;
            PrevToNonMatch(_cx, rowSet, ref left, ref right);
            if (left == null)
                return null;
            return new ExceptBookmark(_cx, rowSet, _pos + 1, left, right);
        }
    }
    /// <summary>
    /// An intersect enumerator for the merge row set
    ///     /
    /// </summary>
    internal class IntersectBookmark : MergeBookmark
    {
        /// <summary>
        /// Constructor: an intersect enumerator for the merge rowset
        /// </summary>
        /// <param name="r">the merge rowset</param>
        IntersectBookmark(Context _cx, MergeRowSet r,int pos=0,Cursor? left=null,Cursor? right=null)
            : base(_cx,r,pos,left,right)
        { }
        IntersectBookmark(IntersectBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v) 
        { }
        static void MoveToMatch(Context _cx, MergeRowSet r, ref Cursor? left, ref Cursor? right)
        {
            for (;;)
            {
                if (left == null || right == null)
                    break;
                var c = _compare(r, left, right);
                if (c < 0)
                    left = left.Next(_cx);
                else if (c > 0)
                    right = right.Next(_cx);
            }
        }
        static void PrevToMatch(Context _cx, MergeRowSet r, ref Cursor? left, ref Cursor? right)
        {
            for (; ; )
            {
                if (left == null || right == null)
                    break;
                var c = _compare(r, left, right);
                if (c > 0)
                    left = left.Previous(_cx);
                else if (c < 0)
                    right = right.Previous(_cx);
            }
        }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new IntersectBookmark(this, cx, p, v);
        }
        internal static IntersectBookmark? New(Context cx, MergeRowSet r)
        {
            var left = ((RowSet?)cx.obs[r.left])?.First(cx);
            var right = ((RowSet?)cx.obs[r.right])?.First(cx);
            MoveToMatch(cx,r, ref left, ref right);
            if (left == null)
                return null;
            return new IntersectBookmark(cx,r, 0, left, right);
        }
        internal static IntersectBookmark? New(MergeRowSet r, Context cx)
        {
            var left = ((RowSet?)cx.obs[r.left])?.Last(cx);
            var right = ((RowSet?)cx.obs[r.right])?.Last(cx);
            PrevToMatch(cx, r, ref left, ref right);
            if (left == null)
                return null;
            return new IntersectBookmark(cx, r, 0, left, right);
        }
        /// <summary>
        /// Move to the next row of the intersect rowset
        /// </summary>
        /// <returns>whether there is a next row</returns>
        protected override Cursor? _Next(Context _cx)
        {
            var left = _left?.Next(_cx);
            var right = _right;
            MoveToMatch(_cx,rowSet, ref left, ref right);
            if (left == null)
                return null;
            return new IntersectBookmark(_cx,rowSet, _pos + 1, left, right);
        }
        protected override Cursor? _Previous(Context _cx)
        {
            var left = _left?.Previous(_cx);
            var right = _right;
            PrevToMatch(_cx, rowSet, ref left, ref right);
            if (left == null)
                return null;
            return new IntersectBookmark(_cx, rowSet, _pos + 1, left, right);
        }
    }
}

