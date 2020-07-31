using Pyrrho.Common;
using Pyrrho.Level3;
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
    /// A MergeRowSet is for handling UNION, INTERSECT, EXCEPT
    /// </summary>
    internal class MergeRowSet : RowSet
    {
        /// <summary>
        /// The first operand of the merge operation
        /// </summary>
        internal long left;
        /// <summary>
        /// The second operand of the merge operation
        /// </summary>
        internal long right;
        /// <summary>
        /// whether the enumeration is of the left operand (if false we are in the right operand)
        /// </summary>
        internal bool useLeft = true;
        /// <summary>
        /// UNION/INTERSECT/EXCEPT
        /// </summary>
        internal Sqlx oper;
        /// <summary>
        /// whether DISTINCT has been specified
        /// </summary>
        internal bool distinct = true;
        /// <summary>
        /// Constructor: a merge rowset from two queries, whose rowsets have been constructed
        /// </summary>
        /// <param name="a">the left operand</param>
        /// <param name="b">the right operand</param>
        /// <param name="q">true if DISTINCT specified</param>
        internal MergeRowSet(Context cx, Query q, RowSet a,RowSet b, bool d, Sqlx op)
            : base(q.defpos,cx,a.dataType,q.display,a.finder,null,q.where,q.ordSpec,q.matches,
                  q.matching)
        {
            distinct = d;
            oper = op;
            left = a.defpos;
            right = b.defpos; 
            if (q.where.Count==0 && oper!=Sqlx.UNION)
                Build(q.defpos,cx,a.dataType);
        }
        protected MergeRowSet(MergeRowSet rs, long a, long b) : base(rs, a, b)
        {
            distinct = rs.distinct;
            oper = rs.oper;
            left = rs.left;
            right = rs.right;
        }
        protected MergeRowSet(Context cx, MergeRowSet rs, BTree<long, Finder> nd, bool bt) 
        :base(cx,rs,nd,bt)
        { }
        internal override RowSet New(long a, long b)
        {
            return new MergeRowSet(this, a, b);
        }
        internal override RowSet New(Context cx,BTree<long,Finder> nd,bool bt)
        {
            return new MergeRowSet(cx, this, nd, bt);
        }
        internal override BTree<long, Finder> AllWheres(Context cx,BTree<long,Finder>nd)
        {
            nd = cx.Needs(nd,this,where);
            nd = cx.Needs(nd,this,cx.data[left].AllWheres(cx,nd));
            nd = cx.Needs(nd,this,cx.data[right].AllWheres(cx,nd));
            return nd;
        }
        internal override BTree<long, Finder> AllMatches(Context cx,BTree<long,Finder>nd)
        {
            nd = cx.Needs(nd, this, matches);
            nd = cx.data[left].AllMatches(cx,nd);
            nd = cx.data[right].AllMatches(cx,nd);
            return nd;
        }
        internal override bool Knows(Context cx, long rp)
        {
            return rp==left || rp==right || base.Knows(cx,rp);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Merge ");
            base._Strategy(sb, indent);
        }
        protected override Cursor _First(Context cx)
        {
            switch (oper)
            {
                case Sqlx.UNION:    return UnionBookmark.New(cx,this,0,
                    cx.data[left].First(cx),cx.data[right].First(cx));
                case Sqlx.INTERSECT: return IntersectBookmark.New(cx,this);
                case Sqlx.EXCEPT:   return ExceptBookmark.New(cx,this);
            }
            throw new PEException("PE899");
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Left: "); sb.Append(DBObject.Uid(left));
            sb.Append(" Right: "); sb.Append(DBObject.Uid(right));
            return sb.ToString();
        }
    }
    /// <summary>
    /// An enumerator for a mergerowset
    /// A class for shared MergeEnumerator machinery. Supports IntersectionEnumerator, ExceptEnumerator and UnionEnumerator
    /// </summary>
    internal abstract class MergeBookmark : Cursor
    {
        /// <summary>
        /// The associated merge rowset
        /// </summary>
        internal readonly MergeRowSet rowSet;
        internal readonly Cursor _left, _right;
        internal readonly bool _useLeft;
        internal override TableRow Rec()
        {
            return _useLeft ? _left.Rec() : _right.Rec();
        }
        /// <summary>
        /// Constructor: a merge enumerator for a mergerowset
        /// </summary>
        /// <param name="r">the rowset</param>
        internal MergeBookmark(Context _cx, MergeRowSet r,int pos,Cursor left=null,
            Cursor right=null,bool ul=false)
            :base(_cx,r,pos,0,ul?left:right)
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
        protected static int _compare(RowSet r, Cursor left, Cursor right)
        {
            if (left == null)
                return -1;
            if (right == null)
                return 1;
            var dt = r.rt;
            for (var i=0;i<dt.Length;i++)
            {
                var n = dt[i];
                var c = left[n].CompareTo(right[n]);
                if (c != 0)
                    return c;
            }
            return 0;
        }
    }
    /// <summary>
    /// A Union enumerator for merge rowset
    /// </summary>
    internal class UnionBookmark : MergeBookmark
    {
        /// <summary>
        /// Constructor: a bookmark for the merge rowset
        /// </summary>
        /// <param name="r">the merge rowset</param>
        UnionBookmark(Context _cx, MergeRowSet r,int pos,Cursor left,Cursor right,bool ul) : 
            base(_cx,r,pos,left,right,ul)
        {
        }
        UnionBookmark(UnionBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
        { }
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new UnionBookmark(this, cx, p, v);
        }
        internal static UnionBookmark New(Context _cx, MergeRowSet r, int pos = 0, 
            Cursor left = null,Cursor right=null)
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
        protected override Cursor _Next(Context _cx)
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
    }
    /// <summary>
    /// An except enumerator for the merge rowset
    /// </summary>
    internal class ExceptBookmark : MergeBookmark
    {
        /// <summary>
        /// Constructor: an except enumerator for the merge rowset
        /// </summary>
        /// <param name="r">the merge rowset</param>
        ExceptBookmark(Context _cx, MergeRowSet r,int pos=0,Cursor left=null,Cursor right=null)
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
        static void MoveToNonMatch(Context _cx, MergeRowSet r, ref Cursor left,ref Cursor right)
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
        internal static ExceptBookmark New(Context cx, MergeRowSet r)
        {
            var left = cx.data[r.left].First(cx);
            var right = cx.data[r.right].First(cx);
            MoveToNonMatch(cx,r, ref left, ref right);
            if (left == null)
                return null;
            return new ExceptBookmark(cx,r, 0, left, right);
        }
        /// <summary>
        /// Move to the next row of the except rowset
        /// </summary>
        /// <returns>whether there is a next row</returns>
        protected override Cursor _Next(Context _cx)
        {
            var left = _left.Next(_cx);
            var right = _right;
            MoveToNonMatch(_cx,rowSet, ref left, ref right);
            if (left == null)
                return null;
            return new ExceptBookmark(_cx,rowSet, _pos + 1, left, right);
        }
    }
    /// <summary>
    /// An intersect enumerator for the merge row set
    /// </summary>
    internal class IntersectBookmark : MergeBookmark
    {
        /// <summary>
        /// Constructor: an intersect enumerator for the merge rowset
        /// </summary>
        /// <param name="r">the merge rowset</param>
        IntersectBookmark(Context _cx, MergeRowSet r,int pos=0,Cursor left=null,Cursor right=null)
            : base(_cx,r,pos,left,right)
        { }
        IntersectBookmark(IntersectBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v) 
        { }
        static void MoveToMatch(Context _cx, MergeRowSet r, ref Cursor left, ref Cursor right)
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
        protected override Cursor New(Context cx, long p, TypedValue v)
        {
            return new IntersectBookmark(this, cx, p, v);
        }
        internal static IntersectBookmark New(Context cx, MergeRowSet r)
        {
            var left = cx.data[r.left].First(cx);
            var right = cx.data[r.right].First(cx);
            MoveToMatch(cx,r, ref left, ref right);
            if (left == null)
                return null;
            return new IntersectBookmark(cx,r, 0, left, right);
        }
        /// <summary>
        /// Move to the next row of the intersect rowset
        /// </summary>
        /// <returns>whether there is a next row</returns>
        protected override Cursor _Next(Context _cx)
        {
            var left = _left.Next(_cx);
            var right = _right;
            MoveToMatch(_cx,rowSet, ref left, ref right);
            if (left == null)
                return null;
            return new IntersectBookmark(_cx,rowSet, _pos + 1, left, right);
        }
    }
}

