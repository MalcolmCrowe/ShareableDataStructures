#nullable enable
/// <summary>
/// See "Shareable Data Structures" (c) Malcolm Crowe, University of the West of Scotland 2018
/// http://shareabledata.org 
/// This is free-to-use software 
/// </summary>
namespace Shareable
{
    /// <summary>
    /// Implementation of an UNBALANCED binary search tree
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SSearchTree<T> :Shareable<T> where T : System.IComparable
    {
        public readonly T node;
        public readonly SSearchTree<T>? left, right;
        public static readonly SSearchTree<T> Empty = new SSearchTree<T>();
        SSearchTree():base(0) { node = default(T); left = null; right = null; } 
        internal SSearchTree(T n,SSearchTree<T>? lf,SSearchTree<T>? rg)
            : base(1 + lf?.Length??0 + rg?.Length??0)
        {
            node = n;
            left = lf;
            right = rg;
        }
        public static SSearchTree<T> New(params T[] els)
        {
            var r = Empty;
            foreach (var t in els)
                r = r.Add(t);
            return r;
        }
        public SSearchTree<T> Add(T n)
        {
            if (this == Empty)
                return new SSearchTree<T>(n, Empty, Empty);
            var c = n.CompareTo(node);
            if (c <= 0)
                return new SSearchTree<T>(node, left?.Add(n) ?? new SSearchTree<T>(n, Empty, Empty),right);
            else
                return new SSearchTree<T>(node, left, right?.Add(n) ?? new SSearchTree<T>(n, Empty,Empty));
        }
        public bool Contains(T n)
        {
            if (this == Empty)
                return false;
            var c = n.CompareTo(node);
            return (c == 0) ? true : (c < 0) ? (left?.Contains(n) ?? false) : (right?.Contains(n) ?? false);
        }
        public override Bookmark<T>? First()
        {
            return (this == Empty) ? null : new SSearchTreeBookmark<T>(this, true);
        }
    }
    public class SSearchTreeBookmark<T> : Bookmark<T> where T : System.IComparable
    {
        internal readonly SSearchTree<T> _s;
        internal readonly SList<SSearchTree<T>>? _stk;
        internal SSearchTreeBookmark(SSearchTree<T> s, bool doLeft,
            SList<SSearchTree<T>>? stk = null,
            int p = 0) : base(p)
        {
            if (stk == null) stk = SList<SSearchTree<T>>.Empty;
            for (; doLeft && s.left != SSearchTree<T>.Empty; 
                s = s.left ?? throw new System.Exception("??")) 
                stk = stk.InsertAt(s, 0);
            _s = s; _stk = stk;
        }
        public override T Value => _s.node;
        public override Bookmark<T>? Next()
        {
            return (_s.right != SSearchTree<T>.Empty) ?
                new SSearchTreeBookmark<T>(_s.right ?? throw new System.Exception("??"), true, _stk, Position + 1) // ok
                : (_stk == SList<SSearchTree<T>>.Empty) ? null
                : new SSearchTreeBookmark<T>((_stk?.First() ?? throw new System.Exception("??")).Value, false, //ok
            (_stk ?? throw new System.Exception("??")).RemoveAt(0), Position + 1);
        }
    }

}
