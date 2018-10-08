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
    public class SSearchTree<T> where T : System.IComparable
    {
        public readonly T node;
        public readonly SSearchTree<T> left, right;
        public SSearchTree(T n,SSearchTree<T> lf,SSearchTree<T> rg)
        {
            node = n;
            left = lf;
            right = rg;
        }
        public static SSearchTree<T> New(T[] a)
        {
            if (a.Length == 0)
                return null;
            var r = new SSearchTree<T>(a[0], null, null);
            for (var i = 1; i < a.Length; i++)
                r = r.Add(a[i]);
            return r;
        }
        /// <summary>
        /// Note that the first entry in the tree will be made by the constructor.
        /// </summary>
        /// <param name="n"></param>
        /// <returns></returns>
        public SSearchTree<T> Add(T n)
        {
            var c = n.CompareTo(node);
            if (c <= 0)
                return new SSearchTree<T>(node, left?.Add(n) ?? new SSearchTree<T>(n, null, null),right);
            else
                return new SSearchTree<T>(node, left, right?.Add(n) ?? new SSearchTree<T>(n, null, null));
        }
        public bool Contains(T n)
        {
            var c = n.CompareTo(node);
            return (c == 0) ? true : (c < 0) ? (left?.Contains(n) ?? false) : (right?.Contains(n) ?? false);
        }
        public int count
        {
            get { return 1 + (left?.count ?? 0) + (right?.count ?? 0); }
        }
        void Traverse(T[] a,ref int i)
        {
            left?.Traverse(a, ref i);
            a[i++] = node;
            right?.Traverse(a, ref i);
        }
        public T[] ToArray()
        {
            var r = new T[count];
            int i = 0;
            Traverse(r, ref i);
            return r;
        }
    }
}
