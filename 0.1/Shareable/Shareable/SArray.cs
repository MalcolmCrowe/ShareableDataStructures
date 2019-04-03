/// <summary>
/// See "Collection Data Structures" (c) Malcolm Crowe, University of the West of Scotland 2018
/// http://shareabledata.org 
/// This is free-to-use software
/// </summary>
namespace Shareable
{
    public class SArray<T> : Collection<T>
    {
        public readonly T[] elements;
        public SArray(params T[] els) :base(els.Length)
        { 
            elements = new T[els.Length];
            for (var i = 0; i <els.Length; i++)
                elements[i] = els[i];
        }
        public SArray<T> InsertAt(int n,params T[] els)
        {
            var x = new T[elements.Length + els.Length];
            for (int i = 0; i < n; i++)
                x[i] = elements[i];
            for (int i = 0; i < els.Length; i++)
                x[i + n] = els[i];
            for (int i = n; i < elements.Length; i++)
                x[i + els.Length] = elements[i];
            return new SArray<T>(x);
        }
        public SArray<T> RemoveAt(int n)
        {
            var x = new T[elements.Length - 1];
            for (int i = 0; i < n; i++)
                x[i] = elements[i];
            for (int i = n+1; i < elements.Length; i++)
                x[i - 1] = elements[i];
            return new SArray<T>(x);
        }
        public SArray<T> UpdateAt(T x, int n)
        {
            var a = new T[elements.Length];
            for (int i = 0; i < n; i++)
               a[i] = elements[i];
            a[n] = x;
            for (int i = n+1; i < elements.Length; i++)
                a[i] = elements[i];
            return new SArray<T>(a);
        }
        public override Bookmark<T>? First() // ok
        {
            return (Length==0)? null : new SArrayBookmark<T>(this,0);
        }
    }
    public class SArrayBookmark<T> : Bookmark<T>
    {
        internal readonly SArray<T> _a;
        internal SArrayBookmark(SArray<T> a, int p) : base(p) { _a = a; }
        public override Bookmark<T>? Next() // ok
        {
            return (Position+1 >= _a.elements.Length) ? null 
                : new SArrayBookmark<T>(_a, Position+1);
        }
        public override T Value => _a.elements[Position];
    }
}
