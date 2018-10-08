/// <summary>
/// See "Shareable Data Structures" (c) Malcolm Crowe, University of the West of Scotland 2018
/// http://shareabledata.org 
/// This is free-to-use software
/// </summary>
namespace Shareable
{
    public class SArray<T> : Shareable<T>
    {
        public readonly T[] elements;
        public SArray(T[] els)
        { 
            elements = new T[els.Length];
            for (var i = 0; i <els.Length; i++)
                elements[i] = els[i];
        }
        public int Length
        {
            get { return elements.Length; }
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
            for (int i = n; i < elements.Length; i++)
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
        public T[] ToArray()
        {
            // DO NOT simply "return elements"! Copy the array first
            var r = new T[elements.Length];
            for (var b = First(); b != null; b = b.Next())
                r[b.Position()] = b.Value();
            return r;
        }

        public Bookmark<T> First()
        {
            return SArrayBookmark<T>.New(this,0);
        }
    }
    public class SArrayBookmark<T> : Bookmark<T>
    {
        public readonly SArray<T> _a;
        public int _pos;
        SArrayBookmark(SArray<T> a,int p)
        {
            _a = a; _pos = p;
        }
        public static SArrayBookmark<T> New(SArray<T> a,int p)
        {
            return (a == null || p >= a.elements.Length)? null 
                : new SArrayBookmark<T>(a, p);
        }
        public Bookmark<T> Next()
        {
            return (_pos+1 >= _a.elements.Length) ? null 
                : new SArrayBookmark<T>(_a, _pos+1);
        }
        public T Value()
        {
            return _a.elements[_pos];
        }
        public int Position()
        {
            return _pos;
        }
    }
}
