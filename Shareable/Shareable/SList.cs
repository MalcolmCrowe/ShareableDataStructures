/// <summary>
/// See "Shareable Data Structures" (c) Malcolm Crowe, University of the West of Scotland 2018
/// http://shareabledata.org 
/// This is free-to-use software
/// </summary>
namespace Shareable
{
    public class SList<T> : Shareable<T>
    {
        public readonly T element;
        public readonly SList<T> next;
        public SList(T e,SList<T> n)
        {
            element = e;
            next = n;
        }
        public static SList<T> New(params T[] els)
        {
            SList<T> r = null;
            for (var i = els.Length - 1; i >= 0; i--)
                r = new SList<T>(els[i], r);
            return r;
        }
        public int Length
        {
            get { return next?.Length ?? 0 + 1; }
        }
        /// <summary>
        /// Note that the first entry in the list must be made by the constructor.
        /// </summary>
        /// <param name="x">The new node</param>
        /// <param name="n">The position in the list (n>=0, Length>n)</param>
        /// <returns>the new list</returns>
        public SList<T> InsertAt(T x, int n)
        {
            if (n == 0)
                return new SList<T>(x, this);
            // if (n<0 || next==null) throw Exception("");
            return new SList<T>(element, next.InsertAt(x, n - 1));
        }
        public SList<T> RemoveAt(int n)
        {
            if (n == 0)
                return next;
            // if (n<0 || next==null) throw Exception("");
            return new SList<T>(element, next.RemoveAt(n - 1));
        }
        public SList<T> UpdateAt(T x,int n)
        {
            if (n == 0)
                return new SList<T>(x, next);
            // if (n<0 || next==null) throw Exception("");
            return new SList<T>(element, next.UpdateAt(x, n - 1));
        }
        public T[] ToArray()
        {
            var r = new T[Length];
            var i = 0;
            for (var b=First();b!=null;b=b.Next())
                r[b.Position()] = b.Value();
            return r;
        }

        public Bookmark<T> First()
        {
            return SListBookmark<T>.New(this);
        }
    }
    public class SListBookmark<T> : Bookmark<T>
    {
        public readonly SList<T> _s;
        public readonly int _pos;
        SListBookmark(SList<T> s,int p)
        {
            _s = s; _pos = p;
        }
        public static SListBookmark<T> New(SList<T> s)
        {
            if (s == null)
                return null;
            return new SListBookmark<T>(s, 0);
        }

        public Bookmark<T> Next()
        {
            if (_s.next==null)
                return null;
            return new SListBookmark<T>(_s.next, _pos + 1);
        }

        public int Position()
        {
            return _pos;
        }

        public T Value()
        {
            return _s.element;
        }
    }
}
