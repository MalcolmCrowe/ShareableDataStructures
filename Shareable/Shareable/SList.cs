/// <summary>
/// See "Shareable Data Structures" (c) Malcolm Crowe, University of the West of Scotland 2018
/// See github.com/MalcolmCrowe/ShareableDataStructures
/// This is free-to-use software
/// </summary>
namespace Shareable
{
    public class SList<T> : Shareable<T>
    {
        internal readonly T element;
        internal readonly SList<T> next;
        public static readonly SList<T> Empty = new SList<T>();
        SList() { element = default(T); next = null; }
        internal SList(T e,SList<T> n) : base(n.Length+1)
        {
            element = e;
            next = n;
        }
        public static SList<T> New(params T[] els) 
        {
            var r = Empty;
            for (var i = els.Length - 1; i >= 0; i--)
                r = new SList<T>(els[i], r);
            return r;
        }
        public SList<T> InsertAt(T x, int n) // n>=0
        {
            if (this==Empty || n==0)
                return new SList<T>(x, this);
            return new SList<T>(element, next.InsertAt(x, n - 1));
        }
        public SList<T> RemoveAt(int n)
        {
            if (this == Empty)
                return Empty;
            if (n == 0)
                return next;
            return new SList<T>(element, next.RemoveAt(n - 1));
        }
        public SList<T> UpdateAt(T x,int n)
        {
            if (n == 0)
                return new SList<T>(x, next);
            return new SList<T>(element, next.UpdateAt(x, n - 1));
        }
        public override Bookmark<T> First()
        {
            return (this==Empty)?null:new SListBookmark<T>(this);
        }
    }
    public class SListBookmark<T> : Bookmark<T>
    {
        internal readonly SList<T> _s;
        internal SListBookmark(SList<T> s, int p = 0) :base(p) { _s = s;  }
        public override Bookmark<T> Next()
        {
            return (_s.next==SList<T>.Empty) ? null : new SListBookmark<T>(_s.next, _pos + 1);
        }
        public override T Value => _s.element;
    }
}
