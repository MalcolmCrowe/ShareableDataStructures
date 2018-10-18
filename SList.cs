using System;
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
        protected SList() { element = default(T); next = null; }
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
    public class SCList<K> : SList<K>,IComparable where K:IComparable
    {
        public new static readonly SCList<K> Empty = new SCList<K>();
        SCList() { }
        public SCList(K el, SCList<K> nx) : base(el, nx) { }
        public new static SCList<K> New(params K[] els)
        {
            var r = Empty;
            for (var i = els.Length - 1; i >= 0; i--)
                r = new SCList<K>(els[i], r);
            return r;
        }
        public int CompareTo(object obj)
        {
            var them = obj as SList<K>;
            SList<K> me = this;
            for (;me!=null && them!=null; me=me.next, them=them.next)
            {
                var c = me.element.CompareTo(them.element);
                if (c != 0)
                    return c;
            }
            return (me != null) ? 1 : (them != null) ? -1 : 0;
        }
    }
    public class SListBookmark<T> : Bookmark<T>
    {
        internal readonly SList<T> _s;
        internal SListBookmark(SList<T> s, int p = 0) :base(p) { _s = s;  }
        public override Bookmark<T> Next()
        {
            return (_s.next==SList<T>.Empty) ? null : new SListBookmark<T>(_s.next, Position + 1);
        }
        public override T Value => _s.element;
    }
}
