using System;
#nullable enable
namespace Shareable
{
    /// <summary>
    /// An empty list is Empty. 
    /// SCLists are never null, so don't test for null. Use Length>0.    /// 
    /// </summary>
    /// <typeparam name="K"></typeparam>
    public class SCList<K> : SList<K>, IComparable where K : IComparable
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
            if (obj == null)
                return 1;
            var them = (SList<K>)obj;
            SList<K> me = this;
            for (; me.Length > 0 && them.Length > 0; me = me.next, them = them.next) // not null
            {
                var c = me.element.CompareTo(them.element);
                if (c != 0)
                    return c;
            }
            return (me.Length > 0) ? 1 : (them.Length > 0) ? -1 : 0;
        }
        protected new SCList<K> Push(K x)
        {
            return new SCList<K>(x, this);
        }
        public static SCList<K> operator+(SCList<K>s,K x)
        {
            return s.Push(x);
        }
        protected new SCList<K> Pop()
        {
            return (SCList<K>)next;
        }
        public static SCList<K> operator--(SCList<K> s)
        {
            return s.Pop();
        }
        public new SCList<K> InsertAt(K x, int n) // n>=0
        {
            if (Length == 0 || n == 0)
                return new SCList<K>(x, this);
            return new SCList<K>(element, ((SCList<K>)next).InsertAt(x, n - 1)); // not null
        }
        public new SCList<K> RemoveAt(int n)
        {
            if (Length == 0)
                return Empty;
            if (n == 0)
                return (SCList<K>?)next;
            return new SCList<K>(element, ((SCList<K>)next).RemoveAt(n - 1)); // not null
        }
        public new SCList<K> UpdateAt(K x, int n)
        {
            if (Length == 0)
                return Empty;
            if (n == 0)
                return new SCList<K>(x, (SCList<K>?)next);
            return new SCList<K>(element, ((SCList<K>)next).UpdateAt(x, n - 1)); // not null
        }
        public override Bookmark<K>? First()
        {
            return (Length == 0) ? null : new SCListBookmark<K>(this);
        }
    }
    public class SCListBookmark<K> : SListBookmark<K> where K : IComparable
    {
        internal new readonly SCList<K> _s;
        internal SCListBookmark(SCList<K> s, int p = 0) : base(s, p) { _s = s; }
        public override Bookmark<K>? Next()
        {
            return (_s.Length <= 1) ? null : new SCListBookmark<K>((SCList<K>)_s.next, Position + 1); // not null
        }
    }
}
