using System;
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
            var them = obj as SList<K> ?? throw new Exception("Type mismatch");
            SList<K> me = this;
            for (; me.Length > 0 && them.Length > 0; me = me.next, them = them.next)
            {
                var c = me.element.CompareTo(them.element);
                if (c != 0)
                    return c;
            }
            return (me.Length > 0) ? 1 : (them.Length > 0) ? -1 : 0;
        }
        public override Bookmark<K> First()
        {
            return (Length == 0) ? null : new SCListBookmark<K>(this);
        }
    }
    public class SCListBookmark<K> : SListBookmark<K> where K : IComparable
    {
        internal new readonly SCList<K> _s;
        internal SCListBookmark(SCList<K> s, int p = 0) : base(s, p) { _s = s; }
        public override Bookmark<K> Next()
        {
            return (_s.Length <= 1) ? null : new SCListBookmark<K>((SCList<K>)_s.next, Position + 1);
        }
    }
}
