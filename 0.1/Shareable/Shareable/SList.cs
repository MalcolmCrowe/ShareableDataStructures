using System;
using System.Text;
/// <summary>
/// See "Collection Data Structures" (c) Malcolm Crowe, University of the West of Scotland 2018
/// See github.com/MalcolmCrowe/ShareableDataStructures
/// This is free-to-use software
/// </summary>
namespace Shareable
{
    /// <summary>
    /// An empty list is Empty. 
    /// SLists are never null, so don't test for null. Use Length>0.
    /// SList can also be used as a stack, as we define suitable methods
    /// for this special case.
    /// </summary>
    /// <typeparam name="T">All fields of type T should be public readonly</typeparam>
    public class SList<T> : Collection<T>
    {
        public readonly T element;
        public readonly SList<T> next;
        public static readonly SList<T> Empty = new SList<T>();
        protected SList():base(0) { element = default(T); next = null; } // allow this one
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
        protected SList<T> Push(T x)
        {
            return new SList<T>(x, this);
        }
        public static SList<T> operator+(SList<T> s,T x)
        {
            return s.Push(x);
        }
        protected SList<T> InsertAt(T x, int n) // n>=0
        {
            if (Length==0 || n==0)
                return new SList<T>(x, this);
            return new SList<T>(element, next.InsertAt(x, n - 1));
        }
        public static SList<T> operator+(SList<T> s,(T,int) x)
        {
            return s.InsertAt(x.Item1, x.Item2);
        }
        public SList<T> Append(SList<T> x)
        {
            var n = 0;
            for (var b = First(); b != null; b = b.Next(), n++)
                x += (b.Value, n);
            return x;
        }
        protected SList<T> Pop()
        {
            return next;
        }
        public static SList<T> operator--(SList<T>s)
        {
            return s.Pop();
        }
        protected SList<T> RemoveAt(int n)
        {
            if (Length==0)
                return Empty;
            if (n == 0)
                return next;
            return new SList<T>(element, next.RemoveAt(n - 1));
        }
        public static SList<T> operator-(SList<T> s,int n)
        {
            return s.RemoveAt(n);
        }
        public SList<T> UpdateAt(T x,int n)
        {
            if (Length == 0)
                return Empty;
            if (n == 0)
                return new SList<T>(x, next);
            return new SList<T>(element, next.UpdateAt(x, n - 1));
        }
        public override Bookmark<T>? First()
        {
            return (Length==0)?null:new SListBookmark<T>(this);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("(");
            var cm = "";
            for (var b=First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.Value);
            }
            sb.Append(')');
            return sb.ToString();
        }
    }
    public class SListBookmark<T> : Bookmark<T>
    {
        internal readonly SList<T> _s;
        internal SListBookmark(SList<T> s, int p = 0) : base(p) { _s = s; }
        public override Bookmark<T>? Next()
        {
            return (_s.Length <= 1) ? null : new SListBookmark<T>(_s.next, Position + 1); // not null
        }
        public override T Value => _s.element;
    }
}
