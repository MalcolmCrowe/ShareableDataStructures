/// <summary>
/// See "Collection Data Structures" (c) Malcolm Crowe, University of the West of Scotland 2018
/// http://shareabledata.org 
/// This is free-to-use software 
/// </summary>
namespace Shareable
{
    public class SListOfInt 
    {
        public readonly int element;
        public readonly SListOfInt next;
        public SListOfInt(int e, SListOfInt n) { element = e; next = n; }
        public bool Contains(int x)
        {
            return x == element || (next?.Contains(x) ?? false);
        } 
        public int this[int n]
        {
            get { return (n == 0) ? element : next[n - 1]; }
        }
        public int Length
        {
            get { return 1 + (next?.Length ?? 0); }
        }
        public SListOfInt InsertAt(int x, int n)
        {
            if (n == 0)
                return new SListOfInt(x, this);
            return new SListOfInt(element, next.InsertAt(x, n - 1));
        }
        public SListOfInt RemoveAt(int n)
        {
            if (n == 0)
                return next;
            return new SListOfInt(element, next.RemoveAt(n - 1));
        }
    }
}
