namespace Shareable
{
    public abstract class Shareable<T>
    {
        readonly int _count;
        protected Shareable(int c = 0) { _count = c; }
        public abstract Bookmark<T> First();
        public int Length => _count;
        public T[] ToArray()
        {
            var r = new T[_count];
            for (var b = First(); b != null; b = b.Next())
                r[b.Position] = b.Value;
            return r;
        }
    }
    public abstract class Bookmark<T>
    {
        internal readonly int _pos;
        protected Bookmark(int p) { _pos = p; }
        public abstract Bookmark<T> Next();
        public abstract T Value { get; }
        public int Position=>_pos; // >=0
    }
}
