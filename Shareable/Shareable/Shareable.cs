using System.Text;
#nullable enable
namespace Shareable
{
    public abstract class Shareable<T>
    {
        public readonly int? Length;
        protected Shareable(int? c = null) { Length = c; }
        public abstract Bookmark<T>? First();
        public T[]? ToArray()
        {
            if (Length == null)
                return null;
            var r = new T[Length.Value];
            for (var b = First(); b != null; b = b.Next())
                r[b.Position] = b.Value;
            return r;
        }
    }
    public abstract class Bookmark<T>
    {
        public readonly int Position;
        protected Bookmark(int p) { Position = p; }
        public abstract Bookmark<T>? Next();
        public abstract T Value { get; }
        public virtual void Append(StringBuilder sb) { }
    }
}
