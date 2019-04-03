using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
namespace Shareable
{
    /// <summary>
    /// SMTree models a multilevel index leading to a long
    /// </summary>
    /// <typeparam name="K"></typeparam>
    public class SMTree<K> : Collection<(SCList<Variant>, long)>,IComparable where K:IComparable
    {
        public class SITree : SDict<Variant, Variant>
        {
            public readonly TreeInfo<K> info;
            public readonly Variants variant;
            internal SITree(TreeInfo<K> ti,Variants vt) : base((SBucket<Variant,Variant>?)null)
            {
                info = ti; variant = vt;
            }
            SITree(TreeInfo<K> ti,Variants vt,SBucket<Variant, Variant> r) : base(r)
            {
                info = ti;
                variant = vt;
            }
            internal SITree(TreeInfo<K> ti, Variants vt, Variant k, Variant v)
                : this(ti, vt, new SLeaf<Variant, Variant>((k, v))) { }
            internal SITree? Update(Variant k, Variant v)
            {
                if (root == null)
                    return null;
                return new SITree(info, variant, root.Update(k, v));
            }
            public override Bookmark<(Variant, Variant)>? PositionAt(Variant k)
            {
                if (k?.ob == null)
                    return First();
                return base.PositionAt(k);
            }
            protected override SDict<Variant, Variant> Add(Variant k, Variant v)
            {
                return (root == null || root.total == 0) ? new SITree(info,variant, k, v) :
                    (root.Contains(k)) ? new SITree(info, variant, root.Update(k, v)) :
                    (root.count == Size) ? new SITree(info, variant, root.Split()).Add(k, v) :
                    new SITree(info, variant, root.Add(k, v));
            }
            internal new SITree? Remove(Variant k)
            {
                return (root == null || root.Lookup(k) == null) ? this :
                    (root.total == 1) ? null :
                    new SITree(info,variant, root.Remove(k));
            }
        }
        public readonly SITree? _impl;
        public readonly SList<TreeInfo<K>> _info;
        SMTree(SList<TreeInfo<K>> ti, SITree? impl,int c) :base(c)
        {
            _info = ti;
            _impl = impl;
        }
        public SMTree(SList<TreeInfo<K>> ti) : base(0)
        {
            _info = ti;
            _impl = null;
        }
        public SMTree(SList<TreeInfo<K>> ti,SList<Variant> k,long v) :base(1)
        {
            _info = ti;
            var e = ti.element;
            var ke = k.element;
            if (e.asc != (ke.variant == Variants.Ascending))
                ke = new Variant(ke.ob, e.asc);
            _impl = (ti.Length < 2) ?
                ((e.onDuplicate == TreeBehaviour.Allow) ?
                    new SITree(e, Variants.Partial, ke,
                        new Variant(Variants.Partial, new SDict<long, bool>(v, true))) :
                    new SITree(e, e.asc?Variants.Ascending:Variants.Descending,ke, new Variant(v))) :
                new SITree(e, Variants.Compound, ke,
                    new Variant(Variants.Compound, new SMTree<K>(ti.next, k.next, v))); //these are not null
        }
        public override Bookmark<(SCList<Variant>,long)>? First()
        {
            return MTreeBookmark<K>.New(this);
        }
        public MTreeBookmark<K>? PositionAt(SCList<Variant>? k)
        {
            return MTreeBookmark<K>.New(this, k);
        }
        public bool Contains(SCList<Variant> k)
        {
            return (k.Length == 0) ? 
                Length != 0 :
                (_impl?.Lookup(k.element) is Variant v) ?
                    ((v.variant == Variants.Compound) ?
                        ((SMTree<K>)v.ob).Contains((SCList<Variant>)k.next) : // not null
                        true) :
                    false;
        }
        public SMTree<K> Add(long v,params Variant[] k)
        {
            var r = Add(SCList<Variant>.New(k), v,out TreeBehaviour tb);
            return (tb == TreeBehaviour.Allow) ? r : throw new Exception(tb.ToString());
        }
        public SMTree<K> Add(SCList<Variant> k,long v, out TreeBehaviour tb)
        {
            if (k == null)
            {
                if (_info.element.onNullKey != TreeBehaviour.Allow)
                {
                    tb = _info.element.onNullKey;
                    return this;
                }
                k = SCList<Variant>.Empty;
            }
            if (Contains(k) && _info.element.onDuplicate != TreeBehaviour.Allow)
            {
                tb = _info.element.onDuplicate;
                return this;
            }
            if (_impl == null)
            {
                tb = TreeBehaviour.Allow;
                return new SMTree<K>(_info, k, v);
            }
            Variant nv;
            SITree st = _impl;
            if (st.Contains(k.element))
            {
                Variant tv = st.Lookup(k.element);
                switch (tv.variant)
                {
                    case Variants.Compound:
                        nv = new Variant(Variants.Compound, (SMTree<K>)tv.ob + ((SCList<Variant>)k.next, v)); // care: immutable
                        break;
                    case Variants.Partial:
                        nv = new Variant(Variants.Partial, (SDict<long, bool>)tv.ob + (v, true)); // care: immutable
                        break;
                    default:
                        throw new Exception("internal error");
                }
                st = _impl.Update(k.element, nv) ?? throw new Exception("Impossible");
            }
            else
            {
                switch (st.variant)
                {
                    case Variants.Compound:
                        nv = new Variant(Variants.Compound, new SMTree<K>(_info.next, (SCList<Variant>)k.next, v)); // these are not null);
                        break;
                    case Variants.Partial:
                        nv = new Variant(Variants.Partial, new SDict<long, bool>(v, true));
                        break;
                    case Variants.Ascending:
                    case Variants.Descending:
                        if (_info.element.onDuplicate == TreeBehaviour.Allow)
                            goto case Variants.Partial;
                        nv = new Variant(v);
                        break;
                    default: // assure VS that nv has been assigned!
                        nv = new Variant(0);
                        break;
                }
                st = (SITree)(_impl + (k.element, nv)); 
            }
            tb = TreeBehaviour.Allow;
            return new SMTree<K>(_info, st, (Length??0) + 1);
        }
        protected SMTree<K> Add(SCList<Variant> k, long v)
        {
            var r = Add(k, v, out TreeBehaviour tb);
            return (tb==TreeBehaviour.Allow)?r:throw new Exception("Duplicate key");
        }
        public static SMTree<K> operator+(SMTree<K> t, (SCList<Variant>,long) x)
        {
            return t.Add(x.Item1,x.Item2);
        }
        protected SMTree<K> Remove(SCList<Variant> k, long p)
        {
            SITree? st = _impl;
            if (!Contains(k) || _impl==null)
                return this;
            var k0 = k.element;
            Variant tv = _impl.Lookup(k0);
            var nc = Length;
            switch (tv.variant)
            {
                case Variants.Compound:
                    {
                        var mt = (SMTree<K>)tv.ob;
                        var c = mt.Length;
                        mt = mt.Remove((SCList<Variant>)k.next,p) as SMTree<K>; // not null
                        nc -= c - mt.Length;
                        if (mt.Length == 0)
                            st = st?.Remove(k0);
                        else
                            st = st?.Update(k0, new Variant(Variants.Compound, mt));
                        break;
                    }
                case Variants.Partial:
                    {
                        var bt = (SDict<long, bool>)tv.ob;
                        if (!bt.Contains(p))
                            return this;
                        nc--;
                        bt = bt-p;
                        if (bt.Length == 0)
                            st = st?.Remove(k0);
                        else
                            st = st?.Update(k0, new Variant(Variants.Partial, bt));
                        break;
                    }
                case Variants.Ascending:
                case Variants.Descending:
                    nc--;
                    st = st?.Remove(k0);
                    break;
            }
            return new SMTree<K>(_info, st, nc??0);
        }
        protected SMTree<K> Remove(SCList<Variant> k)
        {
            SITree? st = _impl;
            if (!Contains(k) || _impl==null)
                return this;
            var k0 = k.element;
            Variant tv = _impl.Lookup(k0);
            var nc = Length;
            switch (tv.variant)
            {
                case Variants.Compound:
                    {
                        var mt = (SMTree<K>)tv.ob;
                        var c = mt.Length;
                        mt = mt.Remove((SCList<Variant>)k.next); // not null
                        nc -= c - mt.Length;
                        if (mt.Length == 0)
                            st = st?.Remove(k0);
                        else
                            st = st?.Update(k0, new Variant(Variants.Compound,mt));
                        break;
                    }
                case Variants.Partial:
                    {
                        var bt = (SDict<long,bool>)tv.ob;
                        nc -= bt.Length;
                        st = st?.Remove(k0);
                        break;
                    }
                case Variants.Ascending:
                case Variants.Descending:
                    nc--;
                    st = st?.Remove(k0);
                    break;
            }
            return new SMTree<K>(_info, st, nc??0);
        }
        public static SMTree<K> operator-(SMTree<K>t,SCList<Variant>k)
        {
            return t.Remove(k);
        }
        public static SMTree<K> operator -(SMTree<K> t, (SCList<Variant>,long) x)
        {
            return t.Remove(x.Item1,x.Item2);
        }
        public int CompareTo(object obj)
        {
            var that = (SMTree<K>)obj;
            if (that == null || that.Length == 0)
                return (Length == 0) ? 0 : 1;
            if (Length == 0)
                return -1;
            var a = First() ?? throw new System.Exception("PE15");
            var b = that.First() ?? throw new System.Exception("PE16");
            return a.Value.Item1.CompareTo(b.Value.Item1); // not null
        }
    }
    public class MTreeBookmark<K> :Bookmark<(SCList<Variant>,long)> where K:IComparable
    {
        readonly SBookmark<Variant, Variant> _outer;
        internal readonly SList<TreeInfo<K>> _info;
        internal readonly MTreeBookmark<K>? _inner;
        readonly Bookmark<(long, bool)>? _pmk;
        internal readonly SCList<Variant> _filter;
        internal readonly bool _changed;
        MTreeBookmark(SBookmark<Variant, Variant> outer, SList<TreeInfo<K>> info,
            bool changed, MTreeBookmark<K>? inner, Bookmark<(long, bool)>? pmk,
            int pos, SCList<Variant>? key = null) : base(pos)
        {
            _outer = outer; _info = info; _changed = changed;
            _inner = inner; _pmk = pmk; _filter = key??SCList<Variant>.Empty;
        }
        /// <summary>
        /// Implementation of mt.First()
        /// </summary>
        /// <param name="mt"></param>
        /// <returns></returns>
        public static MTreeBookmark<K>? New(SMTree<K> mt)
        {
            for (var outer = mt._impl?.First() as SBookmark<Variant, Variant>; outer != null; outer = outer.Next() as SBookmark<Variant, Variant>)
            {
                var ov = outer.Value.Item2;
                switch (ov.variant)
                {
                    case Variants.Compound:
                        if ((ov.ob as SMTree<K>)?.First() is MTreeBookmark<K> inner)
                            return new MTreeBookmark<K>(outer, mt._info, false, inner, null, 0);
                        break;
                    case Variants.Partial:
                        if ((ov.ob as SDict<long, bool>)?.First() is Bookmark<(long, bool)> pmk)
                            return new MTreeBookmark<K>(outer, mt._info, false, null, pmk, 0);
                        break;
                    case Variants.Ascending:
                    case Variants.Descending:
                        return new MTreeBookmark<K>(outer, mt._info, false, null, null, 0);
                }
            }
            return null;
        }
        /// <summary>
        /// Gets a bookmark starting from a given key
        /// </summary>
        /// <param name="mt"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static MTreeBookmark<K>? New(SMTree<K> mt, SCList<Variant>? key)
        {
            if (key == null || key.Length == 0)
                return New(mt);
            var outer = mt._impl?.PositionAt(key.element) as SBookmark<Variant, Variant>;
            if (outer == null)
                return null;
            var ov = outer.Value.Item2;
            switch (ov.variant)
            {
                case Variants.Compound:
                    if ((ov.ob as SMTree<K>)?.PositionAt((SCList<Variant>)key.next) is MTreeBookmark<K> inner) // next not null
                        return new MTreeBookmark<K>(outer, mt._info, true, inner, null, 0, key);
                    break;
                case Variants.Partial:
                    if ((ov.ob as SDict<long, bool>)?.First() is Bookmark<(long, bool)> pmk)
                        return new MTreeBookmark<K>(outer, mt._info, true, null, pmk, 0, key);
                    break;
                case Variants.Ascending:
                case Variants.Descending:
                    if (key.next.Length == 0)
                        return new MTreeBookmark<K>(outer, mt._info, true, null, null, 0, key);
                    break;
            }
            return null;
        }
        public SCList<Variant> key()
        {
            return new SCList<Variant>(_outer.key, _inner?.key()??SCList<Variant>.Empty); 
        }
        public long value()
        {
            return (_inner != null) ? _inner.value() : (_pmk != null) ? _pmk.Value.Item1 :
                (_outer.val != null) ? (long)_outer.val.ob : 0;
        }
        public SRecord Get(SDatabase db)
        {
            return db.Get(value());
        }
        public override Bookmark<(SCList<Variant>,long)>? Next()
        {
            var inner = _inner;
            var outer = _outer;
            var pmk = _pmk;
            var pos = Position;
            var changed = false;
            for (; ; )
            {
                if (inner != null)
                {
                    inner = inner.Next() as MTreeBookmark<K>;
                    if (inner != null)
                        goto done;
                }
                if (pmk != null)
                {
                    pmk = pmk.Next();
                    if (pmk != null)
                        goto done;
                }
                var h = _filter?.element;
                if (h != null)
                    return null;
                var ou = outer.Next();
                if (ou == null)
                    return null;
                else
                    outer = (SBookmark<Variant,Variant>)ou;
                changed = true;
                var oval = outer.val;
                switch (oval.variant)
                {
                    case Variants.Compound:
                        var t = (SMTree<K>)oval.ob;
                        inner = (_filter.Length != 0) ? t.PositionAt((SCList<Variant>)_filter.next) : // ok
                            (MTreeBookmark<K>?)t.First();
                        if (inner != null)
                            goto done;
                        break;
                    case Variants.Partial:
                        pmk = ((SDict<long, bool>)oval.ob).First();
                        if (pmk != null)
                            goto done;
                        break;
                    case Variants.Ascending:
                    case Variants.Descending:
                        goto done;
                }
            }
            done:
            return new MTreeBookmark<K>(outer, _info, changed, inner, pmk, pos + 1, _filter);

        }
        /// <summary>
        /// In join processing if there are ties in both first and second we
        /// often need to repeat groups of tied rows.
        /// </summary>
        /// <param name="depth"></param>
        /// <returns>an earlier bookmark or null</returns>
        internal MTreeBookmark<K> ResetToTiesStart(STransaction tr, int depth)
        {
            var m = (depth > 1) ? _inner?.ResetToTiesStart(tr, depth - 1) : null;
            var ov = (depth == 1) ? _outer.Value.Item2.ob as SDict<long, bool> : null;
            return new MTreeBookmark<K>(_outer, _info, false,
                    m, ov?.First(), _inner?.Position ?? 0);
        }
        /// <summary>
        /// Find out if there are more matches for a partial ordering
        /// </summary>
        /// <param name="depth">The depth in the key</param>
        /// <returns>whether there are more matches</returns>
        internal bool hasMore(STransaction tr, int depth)
        {
            if (depth > 1)
                return _pmk?.Next() != null || (_inner != null && _inner.hasMore(tr, depth - 1));
            var ov = _outer.Value;
            switch (ov.Item1.variant)
            {
                case Variants.Compound:
                    {
                        var m = ov.Item2.ob as SMTree<K>;
                        if (_inner==null||m==null)
                            throw new Exception("!!");
                        return _inner.Position < m.Length - 1;
                    }
                case Variants.Partial:
                    {
                        var t = ov.Item2.ob as SDict<long, bool>;
                        if (_pmk == null || t == null)
                            throw new Exception("!!");
                        return _pmk.Position < t.Length - 1;
                    }
                default:
                    return false;
            }
        }
        /// <summary>
        /// Whether there has been a change at the given depth
        /// </summary>
        /// <param name="depth">the depth in the key</param>
        /// <returns>whether there has been a change</returns>
        internal bool changed(int depth)
        {
            if (_changed)
                return true;
            if (depth > 1 && _inner!=null)
                return _inner.changed(depth - 1);
            return false;
        }
        public override(SCList<Variant>, long) Value
            => (key(),value());
    }
    public enum TreeBehaviour { Ignore, Allow, Disallow  };
    public class TreeInfo<K> where K:IComparable
    {
        public readonly K headName;
        public readonly TreeBehaviour onDuplicate, onNullKey;
        public readonly bool asc;
        TreeBehaviour For(char c)
        {
            switch (c)
            {
                case 'I': return TreeBehaviour.Ignore;
                case 'A': return TreeBehaviour.Allow;
                default:
                case 'D': return TreeBehaviour.Disallow;
            }
        }
        public TreeInfo(K h, char d, char n, bool a=true)
        {
            headName = h;
            onDuplicate = For(d);
            onNullKey = For(n);
            asc = a;
        }
    }
    public enum Variants { Ascending, Descending, Partial, Compound }
    public class Variant :IComparable
    {
        public readonly Variants variant;
        public readonly object ob;
        public Variant(Variants t,object v)
        {
            variant = t;
            ob = v;
        }
        public Variant(object v,bool asc=true)
        {
            if (v is Variant)
                throw new Exception("Internal error");
            variant = asc?Variants.Ascending:Variants.Descending;
            ob = v;
        }
        public int CompareTo(object obj)
        {
            var c = ((IComparable)ob).CompareTo(((Variant)obj).ob);
            return (variant == Variants.Descending) ? -c : c;
        }

        internal long ToLong()
        {
            return (ob is long) ? (long)ob : 0;
        }
        public override string ToString()
        {
            return variant.ToString().Substring(0,1)+" "+ob.ToString();
        }
    }
}
