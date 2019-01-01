using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
#nullable enable
namespace Shareable
{
    /// <summary>
    /// SMTree models a multilevel index leading to a long
    /// </summary>
    /// <typeparam name="K"></typeparam>
    public class SMTree<K> : Shareable<SSlot<SCList<Variant>, long>>,IComparable where K:IComparable
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
                : this(ti, vt, new SLeaf<Variant, Variant>(new SSlot<Variant, Variant>(k, v))) { }
            internal SITree Update(Variant k, Variant v)
            {
                if (root == null)
                    return (SITree)Empty;
                return new SITree(info, variant, root.Update(k, v));
            }
            public Bookmark<SSlot<Variant, Variant>>? PositionAt(Variant k)
            {
                if (k?.ob == null)
                    return First();
                SBookmark<Variant, Variant>? bmk = null;
                var cb = root;
                while (cb != null)
                {
                    var bpos = cb.PositionFor(k, out bool b);
                    bmk = new SBookmark<Variant, Variant>(cb, bpos, bmk);
                    if (bpos == cb.count)
                    {
                        var inr = cb as SInner<Variant, Variant>;
                        if (inr == null)
                            return null;
                        cb = inr.gtr;
                    }
                    else
                        cb = (SBucket<Variant, Variant>)(cb.Slot(bpos).val ?? throw new System.Exception("??"));
                }
                return (bmk==null)?null:new SDictBookmark<Variant, Variant>(bmk);
            }
            public override SDict<Variant, Variant> Add(Variant k, Variant v)
            {
                return (root == null || root.total == 0) ? new SITree(info,variant, k, v) :
                    (root.Contains(k)) ? new SITree(info, variant, root.Update(k, v)) :
                    (root.count == Size) ? new SITree(info, variant, root.Split()).Add(k, v) :
                    new SITree(info, variant, root.Add(k, v));
            }
            public override SDict<Variant, Variant> Remove(Variant k)
            {
                return (root == null || root.Lookup(k) == null) ? this :
                    (root.total == 1) ? Empty :
                    new SITree(info,variant, root.Remove(k));
            }
        }
        public readonly SITree _impl;
        public readonly SList<TreeInfo<K>> _info;
        SMTree(SList<TreeInfo<K>> ti, SITree impl,int c) :base(c)
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
            if (e.asc == false && ke.variant == Variants.Ascending)
                ke = new Variant(ke.ob, e.asc);
            _impl = (ti.Length < 2) ?
                ((e.onDuplicate == TreeBehaviour.Allow) ?
                    new SITree(e, Variants.Partial, ke,
                        new Variant(Variants.Partial, new SDict<long, bool>(v, true))) :
                    new SITree(e, e.asc?Variants.Ascending:Variants.Descending,ke, new Variant(v))) :
                new SITree(e, Variants.Compound, ke,
                    new Variant(Variants.Compound, new SMTree<K>(ti.next, k.next, v))); //these are not null
        }
        public override Bookmark<SSlot<SCList<Variant>,long>>? First()
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
                        {
                            var mt = (SMTree<K>)tv.ob;
                            mt = mt.Add((SCList<Variant>)k.next, v); // not null
                            nv = new Variant(Variants.Compound,mt); // care: immutable
                            break;
                        }
                    case Variants.Partial:
                        {
                            var bt = (SDict<long, bool>)tv.ob;
                            bt = bt.Add(v, true);
                            nv = new Variant(Variants.Partial,bt); // care: immutable
                            break;
                        }
                    default:
                        throw new Exception("internal error");
                }
                st = _impl.Update(k.element, nv);
            }
            else
            {
                switch (st.variant)
                {
                    case Variants.Compound:
                        {
                            var mt = new SMTree<K>(_info.next, (SCList<Variant>)k.next, v); // these are not null
                            nv = new Variant(Variants.Compound,mt);
                            break;
                        }
                    case Variants.Partial:
                        {
                            var bt = new SDict<long, bool>(v, true);
                            nv = new Variant(Variants.Partial,bt);
                            break;
                        }
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
                st = (SITree)_impl.Add(k.element, nv); 
            }
            tb = TreeBehaviour.Allow;
            return new SMTree<K>(_info, st, Length.Value + 1);
        }
        public SMTree<K> Add(SCList<Variant> k, long v)
        {
            var r = Add(k, v, out TreeBehaviour tb);
            return (tb==TreeBehaviour.Allow)?r:throw new Exception("internal error");
        }
        public SMTree<K> Remove(SCList<Variant> k, long p)
        {
            if (!Contains(k))
                return this;
            SITree st = _impl;
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
                            st = (SITree)st.Remove(k0);
                        else
                            st = st.Update(k0, new Variant(Variants.Compound, mt));
                        break;
                    }
                case Variants.Partial:
                    {
                        var bt = (SDict<long, bool>)tv.ob;
                        if (!bt.Contains(p))
                            return this;
                        nc--;
                        bt = bt.Remove(p);
                        if (bt.Length == 0)
                            st = (SITree)st.Remove(k0);
                        else
                            st = st.Update(k0, new Variant(Variants.Partial, bt));
                        break;
                    }
                case Variants.Ascending:
                case Variants.Descending:
                    nc--;
                    st = (SITree)st.Remove(k0);
                    break;
            }
            return new SMTree<K>(_info, st, nc.Value);
        }
        public SMTree<K> Remove(SCList<Variant> k)
        {
            if (!Contains(k))
                return this;
            SITree st = _impl; 
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
                            st = (SITree)st.Remove(k0);
                        else
                            st = st.Update(k0, new Variant(Variants.Compound,mt));
                        break;
                    }
                case Variants.Partial:
                    {
                        var bt = (SDict<long,bool>)tv.ob;
                        nc -= bt.Length;
                        st = (SITree)st.Remove(k0);
                        break;
                    }
                case Variants.Ascending:
                case Variants.Descending:
                    nc--;
                    st = (SITree)st.Remove(k0);
                    break;
            }
            return new SMTree<K>(_info, st, nc.Value);
        }
        public int CompareTo(object obj)
        {
            var that = (SMTree<K>)obj;
            if (that == null || that.Length == 0)
                return (Length == 0) ? 0 : 1;
            if (Length == 0)
                return -1;
            var a = First() ?? throw new System.Exception("??");
            var b = that.First() ?? throw new System.Exception("??");
            return a.Value.key.CompareTo(b.Value.key); // not null
        }
    }
    public class MTreeBookmark<K> :Bookmark<SSlot<SCList<Variant>,long>> where K:IComparable
    {
        readonly SDictBookmark<Variant, Variant> _outer;
        internal readonly SList<TreeInfo<K>> _info;
        internal readonly MTreeBookmark<K>? _inner;
        readonly Bookmark<SSlot<long, bool>>? _pmk;
        internal SCList<Variant> _filter;

        MTreeBookmark(SDictBookmark<Variant, Variant> outer, SList<TreeInfo<K>> info,
            MTreeBookmark<K>? inner, Bookmark<SSlot<long, bool>>? pmk,
            int pos, SCList<Variant>? key = null) : base(pos)
        {
            _outer = outer; _info = info;
            _inner = inner; _pmk = pmk; _filter = key??SCList<Variant>.Empty;
        }
        /// <summary>
        /// Implementation of mt.First()
        /// </summary>
        /// <param name="mt"></param>
        /// <returns></returns>
        public static MTreeBookmark<K>? New(SMTree<K> mt)
        {
            for (var outer = mt._impl?.First() as SDictBookmark<Variant, Variant>; outer != null; outer = outer.Next() as SDictBookmark<Variant, Variant>)
            {
                var ov = outer.Value.val;
                switch (ov.variant)
                {
                    case Variants.Compound:
                        if ((ov.ob as SMTree<K>)?.First() is MTreeBookmark<K> inner)
                            return new MTreeBookmark<K>(outer, mt._info, inner, null, 0);
                        break;
                    case Variants.Partial:
                        if ((ov.ob as SDict<long, bool>)?.First() is Bookmark<SSlot<long, bool>> pmk)
                            return new MTreeBookmark<K>(outer, mt._info, null, pmk, 0);
                        break;
                    case Variants.Ascending:
                    case Variants.Descending:
                        return new MTreeBookmark<K>(outer, mt._info, null, null, 0);
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
            var outer = mt._impl?.PositionAt(key.element) as SDictBookmark<Variant, Variant>;
            if (outer == null)
                return null;
            var ov = outer.Value.val;
            switch (ov.variant)
            {
                case Variants.Compound:
                    if ((ov.ob as SMTree<K>)?.PositionAt((SCList<Variant>)key.next) is MTreeBookmark<K> inner) // next not null
                        return new MTreeBookmark<K>(outer, mt._info, inner, null, 0, key);
                    break;
                case Variants.Partial:
                    if ((ov.ob as SDict<long, bool>)?.First() is Bookmark<SSlot<long, bool>> pmk)
                        return new MTreeBookmark<K>(outer, mt._info, null, pmk, 0, key);
                    break;
                case Variants.Ascending:
                case Variants.Descending:
                    if (key.next == null)
                        return new MTreeBookmark<K>(outer, mt._info, null, null, 0, key);
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
            return (_inner != null) ? _inner.value() : (_pmk != null) ? _pmk.Value.key :
                (_outer.val != null) ? (long)_outer.val.ob : 0;
        }
        public SRecord Get(SDatabase db)
        {
            return db.Get(value());
        }
        public override Bookmark<SSlot<SCList<Variant>,long>>? Next()
        {
            var inner = _inner;
            var outer = _outer;
            var pmk = _pmk;
            var pos = Position;
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
                    outer = (SDictBookmark<Variant,Variant>)ou;
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
            return new MTreeBookmark<K>(outer, _info, inner, pmk, pos + 1, _filter);

        }
        public override SSlot<SCList<Variant>, long> Value
            => new SSlot<SCList<Variant>, long>(key(),value());
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
    }
}
