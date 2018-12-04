using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shareable
{
    public class SMTree<K> : Shareable<SSlot<SCList<Variant>, long>>,IComparable where K:IComparable<K>
    {
        public class SITree : SDict<Variant, Variant>
        {
            public readonly TreeInfo<K> info;
            public readonly Variants variant;
            internal SITree(TreeInfo<K> ti,Variants vt) : base((SBucket<Variant,Variant>)null)
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
                return new SITree(info, variant, root.Update(k, v));
            }
            public Bookmark<SSlot<Variant, Variant>> PositionAt(Variant k)
            {
                if (k?.ob == null)
                    return First();
                SBookmark<Variant, Variant> bmk = null;
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
                        cb = cb.Slot(bpos).val as SBucket<Variant, Variant>;
                }
                return new SDictBookmark<Variant, Variant>(bmk);
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
            _impl = (ti.Length < 2) ?
                ((ti.element.onDuplicate == TreeBehaviour.Allow) ?
                    new SITree(ti.element, Variants.Partial, k.element,
                        new Variant(Variants.Partial, new SDict<long, bool>(v, true))) :
                    new SITree(ti.element, Variants.Single, k.element, new Variant(v))) :
                new SITree(ti.element, Variants.Compound, k.element,
                    new Variant(Variants.Compound, new SMTree<K>(ti.next, k.next, v)));
        }
        public override Bookmark<SSlot<SCList<Variant>,long>> First()
        {
            return MTreeBookmark<K>.New(this);
        }
        public MTreeBookmark<K> PositionAt(SCList<Variant> k)
        {
            return MTreeBookmark<K>.New(this, k);
        }
        public bool Contains(SCList<Variant> k)
        {
            return (k.Length == 0) ? 
                Length != 0 :
                (_impl?.Lookup(k.element) is Variant v) ?
                    ((v.variant == Variants.Compound) ?
                        ((SMTree<K>)v.ob).Contains((SCList<Variant>)k.next) :
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
                k = new SCList<Variant>(new Variant(0),null);
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
            Variant nv = null;
            SITree st = _impl;
            if (st.Contains(k.element))
            {
                Variant tv = st.Lookup(k.element);
                switch (tv.variant)
                {
                    case Variants.Compound:
                        {
                            var mt = tv.ob as SMTree<K>;
                            mt = mt.Add(k.next as SCList<Variant>, v) as SMTree<K>;
                            nv = new Variant(Variants.Compound,mt); // care: immutable
                            break;
                        }
                    case Variants.Partial:
                        {
                            var bt = tv.ob as SDict<long, bool>;
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
                            var mt = new SMTree<K>(_info.next, k.next as SCList<Variant>, v);
                            nv = new Variant(Variants.Compound,mt);
                            break;
                        }
                    case Variants.Partial:
                        {
                            var bt = new SDict<long, bool>(v, true);
                            nv = new Variant(Variants.Partial,bt);
                            break;
                        }
                    case Variants.Single:
                        if (_info.element.onDuplicate == TreeBehaviour.Allow)
                            goto case Variants.Partial;
                        nv = new Variant(v);
                        break;
                }
                st = _impl.Add(k.element, nv) as SITree;
            }
            tb = TreeBehaviour.Allow;
            return new SMTree<K>(_info, st, Length + 1);
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
                        var mt = tv.ob as SMTree<K>;
                        var c = mt.Length;
                        mt = mt.Remove(k.next as SCList<Variant>,p) as SMTree<K>;
                        nc -= c - mt.Length;
                        if (mt.Length == 0)
                            st = st.Remove(k0) as SITree;
                        else
                            st = st.Update(k0, new Variant(Variants.Compound, mt));
                        break;
                    }
                case Variants.Partial:
                    {
                        var bt = tv.ob as SDict<long, bool>;
                        if (!bt.Contains(p))
                            return this;
                        nc--;
                        bt = bt.Remove(p);
                        if (bt.Length == 0)
                            st = st.Remove(k0) as SITree;
                        else
                            st = st.Update(k0, new Variant(Variants.Partial, bt));
                        break;
                    }
                case Variants.Single:
                    nc--;
                    st = st.Remove(k0) as SITree;
                    break;
            }
            return new SMTree<K>(_info, st, nc);
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
                        var mt = tv.ob as SMTree<K>;
                        var c = mt.Length;
                        mt = mt.Remove(k.next as SCList<Variant>) as SMTree<K>;
                        nc -= c - mt.Length;
                        if (mt.Length == 0)
                            st = st.Remove(k0) as SITree;
                        else
                            st = st.Update(k0, new Variant(Variants.Compound,mt));
                        break;
                    }
                case Variants.Partial:
                    {
                        var bt = tv.ob as SDict<long,bool>;
                        nc -= bt.Length;
                        st = st.Remove(k0) as SITree;
                        break;
                    }
                case Variants.Single:
                    nc--;
                    st = st.Remove(k0) as SITree;
                    break;
            }
            return new SMTree<K>(_info, st, nc);
        }
        public int CompareTo(object obj)
        {
            var that = obj as SMTree<K>;
            if (that == null || that.Length == 0)
                return 1;
            return First().Value.key.CompareTo(that.First().Value.key);
        }
    }
    public class MTreeBookmark<K> :Bookmark<SSlot<SCList<Variant>,long>> where K:IComparable<K>
    {
        readonly SDictBookmark<Variant, Variant> _outer;
        internal readonly SList<TreeInfo<K>> _info;
        internal readonly MTreeBookmark<K> _inner;
        readonly Bookmark<SSlot<long, bool>> _pmk;
        internal readonly bool _changed;
        internal SCList<Variant> _filter;


        MTreeBookmark(SDictBookmark<Variant, Variant> outer, SList<TreeInfo<K>> info,
            bool changed, MTreeBookmark<K> inner, Bookmark<SSlot<long, bool>> pmk,
            int pos, SCList<Variant> key = null) : base(pos)
        {
            _outer = outer; _info = info; _changed = changed;
            _inner = inner; _pmk = pmk; _filter = key;
        }
        /// <summary>
        /// Implementation of mt.First()
        /// </summary>
        /// <param name="mt"></param>
        /// <returns></returns>
        public static MTreeBookmark<K> New(SMTree<K> mt)
        {
            for (var outer = mt._impl?.First() as SDictBookmark<Variant, Variant>; outer != null; outer = outer.Next() as SDictBookmark<Variant, Variant>)
            {
                var ov = outer.Value.val;
                switch (ov.variant)
                {
                    case Variants.Compound:
                        if ((ov.ob as SMTree<K>)?.First() is MTreeBookmark<K> inner)
                            return new MTreeBookmark<K>(outer, mt._info, false, inner, null, 0);
                        break;
                    case Variants.Partial:
                        if ((ov.ob as SDict<long, bool>)?.First() is Bookmark<SSlot<long, bool>> pmk)
                            return new MTreeBookmark<K>(outer, mt._info, false, null, pmk, 0);
                        break;
                    case Variants.Single:
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
        public static MTreeBookmark<K> New(SMTree<K> mt, SCList<Variant> key)
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
                    if ((ov.ob as SMTree<K>)?.PositionAt(key.next as SCList<Variant>) is MTreeBookmark<K> inner)
                        return new MTreeBookmark<K>(outer, mt._info, true, inner, null, 0, key);
                    break;
                case Variants.Partial:
                    if ((ov.ob as SDict<long, bool>)?.First() is Bookmark<SSlot<long, bool>> pmk)
                        return new MTreeBookmark<K>(outer, mt._info, true, null, pmk, 0, key);
                    break;
                case Variants.Single:
                    if (key.next == null)
                        return new MTreeBookmark<K>(outer, mt._info, true, null, null, 0, key);
                    break;
            }
            return null;
        }
        public SCList<Variant> key()
        {
            if (_outer == null)
                return null;
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
        public override Bookmark<SSlot<SCList<Variant>,long>> Next()
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
                outer = outer.Next() as SDictBookmark<Variant,Variant>;
                if (outer == null)
                    return null;
                changed = true;
                var oval = outer.val;
                switch (oval.variant)
                {
                    case Variants.Compound:
                        inner = ((SMTree<K>)oval.ob).PositionAt((SCList<Variant>)_filter?.next);
                        if (inner != null)
                            goto done;
                        break;
                    case Variants.Partial:
                        pmk = ((SDict<long, bool>)oval.ob).First();
                        if (pmk != null)
                            goto done;
                        break;
                    case Variants.Single:
                        goto done;
                }
            }
            done:
            return new MTreeBookmark<K>(outer, _info, changed, inner, pmk, pos + 1, _filter);

        }
        public override SSlot<SCList<Variant>, long> Value
            => new SSlot<SCList<Variant>, long>(key(),value());
    }
    public enum TreeBehaviour { Ignore, Allow, Disallow  };
    public class TreeInfo<K> where K:IComparable<K>
    {
        public readonly K headName;
        public readonly TreeBehaviour onDuplicate, onNullKey;
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
        public TreeInfo(K h, char d, char n)
        {
            headName = h;
            onDuplicate = For(d);
            onNullKey = For(n);
        }
    }
    public enum Variants { Single, Partial, Compound }
    public class Variant :IComparable
    {
        public readonly Variants variant;
        public readonly object ob;
        public Variant(Variants t,object v)
        {
            variant = t;
            ob = v;
        }
        public Variant(object v)
        {
            if (v is Variant)
                throw new Exception("Internal error");
            variant = Variants.Single;
            ob = v;
        }
        public int CompareTo(object obj)
        {
            return ((IComparable)ob).CompareTo(((Variant)obj).ob);
        }

        internal long ToLong()
        {
            return (ob is long) ? (long)ob : 0;
        }
    }
}
