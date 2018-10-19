using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shareable
{
    public class SMTree : SDict<SCList<Variant>, Variant>,IComparable
    {
        public class SITree : SDict<Variant, Variant>
        {
            public readonly TreeInfo info;
            public readonly Variants variant;
            internal SITree(TreeInfo ti) : base(null) { info = ti; }
            SITree(TreeInfo ti,SBucket<Variant, Variant> r) : base(r)
            {
                info = ti;
                variant = ((r?.count??0)>0)?((Variant)r.Slot(0).val).variant:Variants.Compound;
            }
            internal SITree(TreeInfo ti, Variant k, Variant v)
                : this(ti, new SLeaf<Variant, Variant>(new SSlot<Variant, Variant>(k, v))) { }
            internal SITree Update(Variant k, Variant v)
            {
                return new SITree(info, root.Update(k, v));
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
                return (root == null || root.total == 0) ? new SITree(info,k, v) :
                    (root.Contains(k)) ? new SITree(info, root.Update(k, v)) :
                    (root.count == Size) ? new SITree(info, root.Split()).Add(k, v) :
                    new SITree(info, root.Add(k, v));
            }
            public override SDict<Variant, Variant> Remove(Variant k)
            {
                return (root == null || root.Lookup(k) == null) ? this :
                    (root.total == 1) ? Empty :
                    new SITree(info,root.Remove(k));
            }
        }
        public readonly SITree _impl;
        public readonly SList<TreeInfo> _info;
        public readonly int _count;
        SMTree(SList<TreeInfo> ti, SITree impl,int c) :base(null)
        {
            _info = ti;
            _impl = impl;
            _count = c;
            if (ti.Length>1 && ti.element.onDuplicate != TreeBehaviour.Disallow)
                throw new Exception("Dplicates are allowed only on last TreeInfo");
        }
        public SMTree(SList<TreeInfo> ti) : this(ti, (SITree)null, 0)  { }
        public SMTree(SList<TreeInfo> ti,SList<Variant> k,int v) :this(ti)
        {
            if (ti.Length<2)
            {
                if (ti.element.onDuplicate == TreeBehaviour.Allow)
                    _impl = new SITree(ti.element, k.element,
                        new Variant(Variants.Partial, new SDict<int,bool>(v, true)));
                else
                    _impl = new SITree(ti.element, k.element, new Variant(v));
            }
            else
                _impl = new SITree(ti.element, k.element,
                    new Variant(Variants.Compound, new SMTree(ti.next, k.next, v)));
            _count = 1;
        }
        public override Bookmark<SSlot<SCList<Variant>,Variant>> First()
        {
            return MTreeBookmark.New(this);
        }
        public MTreeBookmark PositionAt(SCList<Variant> k)
        {
            return MTreeBookmark.New(this, k);
        }
        public SMTree Add(int v,params Variant[] k)
        {
            var r = Add(SCList<Variant>.New(k), v,out TreeBehaviour tb);
            return (tb == TreeBehaviour.Allow) ? r : throw new Exception(tb.ToString());
        }
        public SMTree Add(SCList<Variant> k,int v, out TreeBehaviour tb)
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
                return new SMTree(_info, k, v);
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
                            var mt = tv.ob as SMTree;
                            mt = mt.Add(k.next as SCList<Variant>, new Variant(v)) as SMTree;
                            nv = new Variant(Variants.Compound,mt); // care: immutable
                            break;
                        }
                    case Variants.Partial:
                        {
                            var bt = tv.ob as SDict<int, bool>;
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
                            var mt = new SMTree(_info.next, k.next as SCList<Variant>, v);
                            nv = new Variant(Variants.Compound,mt);
                            break;
                        }
                    case Variants.Partial:
                        {
                            var bt = new SDict<int, bool>(v, true);
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
            return new SMTree(_info, st, _count + 1);
        }
        public override SDict<SCList<Variant>, Variant> Add(SCList<Variant> k, Variant v)
        {
            var r = Add(k, (int)v.ob, out TreeBehaviour tb);
            return (tb==TreeBehaviour.Allow)?r:throw new Exception("internal error");
        }
        public override SDict<SCList<Variant>, Variant> Remove(SCList<Variant> k)
        {
            if (!Contains(k))
                return this;
            SITree st = _impl; 
            var k0 = k.element;
            Variant tv = _impl.Lookup(k0);
            var nc = _count;
            switch (tv.variant)
            {
                case Variants.Compound:
                    {
                        var mt = tv.ob as SMTree;
                        var c = mt._count;
                        mt = mt.Remove(k.next as SCList<Variant>) as SMTree;
                        nc -= c - mt._count;
                        if (mt.Count == 0)
                            st = st.Remove(k0) as SITree;
                        else
                            st = st.Update(k0, new Variant(Variants.Compound,mt));
                        break;
                    }
                case Variants.Partial:
                    {
                        var bt = tv.ob as SDict<int,bool>;
                        nc -= bt.Count;
                        st = st.Remove(k0) as SITree;
                        break;
                    }
                case Variants.Single:
                    nc--;
                    st = st.Remove(k0) as SITree;
                    break;
            }
            return new SMTree(_info, st, nc);
        }
        public int CompareTo(object obj)
        {
            var that = obj as SMTree;
            if (that == null || that.Count == 0)
                return 1;
            return First().Value.key.CompareTo(that.First().Value.key);
        }
    }
    public class MTreeBookmark : Bookmark<SSlot<SCList<Variant>, Variant>>
    {
        readonly SDictBookmark<Variant,Variant> _outer;
        internal readonly SList<TreeInfo> _info;
        internal readonly MTreeBookmark _inner;
        readonly Bookmark<SSlot<int, bool>> _pmk;
        internal readonly bool _changed;
        internal SCList<Variant> _filter;
        MTreeBookmark(SDictBookmark<Variant, Variant> outer, SList<TreeInfo> info,
            bool changed, MTreeBookmark inner,Bookmark<SSlot<int, bool>> pmk, 
            int pos, SCList<Variant> key=null) :base(pos)
        {
            _outer = outer; _info = info; _changed = changed;
            _inner = inner; _pmk = pmk; _filter = key;
        }
        /// <summary>
        /// Implementation of mt.First()
        /// </summary>
        /// <param name="mt"></param>
        /// <returns></returns>
        public static MTreeBookmark New(SMTree mt)
        {
            for (var outer = mt._impl?.First() as SDictBookmark<Variant, Variant>; outer!=null;outer=outer.Next() as SDictBookmark<Variant, Variant>)
            {
                var ov = outer.Value.val;
                switch (ov.variant)
                {
                    case Variants.Compound:
                        if ((ov.ob as SMTree)?.First() is MTreeBookmark inner)
                            return new MTreeBookmark(outer, mt._info, false, inner,null, 0);
                        break;
                    case Variants.Partial:
                        if ((ov.ob as SDict<int, bool>)?.First() is Bookmark<SSlot<int, bool>> pmk)
                            return new MTreeBookmark(outer, mt._info, false, null, pmk, 0);
                        break;
                    case Variants.Single:
                        return new MTreeBookmark(outer, mt._info, false, null, null, 0);
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
        public static MTreeBookmark New(SMTree mt,SCList<Variant> key)
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
                    if ((ov.ob as SMTree)?.PositionAt(key.next as SCList<Variant>) is MTreeBookmark inner)
                        return new MTreeBookmark(outer, mt._info, true, inner, null, 0, key);
                    break;
                case Variants.Partial:
                    if ((ov.ob as SDict<int, bool>)?.First() is Bookmark<SSlot<int, bool>> pmk)
                        return new MTreeBookmark(outer, mt._info, true, null, pmk, 0, key);
                    break;
                case Variants.Single:
                    if (key.next==null)
                        return new MTreeBookmark(outer, mt._info, true, null, null, 0, key);
                    break;
            }
            return null;
        }
        public SCList<Variant> key()
        {
            if (_outer == null)
                return null;
            return new SCList<Variant>(_outer.key, _inner?.key());
        }
        public override SSlot<SCList<Variant>, Variant> Value => throw new NotImplementedException();
        public int value()
        {
            return (_inner!=null)?_inner.value() : (_pmk!=null)?_pmk.Value.key : 
                (_outer.val!=null)?(int)_outer.val.ob : 0;
        }
        public override Bookmark<SSlot<SCList<Variant>, Variant>> Next()
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
                    inner = inner.Next() as MTreeBookmark;
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
                        inner = ((SMTree)oval.ob).PositionAt((SCList<Variant>)_filter?.next);
                        if (inner != null)
                            goto done;
                        break;
                    case Variants.Partial:
                        pmk = ((SDict<int, bool>)oval.ob).First();
                        if (pmk != null)
                            goto done;
                        break;
                    case Variants.Single:
                        goto done;
                }
            }
            done:
            return new MTreeBookmark(outer, _info, changed, inner, pmk, pos + 1, _filter);

        }
    }
    public enum TreeBehaviour { Ignore, Allow, Disallow  };
    public class TreeInfo 
    {
        public readonly string headName;
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
        public TreeInfo(string h, char d, char n)
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
    }
}
