/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 *
 * @author Malcolm
 */
public class SMTree<K extends Comparable> extends Shareable<SSlot<SCList<Variant>, Long>> implements Comparable {

    public boolean Contains(SCList<Variant> k) {
        if (k.Length == 0) {
            return Length != 0;
        }
        if (_impl != null) {
            Variant v = _impl.Lookup(k.element);
            if (v == null) {
                return false;
            }
            if (v.variant == Variants.Compound) {
                return ((SMTree<K>) v.ob).Contains((SCList<Variant>) k.next);
            }
            return true;
        }
        return false;
    }

    class SITree extends SDict<Variant, Variant> {

        public final TreeInfo info;
        public final Variants variant;

        SITree(TreeInfo ti, Variants vt) {
            super(null);
            info = ti;
            variant = vt;
        }

        SITree(TreeInfo ti, Variants vt, SBucket<Variant, Variant> r) {
            super(r);
            info = ti;
            variant = vt;
        }

        SITree(TreeInfo ti, Variants vt, Variant k, Variant v) {
            this(ti, vt, new SLeaf<Variant, Variant>(new SSlot<Variant, Variant>(k, v)));
        }

        SITree Update(Variant k, Variant v) {
            return new SITree(info, variant, root.Update(k, v));
        }

        public Bookmark<SSlot<Variant, Variant>> PositionAt(Variant k) {
            if (k == null || k.ob == null) {
                return First();
            }
            SBookmark<Variant, Variant> bmk = null;
            SBucket<Variant, Variant> cb = root;
            while (cb != null) {
                MatchPos m = cb.PositionFor(k);
                bmk = new SBookmark<Variant, Variant>(cb, m.pos, bmk);
                if (m.pos == cb.count) {
                    SInner<Variant, Variant> inr = (SInner<Variant, Variant>) cb;
                    if (inr == null) {
                        return null;
                    }
                    cb = inr.gtr;
                } else {
                    cb = (SBucket<Variant, Variant>) cb.Slot(m.pos).val;
                }
            }
            return new SDictBookmark<Variant, Variant>(bmk);
        }

        public SDict<Variant, Variant> Add(Variant k, Variant v) {
            return (root == null || root.total == 0)
                    ? new SITree(info, variant, k, v)
                    : (root.Contains(k))
                    ? new SITree(info, variant, root.Update(k, v))
                    : (root.count == SIZE)
                            ? new SITree(info, variant, root.Split()).Add(k, v)
                            : new SITree(info, variant, root.Add(k, v));
        }

        public SDict<Variant, Variant> Remove(Variant k, long p) {
            return (root == null || root.Lookup(k) == null) ? this
                    : (root.total == 1) ? new SDict<Variant, Variant>(null)
                            : new SITree(info, variant, root.Remove(k));
        }
    }

    class MTResult {

        SMTree t;
        TreeBehaviour tb;

        MTResult(SMTree s, TreeBehaviour b) {
            t = s;
            tb = b;
        }
    }
    public final SITree _impl;
    public final SList<TreeInfo> _info;

    SMTree(SList<TreeInfo> ti, SITree impl, int c) throws Exception {
        super(c);
        _info = ti;
        _impl = impl;
        if (ti.Length > 1 && ti.element.onDuplicate != TreeBehaviour.Disallow) {
            throw new Exception("Dplicates are allowed only on last TreeInfo");
        }
    }

    public SMTree(SList<TreeInfo> ti) throws Exception {
        this(ti, (SITree) null, 0);
    }

    public SMTree(SList<TreeInfo> ti, SList<Variant> k, long v) throws Exception {
        super(1);
        _info = ti;
        _impl = (ti.Length < 2)
                ? ((ti.element.onDuplicate == TreeBehaviour.Allow)
                        ? new SITree(ti.element, Variants.Partial, k.element,
                                new Variant(Variants.Partial, 
                                        new SDict<>(v, true)))
                        : new SITree(ti.element, Variants.Single, k.element, 
                                new Variant(v)))
                : new SITree(ti.element, Variants.Compound, k.element,
                        new Variant(Variants.Compound, 
                                new SMTree(ti.next, k.next, v)));
    }

    public Bookmark<SSlot<SCList<Variant>, Long>> First() {
        return (Length == 0) ? null : MTreeBookmark.New(this);
    }

    public MTreeBookmark PositionAt(SCList<Variant> k) {
        return MTreeBookmark.New(this, k);
    }

    public SMTree Add(int v, Variant... k) throws Exception {
        MTResult r = Add(new SCList<Variant>(k), v);
        if (r.tb == TreeBehaviour.Allow) {
            return r.t;
        }
        throw new Exception(r.tb.toString());
    }

    public MTResult Add(SCList<Variant> k, long v) throws Exception {
        if (k == null) {
            if (_info.element.onNullKey != TreeBehaviour.Allow) {
                return new MTResult(this, _info.element.onNullKey);
            }
            k = new SCList<Variant>(new Variant(0), null);
        }
        if (Contains(k) && _info.element.onDuplicate != TreeBehaviour.Allow) {
            return new MTResult(this, _info.element.onDuplicate);
        }
        if (_impl == null) {
            return new MTResult(new SMTree(_info, k, v), TreeBehaviour.Allow);
        }
        Variant nv = null;
        SITree st = _impl;
        if (st != null && st.Contains(k.element)) {
            Variant tv = st.Lookup(k.element);
            switch (tv.variant) {
                case Compound: {
                    SMTree mt = (SMTree) tv.ob;
                    mt = (SMTree) mt.Add((SCList<Variant>) k.next, v).t;
                    nv = new Variant(Variants.Compound, mt); // care: immutable
                    break;
                }
                case Partial: {
                    var bt = (SDict<Long, Boolean>) tv.ob;
                    bt = bt.Add(v, true);
                    nv = new Variant(Variants.Partial, bt); // care: immutable
                    break;
                }
                default:
                    throw new Exception("internal error");
            }
            st = _impl.Update(k.element, nv);
        } else {
            switch (st.variant) {
                case Compound:
                    SMTree mt = new SMTree(_info.next, (SCList<Variant>) k.next, v);
                    nv = new Variant(Variants.Compound, mt);
                    break;
                case Single:
                    if (_info.element.onDuplicate != TreeBehaviour.Allow) {
                        nv = new Variant(v);
                        break;
                    } // else fall into
                case Partial:
                    var bt = new SDict<>(v, true);
                    nv = new Variant(Variants.Partial, bt);
                    break;
            }
            st = (SITree) _impl.Add(k.element, nv);
        }
        return new MTResult(new SMTree(_info, st, Length + 1), TreeBehaviour.Allow);
    }

    public SMTree Remove(SCList<Variant> k) {
        if (!Contains(k)) {
            return this;
        }
        SITree st = _impl;
        Variant k0 = k.element;
        Variant tv = _impl.Lookup(k0);
        int nc = Length;
        switch (tv.variant) {
            case Compound: {
                SMTree mt = (SMTree) tv.ob;
                int c = mt.Length;
                mt = (SMTree) mt.Remove((SCList<Variant>) k.next);
                nc -= c - mt.Length;
                if (mt.Length == 0) {
                    st = (SITree) st.Remove(k0);
                } else {
                    st = st.Update(k0, new Variant(Variants.Compound, mt));
                }
                break;
            }
            case Partial: {
                SDict<Integer, Boolean> bt = (SDict<Integer, Boolean>) tv.ob;
                nc -= bt.Length;
                st = (SITree) st.Remove(k0);
                break;
            }
            case Single:
                nc--;
                st = (SITree) st.Remove(k0);
                break;
        }
        try {
            return new SMTree(_info, st, nc);
        } catch (Exception e) {
        }
        return this;
    }

    public SMTree Remove(SCList<Variant> k, long v) {
        if (!Contains(k)) {
            return this;
        }
        SITree st = _impl;
        Variant k0 = k.element;
        Variant tv = _impl.Lookup(k0);
        int nc = Length;
        switch (tv.variant) {
            case Compound: {
                SMTree mt = (SMTree) tv.ob;
                int c = mt.Length;
                mt = (SMTree) mt.Remove((SCList<Variant>) k.next, v);
                nc -= c - mt.Length;
                if (mt.Length == 0) {
                    st = (SITree) st.Remove(k0);
                } else {
                    st = st.Update(k0, new Variant(Variants.Compound, mt));
                }
                break;
            }
            case Partial: {
                var bt = (SDict<Long, Boolean>) tv.ob;
                if (!bt.Contains(v)) {
                    return this;
                }
                nc--;
                bt = bt.Remove(v);
                if (bt.Length == 0) {
                    st = (SITree) st.Remove(k0);
                } else {
                    st = st.Update(k0, new Variant(Variants.Partial, bt));
                }
                break;
            }
            case Single:
                nc--;
                st = (SITree) st.Remove(k0);
                break;
        }
        try {
            return new SMTree(_info, st, nc);
        } catch (Exception e) {
        }
        return this;
    }

    public int compareTo(Object obj) {
        SMTree that = (SMTree) obj;
        if (that == null || that.Length == 0) {
            return 1;
        }
        return First().getValue().key.compareTo(that.First().getValue());
    }

}
