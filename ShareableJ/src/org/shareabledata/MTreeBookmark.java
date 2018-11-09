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
    public class MTreeBookmark<K extends Comparable> extends Bookmark<SSlot<SCList<Variant>, Variant>>
    {
        final SDictBookmark<Variant,Variant> _outer;
        final SList<TreeInfo> _info;
        final MTreeBookmark _inner;
        final Bookmark<SSlot<Integer, Boolean>> _pmk;
        final boolean _changed;
        SCList<Variant> _filter;
        MTreeBookmark(SDictBookmark<Variant, Variant> outer, SList<TreeInfo> info,
            boolean changed, MTreeBookmark inner,Bookmark<SSlot<Integer, Boolean>> pmk, 
            int pos, SCList<Variant> key) 
        {
            super(pos);
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
            for (var outer = (SDictBookmark<Variant, Variant>)((mt._impl==null)?null:mt._impl.First());
                    outer!=null;
                    outer=(SDictBookmark<Variant, Variant>)outer.Next())
            {
                Variant ov = outer.getValue().val;
                switch (ov.variant)
                {
                    case Compound:
                    {
                        MTreeBookmark inner = (MTreeBookmark)((SMTree)ov.ob).First();
                        if (inner==null)
                            return null;
                        return new MTreeBookmark(outer, mt._info, false, inner,null, 0,null);
                    }
                    case Partial:
                        SDictBookmark<Integer,Boolean> pmk = (SDictBookmark<Integer,Boolean>)((SDict<Integer,Boolean>)ov.ob).First();
                        if (pmk==null)
                            return null;
                        return new MTreeBookmark(outer, mt._info, false, null, pmk, 0,null);
                    case Single:
                        return new MTreeBookmark(outer, mt._info, false, null, null, 0,null);
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
            if (mt._impl==null)
                return null;
            SDictBookmark<Variant,Variant> outer = (SDictBookmark<Variant,Variant>)(mt._impl.PositionAt(key.element));
            if (outer == null)
                return null;
            Variant ov = outer.getValue().val;
            switch (ov.variant)
            {
                case Compound:
                    MTreeBookmark inner = ((SMTree)ov.ob).PositionAt((SCList<Variant>)key.next);
                    if (inner==null)
                        return null;
                    return new MTreeBookmark(outer, mt._info, true, inner, null, 0, key);
                case Partial:
                    Bookmark<SSlot<Integer, Boolean>> pmk = ((SDict<Integer,Boolean>)ov.ob).First();
                    if (pmk==null)
                        return null;
                    return new MTreeBookmark(outer, mt._info, true, null, pmk, 0, key);
                case Single:
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
            SCList<Variant> ink = null;
            if (_inner !=null)
                ink = _inner.key();
            return new SCList<Variant>(_outer.getValue().key, ink);
        }
        public SSlot<SCList<Variant>, Variant> getValue()
        {
            try {
                return new SSlot<SCList<Variant>,Variant>(key(),new Variant(value()));
            } catch (Exception e) {}
            return null;
        }
        public int value()
        {
            return (_inner!=null)?_inner.value() : (_pmk!=null)?_pmk.getValue().key : 
                (_outer.getValue().val!=null)?(int)_outer.getValue().val.ob : 0;
        }
        public Bookmark<SSlot<SCList<Variant>, Variant>> Next()
        {
            SDictBookmark<Variant,Variant> outer = _outer;
            MTreeBookmark inner = _inner;
            Bookmark<SSlot<Integer, Boolean>> pmk = _pmk;
            boolean changed = false;
            int pos = Position;
            for (boolean done = false;!done ; )
            {
                if (inner != null)
                {
                    inner = (MTreeBookmark)inner.Next();
                    if (inner != null)
                        break;
                }
                if (pmk != null)
                {
                    pmk = pmk.Next();
                    if (pmk != null)
                        break;
                }
                Variant h = (_filter==null)?null:_filter.element;
                if (h != null)
                    return null;
                outer = (SDictBookmark<Variant,Variant>)outer.Next();
                if (outer == null)
                    return null;
                changed = true;
                Variant oval = outer.getValue().val;
                switch (oval.variant)
                {
                    case Compound:
                        SCList<Variant> vn = (SCList<Variant>)((_filter==null)?null:_filter.next);
                        inner = ((SMTree)oval.ob).PositionAt(vn);
                        if (inner != null)
                            done = true;
                        break;
                    case Partial:
                        pmk = ((SDict<Integer, Boolean>)oval.ob).First();
                        if (pmk != null)
                            done = true;
                        break;
                    case Single:
                        done = true;
                }
            }
            return new MTreeBookmark(outer, _info, changed, inner, pmk, pos + 1, _filter);

        }
    }
