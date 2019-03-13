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
public class SFunction extends Serialisable {
        public final Serialisable arg; // probably an SQuery
        public final byte func;
        static long _fid = 0;
        public final long fid = ++_fid;
        public SFunction(SDatabase db,byte fn,Reader f) throws Exception
        {
            super(Types.SFunction);
            func = fn;
            arg = f._Get(db);
        }
        public SFunction(byte fn, Serialisable a)
        { 
            super(Types.SFunction);
            func = fn;
            arg = a;
        }
        public boolean isValue() { return false;}
        class Func {
            static final byte Sum =0, Count =1, Max=2, Min=3, Null=4;
        }
        static SFunction Get(SDatabase db,Reader f) throws Exception
        {
            return new SFunction(db, (byte)f.ReadByte(), f);
        }
        public void Put(StreamBase f)
        {
            super.Put(f);
            f.WriteByte(func);
            arg.Put(f);
        }
        @Override
        public Serialisable Lookup(Context cx)
        {
            if (cx.ags==null)
                return this;
            var x = arg.Lookup(cx);
            if (func == Func.Null)
                return SBoolean.For(x == Null);
            return cx.defines(fid) ? cx.get(fid) : Null;
        }
        @Override
        public Serialisable StartCounter(Serialisable v)
        {
            switch (func)
            {
                case Func.Count:
                    return SInteger.One;
                case Func.Max:
                case Func.Min:
                case Func.Sum:
                    return v;
            }
            return Null;
        }
        public Serialisable AddIn(Serialisable a,Serialisable v)
        {
            switch (func)
            {
                case Func.Count:
                    if (v == Null)
                        return a;
                    return new SInteger(((SInteger)a).value + 1);
                case Func.Max:
                    return (a.compareTo(v) > 0) ? a : v;
                case Func.Min:
                    return (a.compareTo(v) < 0) ? a : v;
                case Func.Sum:
                    switch (a.type)
                    {
                        case Types.SInteger:
                            {
                                var lv = ((SInteger)a).value;
                                switch (v.type)
                                {
                                    case Types.SInteger:
                                        return new SInteger(lv + ((SInteger)v).value);
                                    case Types.SBigInt:
                                        return new SInteger(new Bigint(lv).Plus(getbig(v)));
                                    case Types.SNumeric:
                                        return new SNumeric(new Numeric(new Bigint(lv), 0).Add(((SNumeric)v).num));
                                }
                                break;
                            }
                        case Types.SBigInt:
                            {
                                var lv = getbig(a);
                                switch (v.type)
                                {
                                    case Types.SInteger:
                                        return new SInteger(lv.Plus(new Bigint(((SInteger)v).value)));
                                    case Types.SBigInt:
                                        return new SInteger(lv.Plus(getbig(v)));
                                    case Types.SNumeric:
                                        return new SNumeric(new Numeric(lv,0).Add(((SNumeric)v).num));
                                }
                                break;
                            }
                        case Types.SNumeric:
                            {
                                var lv = ((SNumeric)a).num;
                                switch (v.type)
                                {
                                    case Types.SInteger:
                                        return new SNumeric(lv.Add(new Numeric(((SInteger)v).value)));
                                    case Types.SBigInt:
                                        return new SNumeric(lv.Add(new Numeric(getbig(v), 0)));
                                    case Types.SNumeric:
                                        return new SNumeric(lv.Add(((SNumeric)v).num));
                                }
                                break;
                            }

                    }
                    break;
            }
            return v;
        }
        Bigint getbig(Serialisable x)
        {
            return ((SInteger)x).big;
        }
        public boolean isAgg() { return func!=Func.Null; }
        @Override
        public SDict<Long, SFunction> Aggregates(SDict<Long, SFunction> a, Context cx)
        {
            return isAgg()? ((a==null)?new SDict(fid, this):a.Add(fid,this)) : a;
        }
    }

