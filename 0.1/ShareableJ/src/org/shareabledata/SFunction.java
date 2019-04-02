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
        public final long fid = --SysTable._uid;
        public SFunction(byte fn,Reader f) throws Exception
        {
            super(Types.SFunction);
            func = fn;
            arg = f._Get();
        }
        public SFunction(byte fn, Serialisable a)
        { 
            super(Types.SFunction);
            func = fn;
            arg = a;
        }
        @Override
        public boolean isValue() { return false;}
        public static class Func {
            public static final byte Sum =0, Count =1, Max=2, Min=3, Null=4,
                    NotNull=5,Constraint=6,Default=7,Generated=8;
            public static String names[] = { "Sum","Count","Max","Min","Null",
            "NotNull","Constraint","Default","Generated"};
        }
        @Override
        public Serialisable Fix(AStream f)
        {
            return new SFunction(func,arg.Fix(f));
        }
        public static SFunction Get(Reader f) throws Exception
        {
            return new SFunction((byte)f.ReadByte(), f);
        }
        @Override
        public Serialisable UseAliases(SDatabase db, SDict<Long, Long> ta)
        {
            return new SFunction(func,arg.UseAliases(db, ta));
        }
        @Override
        public Serialisable Prepare(STransaction tr,SDict<Long,Long> pt) throws Exception
        {
            return new SFunction(func,arg.Prepare(tr,pt));
        }
        @Override
        public void Put(StreamBase f)
        {
            super.Put(f);
            f.WriteByte(func);
            arg.Put(f);
        }
        public boolean isAgg() { return (func!=Func.Null);}
        @Override
        public Serialisable Lookup(Context cx)
        {
            if (cx.refs==null)
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
                    if ((!v.isValue()) || v==Null)
                        return SInteger.Zero;
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
                    if ((!v.isValue()) || v == Null)
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
        @Override
        public SDict<Long, Serialisable> Aggregates(SDict<Long, Serialisable> a)
        {
            return isAgg()? ((a==null)?new SDict(fid, this):a.Add(fid,this)) : a;
        }
        @Override
        public String toString()
        {
            return new Func().names[func] + "(" + arg + ")";
        }
    }

