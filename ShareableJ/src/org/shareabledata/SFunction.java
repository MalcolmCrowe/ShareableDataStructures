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
        public Serialisable Lookup(ILookup<String,Serialisable> nms)
        {
            var x = arg.Lookup(nms);
            if (!x.isValue() || !(nms instanceof RowBookmark))
                return new SFunction(func,x);
            if (func == Func.Null)
                return SBoolean.For(x == Serialisable.Null);
            var t = Types.Serialisable;
            var empty = true;
            var rb = (RowBookmark)nms;
            Bigint ai = Bigint.Zero;
            Numeric an = Numeric.Zero;
            String ac = "";
            int ic = 0;
            for (var b = (RowBookmark)rb._rs.First(); b != null; 
                    b = (RowBookmark)b.Next())
                if (b.SameGroupAs(rb))
                {
                    var a = arg.Lookup(b);
                    t = a.type;
                    switch (func)
                    {
                        case Func.Count:
                            if (a!=Serialisable.Null)
                                ic++;
                            break;
                        case Func.Sum:
                            switch (t)
                            {
                                case Types.SInteger:
                                    var xi = new Bigint(((SInteger)a).value);
                                    ai = ai.Add(xi,0); break;
                                case Types.SBigInt:
                                    var xb = ((SInteger)a).big;
                                    ai = ai.Add(xb, 0); break;
                                case Types.SNumeric:
                                    var xn = ((SNumeric)a).num;
                                    an = an.Add(xn); break;
                            }
                            break;
                        case Func.Max:
                            switch (t)
                            {
                                case Types.SInteger:
                                    var xi = new Bigint(((SInteger)a).value);
                                    ai = ((empty || xi.compareTo(ai)>0) ? xi : ai);
                                    break;
                                case Types.SBigInt:
                                    var xb = ((SInteger)a).big;
                                    ai = ((empty || xb.compareTo(ai)>0) ? xb : ai);
                                    break;
                                case Types.SNumeric:
                                    var xn = ((SNumeric)a).num;
                                    an = (empty || xn.compareTo(an)>0) ? xn : an;
                                    break;
                                case Types.SString:
                                    var xc = ((SString)a).str;
                                    ac = (empty || xc.compareTo(ac) > 0)? xc : ac;
                                    break;
                            }
                            empty = false;
                            break;
                        case Func.Min:
                            switch (t)
                            {
                                case Types.SInteger:
                                    var xi = new Bigint(((SInteger)a).value);
                                    ai = ((empty || xi.compareTo(ai)<0) ? xi : ai);
                                    break;
                                case Types.SBigInt:
                                    var xb = ((SInteger)a).big;
                                    ai = (empty || xb.compareTo(ai)<0) ? xb : ai;
                                    break;
                                case Types.SNumeric:
                                    var xn = ((SNumeric)a).num;
                                    an = (empty || xn.compareTo(an)<0) ? xn : an;
                                    break;
                                case Types.SString:
                                    var xc = ((SString)a).str;
                                    ac = (empty || xc.compareTo(ac) < 0) ? xc : ac;
                                    break;
                            }
                            empty = false;
                            break;
                    }
                }
            if (func == Func.Count)
                return new SInteger(ic);
            switch(t)
            {
                case Types.SInteger: 
                case Types.SBigInt: return new SInteger(ai);
                case Types.SNumeric: return new SNumeric(an);
                case Types.SString: return new SString(ac);
                default: return Serialisable.Null;
            }
        }
    }

