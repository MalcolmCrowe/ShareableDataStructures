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
public class SExpression extends Serialisable {
        public final Serialisable left, right;
        public final int op; // see Op
        public SExpression(SDatabase db,Serialisable lf,int o,Reader f)
                throws Exception
        {
            super(Types.SExpression);
            left = lf;
            op = o;
            right = f._Get(db);
        }
        public SExpression(Serialisable lf,int o,Serialisable rt)
        {
            super(Types.SExpression);
            left = lf; right = rt; op = o;
        }
        public  boolean isValue() {return false; }
        public class Op 
        { 
            public static final int 
            Plus =0,Minus =1,Times=2,Divide=3,Eql=4,NotEql=5,Lss=6, 
            Leq=7, Gtr=8, Geq=9, Dot=10, And=11, Or=12, UMinus=13, Not=14;
        }
        static SExpression Get(SDatabase db,Reader f) throws Exception
        {
            var lf = f._Get(db);
            return new SExpression(db, lf, f.ReadByte(), f);
        }
        public void Put(StreamBase f)
        {
            super.Put(f);
            left.Put(f);
            f.WriteByte((byte)op);
            right.Put(f);
        }
        public Serialisable Lookup(Context nms)
        {
            if (op == Op.Dot)
            {
                if (nms.head instanceof RowBookmark)
                {
                    var rb = (RowBookmark)nms.head;
                    var ls = ((SString)left).str;
                    var rs = ((SString)right).str;
                    var n = ls + "." +rs;
                    if (rb._ob.vals.Contains(n))
                        return rb._ob.vals.Lookup(n);
                    if (nms==null || !nms.defines(ls))
                        return this;
                    if (ls.compareTo(rb._rs._qry.getAlias()) == 0)
                        return (nms!=null && nms.defines(rs))?nms.get(rs):this;
                    var rw = (SRow)rb._ob.get(ls);
                    return (rw!=null && rw.defines(rs))?rw.get(rs).Lookup(nms):Null;
                }
                return this;
            }
            var lf = left.Lookup(nms);
            var rg = right.Lookup(nms);
            if (!(lf.isValue() && rg.isValue()))
                return new SExpression(lf, op, rg);
            switch (op)
            {
                 case Op.Plus:
                    {
                        switch (lf.type)
                        {
                            case Types.SInteger:
                                {
                                    var lv = ((SInteger)lf).value;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv + ((SInteger)rg).value);
                                        case Types.SBigInt:
                                            return new SInteger(new Bigint(lv).Add(getbig(rg),0));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(new Bigint(lv), 0).Add(((SNumeric)rg).num));
                                    }
                                    break;
                                }
                            case Types.SBigInt:
                                {
                                    var lv = getbig(lf);
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv.Add(new Bigint(((SInteger)rg).value),0));
                                        case Types.SBigInt:
                                            return new SInteger(lv.Add(getbig(rg),0));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(lv, 0).Add(((SNumeric)rg).num));
                                    }
                                    break;
                                }
                            case Types.SNumeric:
                                {
                                    var lv = ((SNumeric)lf).num;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SNumeric(lv.Add(new Numeric(((SInteger)rg).value)));
                                        case Types.SBigInt:
                                            return new SNumeric(lv.Add(new Numeric(getbig(rg), 0)));
                                        case Types.SNumeric:
                                            return new SNumeric(lv.Add(((SNumeric)rg).num));
                                    }
                                    break;
                                }
                        }
                    }
                    break;
                case Op.Minus:
                    {
                        switch (lf.type)
                        {
                            case Types.SInteger:
                                {
                                    var lv = ((SInteger)lf).value;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv - ((SInteger)rg).value);
                                        case Types.SBigInt:
                                            return new SInteger(new Bigint(lv).Minus(getbig(rg)));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(new Bigint(lv), 0).Minus(((SNumeric)rg).num));
                                    }
                                    break;
                                }
                            case Types.SBigInt:
                                {
                                    var lv = getbig(lf);
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv.Minus(new Bigint(((SInteger)rg).value)));
                                        case Types.SBigInt:
                                            return new SInteger(lv.Minus(getbig(rg)));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(lv, 0).Minus(((SNumeric)rg).num));
                                    }
                                    break;
                                }
                            case Types.SNumeric:
                                {
                                    var lv = ((SNumeric)lf).num;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SNumeric(lv.Minus(new Numeric(((SInteger)rg).value)));
                                        case Types.SBigInt:
                                            return new SNumeric(lv.Minus(new Numeric(getbig(rg), 0)));
                                        case Types.SNumeric:
                                            return new SNumeric(lv.Minus(((SNumeric)rg).num));
                                    }
                                    break;
                                }
                        }
                    }
                    break;
                case Op.Times:
                    {
                        switch (lf.type)
                        {
                            case Types.SInteger:
                                {
                                    var lv = ((SInteger)lf).value;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv * ((SInteger)rg).value);
                                        case Types.SBigInt:
                                            return new SInteger(new Bigint(lv).Times(getbig(rg)));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(new Bigint(lv), 0).Times(((SNumeric)rg).num));
                                    }
                                    break;
                                }
                            case Types.SBigInt:
                                {
                                    var lv = getbig(lf);
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv.Times(new Bigint(((SInteger)rg).value)));
                                        case Types.SBigInt:
                                            return new SInteger(lv.Times(getbig(rg)));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(lv, 0).Times(((SNumeric)rg).num));
                                    }
                                    break;
                                }
                            case Types.SNumeric:
                                {
                                    var lv = ((SNumeric)lf).num;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SNumeric(lv.Times(new Numeric(((SInteger)rg).value)));
                                        case Types.SBigInt:
                                            return new SNumeric(lv.Times(new Numeric(getbig(rg), 0)));
                                        case Types.SNumeric:
                                            return new SNumeric(lv.Times(((SNumeric)rg).num));
                                    }
                                    break;
                                }
                        }
                    }
                    break;
                case Op.Divide:
                    {
                        switch (lf.type)
                        {
                            case Types.SInteger:
                                {
                                    var lv = ((SInteger)lf).value;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv / ((SInteger)rg).value);
                                        case Types.SBigInt:
                                            return new SInteger(new Bigint(lv).Divide(getbig(rg)));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(new Bigint(lv), 0).Divide(((SNumeric)rg).num));
                                    }
                                    break;
                                }
                            case Types.SBigInt:
                                {
                                    var lv = getbig(lf);
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SInteger(lv.Divide(new Bigint(((SInteger)rg).value)));
                                        case Types.SBigInt:
                                            return new SInteger(lv.Divide(getbig(rg)));
                                        case Types.SNumeric:
                                            return new SNumeric(new Numeric(lv, 0).Divide(((SNumeric)rg).num));
                                    }
                                    break;
                                }
                            case Types.SNumeric:
                                {
                                    var lv = ((SNumeric)lf).num;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            return new SNumeric(lv.Divide(new Numeric(((SInteger)rg).value)));
                                        case Types.SBigInt:
                                            return new SNumeric(lv.Divide(new Numeric(getbig(rg), 0)));
                                        case Types.SNumeric:
                                            return new SNumeric(lv.Divide(((SNumeric)rg).num));
                                    }
                                    break;
                                }
                        }
                    }
                    break;
                case Op.Eql:
                case Op.NotEql:
                case Op.Gtr:
                case Op.Geq:
                case Op.Leq:
                case Op.Lss:
                    {
                        int c = 0;
                        switch (lf.type)
                        {
                            case Types.SInteger:
                                {
                                    var lv = ((SInteger)lf).value;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            c = compare(lv,((SInteger)rg).value); break;
                                        case Types.SBigInt:
                                            c = new Bigint(lv).compareTo(getbig(rg)); break;
                                        case Types.SNumeric:
                                            c = new Numeric(new Bigint(lv), 0).compareTo(((SNumeric)rg).num); break;
                                    }
                                    break;
                                }
                            case Types.SBigInt:
                                {
                                    var lv = getbig(lf);
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            c = lv.compareTo(((SInteger)rg).value); break;
                                        case Types.SBigInt:
                                            c = lv.compareTo(getbig(rg)); break;
                                        case Types.SNumeric:
                                            c = new Numeric(lv, 0).compareTo(((SNumeric)rg).num); break;
                                    }
                                    break;
                                }
                            case Types.SNumeric:
                                {
                                    var lv = ((SNumeric)lf).num;
                                    switch (rg.type)
                                    {
                                        case Types.SInteger:
                                            c = lv.compareTo(new Numeric(((SInteger)rg).value)); break;
                                        case Types.SBigInt:
                                            c = lv.compareTo(new Numeric(getbig(rg), 0)); break;
                                        case Types.SNumeric:
                                            c = lv.compareTo(((SNumeric)rg).num); break;
                                    }
                                    break;
                                }
                        }
                        boolean r = true;
                        switch (op)
                        {
                            case Op.Eql: r = c == 0; break;
                            case Op.NotEql: r = c != 0; break;
                            case Op.Leq: r = c <= 0; break;
                            case Op.Lss: r = c < 0; break;
                            case Op.Geq: r = c >= 0; break;
                            case Op.Gtr: r = c > 0; break;
                        }
                        return SBoolean.For(r);
                    }
                case Op.UMinus:
                    switch (left.type)
                    {
                        case Types.SInteger: return new SInteger(-((SInteger)left).value);
                        case Types.SBigInt: return new SInteger(getbig(lf).Negate());
                        case Types.SNumeric: return new SNumeric(((SNumeric)left).num.Negate());
                    }
                    break;
                case Op.Not:
                    {
                        if (lf instanceof SBoolean)
                                return For(!((SBoolean)lf).sbool);
                        break;
                    }
                case Op.And:
                    {
                        if (lf instanceof SBoolean && rg instanceof SBoolean)
                            return For(((SBoolean)lf).sbool && ((SBoolean)rg).sbool);
                        break;
                    }
                case Op.Or:
                    {
                        if (lf instanceof SBoolean && rg instanceof SBoolean)
                            return For(((SBoolean)lf).sbool|| ((SBoolean)rg).sbool);
                        break;
                    }
                case Op.Dot:
                    {
                        var ls = ((SString)left).str;
                        var a = (nms!=null && nms.defines(ls)) ? nms.get(ls)  : left;
                        return a.Lookup(nms);
                    }
            }
            return Null;
        }
        int compare(int a,int b)
        {
            return (a==b)?0:(a<b)?-1:1;
        }
        Bigint getbig(Serialisable x)
        {
            return ((SInteger)x).big;
        }
        SBoolean For(boolean v)
        {
            return v ? SBoolean.True : SBoolean.False;
        }
        public SDict<Long,SFunction> Aggregates(SDict<Long,SFunction> ags,Context cx)
        {
            if (left != null)
                ags = left.Aggregates(ags, cx);
            if (right != null)
                ags = right.Aggregates(ags, cx);
            return ags;
        }
}
