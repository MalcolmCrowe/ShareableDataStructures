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
public class SExpression extends SDbObject {
        public final Serialisable left, right;
        public final int op; // see Op
        public SExpression(Serialisable lf,int o,ReaderBase f)
                throws Exception
        {
            super(Types.SExpression);
            left = lf;
            op = o;
            right = f._Get();
        }
        public SExpression(Serialisable lf,int o,Serialisable rt)
        {
            super(Types.SExpression);
            left = lf; right = rt; op = o;
        }
        @Override
        public  boolean isValue() {return false; }
        public class Op 
        { 
            public static final int 
            Plus =0,Minus =1,Times=2,Divide=3,Eql=4,NotEql=5,Lss=6, 
            Leq=7, Gtr=8, Geq=9, Dot=10, And=11, Or=12, UMinus=13, Not=14;
        }
        public static SExpression Get(ReaderBase f) throws Exception
        {
            var u = f.GetLong();
            var lf = f._Get();
            return new SExpression(lf, f.ReadByte(), f);
        }
        @Override
        public Serialisable Prepare(STransaction db,SDict<Long,Long> pt)throws Exception
        {
            var lf = left.Prepare(db, pt);
            if (op == Op.Dot && lf instanceof SDbObject)
            {
                var qq = db.objects.get(((SDbObject)lf).uid);
                if (qq instanceof SQuery)
                pt = ((SQuery)qq).Names(db,pt);
            }
            return new SExpression(lf, op, right.Prepare(db, pt));          
        }
        @Override
        public Serialisable UseAliases(SDatabase db, SDict<Long, Long> ta)
        {
            if (op == Op.Dot)
                return new SExpression(left.UseAliases(db,ta), op, right);
            return new SExpression(left.UseAliases(db, ta),op,right.UseAliases(db,ta));
        }
        @Override
        public Serialisable UpdateAliases(SDict<Long,String> uids)
        {
            var lf = left.UpdateAliases(uids);
            var rg = right.UpdateAliases(uids);
            return (lf == left && rg == right) ?
                this : new SExpression(lf, op, rg);            
        }
        @Override
        public void Put(WriterBase f) throws Exception
        {
            super.Put(f);
            left.Put(f);
            f.WriteByte((byte)op);
            right.Put(f);
        }
        @Override
        public Serialisable Fix(Writer f)
        {
            return new SExpression(left.Fix(f),op,right.Fix(f));            
        }
        @Override
        public Serialisable Lookup(STransaction tr,Context cx) 
        {
            var lf = left.Lookup(tr,cx);
            if (op == Op.Dot && right instanceof SDbObject)
            {
                SDbObject rn = (SDbObject)right;
                if (lf instanceof SRow)
                {
                    var rw = (SRow)lf;
                    if (rw.defines(rn.uid))
                        return rw.get(rn.uid);
                }
                if (lf instanceof SDbObject)
                {
                    var ln = (SDbObject)lf;
                    if (cx.defines(ln.uid))
                    {
                        var t = cx.get(ln.uid);
                        if (t instanceof SRow)
                        {
                            var sr = (SRow)t;
                            if (sr.defines(rn.uid))
                                return sr.get(rn.uid);
                        }
                    }
                }
                return this;
            }
            var rg = right.Lookup(tr,cx);
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
                case Op.Eql: return SBoolean.For(lf.compareTo(rg) == 0); 
                case Op.NotEql: return SBoolean.For(lf.compareTo(rg) != 0);
                case Op.Leq: return SBoolean.For(lf.compareTo(rg) <= 0);
                case Op.Lss: return SBoolean.For(lf.compareTo(rg) < 0);
                case Op.Geq: return SBoolean.For(lf.compareTo(rg) >= 0);
                case Op.Gtr: return SBoolean.For(lf.compareTo(rg) > 0);
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
                        var ls = (ILookup<Long,Serialisable>)left.Lookup(tr,cx);
                        if (ls!=null)
                            return ls.get(((SDbObject)right).uid);
                        break;
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
        public SDict<Long,Serialisable> Aggregates(SDict<Long,Serialisable> ags)
        {
            if (left != null)
                ags = left.Aggregates(ags);
            if (right != null)
                ags = right.Aggregates(ags);
            return ags;
        }
}
