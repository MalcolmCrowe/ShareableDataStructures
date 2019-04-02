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
public class SInPredicate extends Serialisable {
        public final Serialisable arg;
        public final Serialisable list;
        public SInPredicate(Serialisable a,Serialisable r)
        {
            super(Types.SInPredicate);
            arg = a; list = r;
        }
        @Override
        public boolean isValue() { return false;}
        public static SInPredicate Get(Reader f) throws Exception
        {
            var a = f._Get();
            return new SInPredicate(a, f._Get());
        }
        public Serialisable Prepare(STransaction db, SDict<Long, Long> pt)
                throws Exception
        {
            var a = arg.Prepare(db,pt);
            if (list instanceof SQuery)
                pt = ((SQuery)list).Names(db, pt);
            return new SInPredicate(a,list.Prepare(db,pt));
        }
        @Override
        public void Put(StreamBase f)
        {
            super.Put(f);
            arg.Put(f);
            list.Put(f);
        }
        @Override
        public Serialisable Lookup(Context cx)
        {
            var a = arg.Lookup(cx);
            var ls = list.Lookup(cx);
            switch(list.type)
            {
                case Types.SValues:
                    for (var b = ((SValues)ls).vals.First(); b != null; b = b.Next())
                        if (b.getValue().compareTo(a) == 0)
                            return SBoolean.True;
                    break;
                case Types.SRow:
                    for (var b = ((SRow)ls).cols.First(); b != null; b = b.Next())
                        if (b.getValue().val.compareTo(a) == 0)
                            return SBoolean.True;
                    break;
                case Types.SSelect:
                    try {
                        var ss = (SSelectStatement)list;
                        var tr = cx.Transaction();
                        for (var b = ss.RowSet(tr,ss,null).First(); 
                                b != null; b = b.Next())
                            if (b.getValue().compareTo(a) == 0)
                                return SBoolean.True;
                    } catch(Exception e) {
                        return SBoolean.False;
                    }
                    break;
            }
            return SBoolean.False;
        }
}
