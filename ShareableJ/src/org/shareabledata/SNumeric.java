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
public class SNumeric extends Serialisable implements Comparable {
        public final Numeric num;
        public SNumeric(Numeric n)
        {
            super(Types.SNumeric);
            num = n;
        }
        SNumeric(ReaderBase f) throws Exception
        {
            super(Types.SNumeric);
            var mantissa = f.GetInteger();
            var precision = f.GetInt();
            var scale = f.GetInt();
            num = new Numeric(mantissa, scale, precision);
        }
        public void Put(WriterBase f) throws Exception
        {
            super.Put(f);
            f.PutInteger(num.mantissa);
            f.PutInt(num.precision);
            f.PutInt(num.scale);
        }
        public static Serialisable Get(ReaderBase f) throws Exception
        {
            return new SNumeric(f);
        }
        @Override
        public int compareTo(Object o) {
            if (o==Null)
                return 1;
            if (o instanceof SRow)
            {
                var sr = (SRow)o;
                if (sr.cols.Length==1)
                    return compareTo(sr.vals.First().getValue().val);
            }
            if (o instanceof SInteger)
            {
                var si = (SInteger)o;
                if (si.big==null)
                    return num.compareTo(new Numeric(si.value));
                else
                    return num.compareTo(new Numeric(si.big,0));
            }
            return num.compareTo(((SNumeric)o).num);
        }
        @Override
        public void Append(SDatabase db,StringBuilder sb)
        {
           sb.append(num.toDouble());
        }
        @Override
        public String toString()
        {
            return "Numeric " + num.toString();
        }
}
