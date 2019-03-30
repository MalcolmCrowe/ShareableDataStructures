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
public class Numeric {
    Bigint mantissa;
    int scale;
    int precision;

    Numeric(Bigint m, int s) {
        mantissa = m;
        scale = s;
        precision = 0;
    }
    
    public Numeric(Bigint m, int s, int p) {
        mantissa = m;
        scale = s;
        precision = p;
    }

    Numeric(long n)
    {
        mantissa = new Bigint(n);
        scale = 0;
        precision = 0;
    }
    public Numeric(Double d)
    {
        Numeric a = Parse(d.toString());
        mantissa = a.mantissa;
        scale = a.scale;
        precision = 0;
    }
    static Numeric Zero = new Numeric(0L);
		Numeric Normalise()
		{
			int n = 0;
			Bigint m = mantissa;
			if (m.bytes.length==0)
				return Zero;
			boolean sg = m.getSign();
			if (sg)
				m = m.Negate();
            for (; ; )
            {
                int r = 0;
                Bigint q = m.Divide(new Bigint(10));
                if (r != 0)
                    break;
                m = q;
                if (m.bytes.length == 0)
                    return Zero;
                n++;
            }
			if (sg)
				m = m.Negate();
			return new Numeric(m,scale-n);
		}
        public static Numeric Ceiling(Numeric x)
        {
            Bigint m = x.mantissa;
            boolean sg = m.getSign();
            if (sg)
                m = m.Negate();
            Bigint d = Bigint.Pow10(x.scale);
            Bigint n = m.Divide(d).Times(d);
            if (sg)
                n = n.Negate();
            if (n.compareTo(x.mantissa)<0)
                n = n.Plus(d);
            return new Numeric(n, x.scale);
        }
        public static Numeric Floor(Numeric x)
        {
            Bigint m = x.mantissa;
            boolean sg = m.getSign();
            if (sg)
                m = m.Negate();
            Bigint d = Bigint.Pow10(x.scale);
            Bigint n = m.Divide(d).Times(d);
            if (sg)
                n = n.Negate();
            if (n.compareTo(x.mantissa)>0)
                n = n.Minus(d);
            return new Numeric(n, x.scale);
        }
        public Numeric Round(int sc)
        {
            if (scale <= sc)
                return Denormalise(sc - scale);
            Bigint m = mantissa;
            boolean sg = m.getSign();
            if (sg)
                m = m.Negate();
            Bigint d = Bigint.Pow10(scale-sc);
            Bigint n = m.Divide(d).Times(d);
            Bigint r = m.Minus(n);
            if (r.Times((byte)2).compareTo(d)>=0)
                n = n.Plus(d);
            if (sg)
                n = n.Negate();
            return new Numeric(n, scale);
        }
        /// <summary>
        /// Creator: Add trailing 0s to the mantissa by adjusting the scale
        /// </summary>
        /// <param name="n">The number of places to shift</param>
        /// <returns>The new Decimal</returns>
        Numeric Denormalise(int n)
        {
            if (n <= 0)
                return this;
            Bigint m = mantissa;
            if (m.bytes.length != 0)
                for (int j = 0; j < n; j++)
                    m = m.Times10();
            return new Numeric(m, scale + n);
        }


		public static Numeric Add(Numeric a,Numeric b)
		{
			int na = a.scale, nb = b.scale;
			a = a.Denormalise(nb-na);
			b = b.Denormalise(na-nb);
			return new Numeric(a.mantissa.Plus(b.mantissa),a.scale).Normalise();
		}
        public Numeric Add(Numeric b) { return Add(this,b); }
        public Numeric Negate()
        {
            return new Numeric(mantissa.Negate(), scale);
        }
        public Numeric Minus(Numeric b) { return Add(this,b.Negate()); }
    static Numeric Divide(Numeric a, Numeric b, int prec) {
        // we want at least prec bytes in the result, i.e. len(a)-len(b)>prec+1
        if (prec == 0) {
            prec = 13; // if no precision specified
        }
        a = a.Denormalise(b.mantissa.bytes.length - a.mantissa.bytes.length + prec + 1);
        return new Numeric(a.mantissa.Divide(b.mantissa), a.scale - b.scale).Normalise();
    }
    Numeric Times(Numeric b)
    {
        return new Numeric(mantissa.Times(b.mantissa),scale+b.scale).Normalise();
    }
    Numeric Divide(Numeric b)
    {
        return Divide(this,b,4);
    }
    public int compareTo(Object obj)
    {
        Numeric a = this;
        Numeric b;
        if (obj == null) {
            return 1;
        }
        if (obj instanceof Integer) {
            b = new Numeric((long) (int) obj);
        } else if (obj instanceof Numeric) {
            b = (Numeric) obj;
        } else if (obj instanceof Double) {
            b = new Numeric((double) obj);
        } else if (obj instanceof Long) {
            b = new Numeric((long) obj);
        } else if (obj instanceof Bigint) {
            b = new Numeric((Bigint) obj, 0);
        } else {
            return -1;
        }
        int na = a.scale, nb = b.scale;
        a = Denormalise(nb - na);
        b = b.Denormalise(na - nb);
        return a.mantissa.compareTo(b.mantissa);
    }
    public double toDouble()
    {
        return mantissa.toDouble()*Math.pow(10.0,-(double)scale);
    }
    @Override
    public String toString() {
        if (mantissa == null) {
            return "";
        }
        String m = mantissa.toString();
        int n = m.length();
        if (scale == 0) {
            return m;
        }
        if (scale < 0) {
            for (int j = scale; j < 0; j++) {
                m += '0';
            }
            return m;
        }
        if (m.charAt(0) == '-' && scale > n - 1) {
            var pre = "-0.";
            for (int j = scale; j > n - 1; j--) {
                pre += '0';
            }
            return pre + m.substring(1);
        }
        if (scale >= n) {
            var pre = "0.";
            for (int j = scale; j > n; j--) {
                pre += '0';
            }
            return pre + m;
        }
        return m.substring(0, n - scale) + "." + m.substring(n - scale);
    }
    public String doubleFormat() {
        String m = mantissa.toString();
        int n = m.length();
        if (n == 1) {
            return "" + m.substring(0, 1) + ".E" + (n - 1 - scale);
        }
        return m.substring(0, 1) + "." + m.substring(1) + "E" + (n - 1 - scale);
    }
    public static Numeric Parse(String s) {
        int m = s.indexOf('e');
        if (m < 0) {
            m = s.indexOf('E');
        }
        int exp = 0;
        if (m > 0) {
            exp = Integer.parseInt(s.substring(m + 1));
            s = s.substring(0, m);
        }
        int n = s.indexOf('.');
        if (n < 0) {
            return new Numeric(Bigint.Parse(s), -exp);
        }
        int ln = s.length();
        return new Numeric(Bigint.Parse(s.substring(0, n) + s.substring(n + 1)), ln - n - 1 - exp);
    }
 
}
