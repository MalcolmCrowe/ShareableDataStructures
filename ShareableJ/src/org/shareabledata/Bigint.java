/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.util.*;

/**
 *
 * @author Malcolm
 */
public class Bigint implements Comparable {

    public final byte[] bytes;
    public static Bigint Zero = new Bigint(new byte[0]);
    public static Bigint One = new Bigint(1);
    public static Bigint intMax = new Bigint(Integer.MAX_VALUE);
    public static Bigint intMin = new Bigint(Integer.MIN_VALUE);
    public static Bigint longMax = new Bigint(Long.MAX_VALUE);
    public static Bigint longMin = new Bigint(Long.MIN_VALUE);    
    static byte[][] pow10 = new byte[][]{
        new byte[]{1},
        new byte[]{10},
        new byte[]{100},
        new byte[]{3, -24},
        new byte[]{39, 16},
        new byte[]{1, -122, -96},
        new byte[]{15, 66, 64},
        new byte[]{0, -104, -106, -128},
        new byte[]{5, -11, -31, 0},
        new byte[]{59, -102, -54, 0},
        new byte[]{2, 84, 11, -28, 0},
        new byte[]{23, 72, 118, -24, 0},
        new byte[]{0, -24, -44, -91, 16, 0},
        new byte[]{9, 24, 78, 114, -96, 0},
        new byte[]{90, -13, 16, 122, 64, 0},
        new byte[]{3, -115, 126, -92, -58, -128, 0},
        new byte[]{35, -122, -14, 111, -63, 0, 0},
        new byte[]{1, 99, 69, 120, 93, -118, 0, 0},
        new byte[]{13, -32, -74, -77, -89, 100, 0, 0},
        new byte[]{0, -118, -57, 35, 4, -119, -24, 0, 0},
        new byte[]{5, 107, -57, 94, 45, 99, 16, 0, 0},
        new byte[]{54, 53, -55, -83, -59, -34, -96, 0, 0},
        new byte[]{2, 30, 25, -32, -55, -70, -78, 64, 0, 0},
        new byte[]{21, 45, 2, -57, -31, 74, -10, -128, 0, 0},
        new byte[]{0, -45, -62, 27, -50, -52, -19, -95, 0, 0, 0},
        new byte[]{8, 69, -107, 22, 20, 1, 72, 74, 0, 0, 0},
        new byte[]{82, -73, -46, -36, -56, 12, -46, -28, 0, 0, 0},
        new byte[]{3, 59, 46, 60, -97, -48, -128, 60, -24, 0, 0, 0},
        new byte[]{32, 79, -50, 94, 62, 37, 2, 97, 16, 0, 0, 0},
        new byte[]{1, 67, 30, 15, -82, 109, 114, 23, -54, -96, 0, 0, 0},
        new byte[]{12, -97, 44, -100, -48, 70, 116, -19, -22, 64, 0, 0, 0},
        new byte[]{126, 55, -66, 32, 34, -64, -111, 75, 38, -128, 0, 0, 0},
        new byte[]{4, -18, 45, 109, 65, 91, -123, -84, -17, -127, 0, 0, 0, 0},
        new byte[]{49, 77, -58, 68, -115, -109, 56, -63, 91, 10, 0, 0, 0, 0},
        new byte[]{1, -19, 9, -66, -83, -121, -64, 55, -115, -114, 100, 0, 0, 0, 0},
        new byte[]{19, 66, 97, 114, -57, 77, -126, 43, -121, -113, -24, 0, 0, 0, 0},
        new byte[]{0, -64, -105, -50, 123, -55, 7, 21, -77, 75, -97, 16, 0, 0, 0, 0},
        new byte[]{7, -123, -18, 16, -43, -38, 70, -39, 0, -12, 54, -96, 0, 0, 0, 0},
        new byte[]{75, 59, 76, -88, 90, -122, -60, 122, 9, -118, 34, 64, 0, 0, 0, 0},
        new byte[]{2, -16, 80, -2, -109, -119, 67, -84, -60, 95, 101, 86, -128, 0, 0, 0, 0},
        new byte[]{29, 99, 41, -15, -61, 92, -92, -65, -85, -71, -11, 97, 0, 0, 0, 0, 0},
        new byte[]{1, 37, -33, -93, 113, -95, -98, 111, 124, -75, 67, -107, -54, 0, 0, 0, 0, 0},
        new byte[]{11, 122, -68, 98, 112, 80, 48, 90, -33, 20, -93, -39, -28, 0, 0, 0, 0, 0},
        new byte[]{114, -53, 91, -40, 99, 33, -29, -116, -74, -50, 102, -126, -24, 0, 0, 0, 0, 0},
        new byte[]{4, 123, -15, -106, 115, -33, 82, -29, 127, 36, 16, 1, 29, 16, 0, 0, 0, 0, 0},
        new byte[]{44, -41, 111, -32, -122, -71, 60, -30, -9, 104, -96, 11, 34, -96, 0, 0, 0, 0, 0},
        new byte[]{1, -64, 106, 94, -59, 67, 60, 96, -35, -86, 22, 64, 111, 90, 64, 0, 0, 0, 0, 0},
        new byte[]{17, -124, 39, -77, -76, -96, 91, -56, -88, -92, -34, -124, 89, -122, -128, 0, 0, 0, 0, 0},
        new byte[]{0, -81, 41, -115, 5, 14, 67, -107, -42, -106, 112, -79, 43, 127, 65, 0, 0, 0, 0, 0, 0},
        new byte[]{6, -41, -97, -126, 50, -114, -93, -38, 97, -32, 102, -21, -78, -8, -118, 0, 0, 0, 0, 0, 0},
        new byte[]{68, 108, 59, 21, -7, -110, 102, -121, -46, -60, 5, 52, -3, -75, 100, 0, 0, 0, 0, 0, 0},};

    Bigint(byte[] b) {
        bytes = b;
    }

    public Bigint(long iVal) {
        if (iVal == 0) {
            bytes = new byte[0];
        } else if (iVal >= -127 && iVal <= 127) {
            bytes = new byte[]{(byte) iVal};
        } else {
            ArrayList<Byte> v = new ArrayList<Byte>();
            byte b = (iVal < 0) ? 0 : (byte) -1;
            while (iVal != 0 && iVal != -1) {
                b = (byte) (iVal & 0xff);
                v.add(b);
                iVal >>= 8;
            }
            if (iVal == -1 && b > 0) 
                v.add((byte) -1);
            if (iVal==0 && b<0)
                v.add((byte)0);
            var bs = new byte[v.size()];
            int len = 0;
            for (int j = v.size() - 1; j >= 0; j--) {
                bs[len++] = v.get(j);
            }
            bytes = bs;
        }
    }
    
    int toInt()
    {
        int n = bytes.length; // better <4
        int j=0;
        int iVal = (n>0 && bytes[0]<0)?-1:0;
        for (;j<n;j++)
                iVal = (iVal<<8)|((int)bytes[j]&0xff);
        return iVal;
    }
    long toLong()
    {
        int n = bytes.length; // better <8
        int j=0;
        long Val = (n>0 && bytes[0]<0)?-1:0;
        for (;j<n;j++)
                Val = (Val<<8)|((int)bytes[j]&0xff);
        return Val;
    }
    
    public boolean getSign()
    {
        return bytes.length>0 && bytes[0]<0;
    }

    public Bigint Negate() {
        var c = new byte[bytes.length];
        for (int j = 0; j < bytes.length; j++) {
            c[j] = (byte) (-bytes[j] - 1);
        }
        var r = (byte) 1;
        for (int j = bytes.length - 1; j >= 0 && r == 1; j--) {
            if (c[j] == (byte) -1) {
                c[j] = 0;
            } else {
                c[j] += 1;
                r = 0;
            }
        }
        return new Bigint(c);
    }
    
    public Bigint Abs()
    {
        return (getSign())? Negate() : this;
    }
    
    public Bigint Add(Bigint b,int shift)
    {
        int off = 0;
        int boff = 0;
        int m;
        byte h = (byte)0;
        int n = bytes.length;
        int bn = b.bytes.length + shift;
        boolean s = getSign();
        boolean bs = b.getSign();
        if (n>bn)
        {
            boff = n-bn;
            m = n;
        } else
        {
            off = bn - n;
            m = bn;
        }
        var t = new byte[m+1];
        int r = 0;
        for (int j=m-1;j>= -1; j--)
        {
            int d = r;
            if (j-off>=0)
                d += ((int)bytes[j-off])&0xff;
            else if (s)
                d += 255;
            if (j-boff>=b.bytes.length)
                d += 0;
            else if (j-boff>=0)
                d += ((int)b.bytes[j-boff])&0xff;
            else if (bs)
                d += 255;
            t[j+1] = (byte)(d&0xff);
            r = d>>8;
        }
        if (t[0]==(byte)-1)
            h = (byte)-1;
        int j = 0;
        while (j<m+1 && t[j]==h)
            j++;
        if (j==m+1 || (h==(byte)-1 && t[j]>=0 )||(h==(byte)0 && t[j]<0))
            j--;
        var c = new byte[m+1-j];
        for (int k=0;k<c.length;k++)
            c[k] = t[j+k];
        return new Bigint(c);
    }
    
    Bigint Times(byte b)
    {
        if (bytes.length==0)
            return this;
        var m = ((int)b)&0xff;
        boolean s = getSign();
        var th = s? Negate():this;
        var c = new byte[th.bytes.length];
        int r = 0;
        for (int j=th.bytes.length-1;j>=0;j--)
        {
            var d = ((int)th.bytes[j])&0xff;
            d = d*m+r;
            c[j] = (byte)(d&0xff);
            r = d>>8;
        }
        if (r>0||c[0]<0)
        {
            var a = new byte[th.bytes.length+1];
            for (int j=0;j<th.bytes.length;j++)
                a[j+1] = c[j];
            a[0] = (byte)r;
            c = a;
        }
        var rs = new Bigint(c);
        return s?rs.Negate():rs;
    }
    
    Bigint Times10()
    {
        return Bigint.this.Times((byte)10);
    }

    @Override
    public int compareTo(Object o) {
       Bigint x;
       if (o instanceof Bigint)
           x = (Bigint)o;
       else //if (o instanceof Long)
           x = new Bigint((Long)o);
 //      else
 //          return new Numeric(this).compareTo(o);
        int n = bytes.length;
        int xn = x.bytes.length;
        if (bytes.length==0)
        {
            if (x.bytes.length==0)
               return 0;
            return x.getSign()?1:-1;
        } else if (bytes[0]<0)
        {
            if (x.bytes.length == 0) {
                return -1;
            }
            if (x.bytes[0] >=0) {
                return -1;
            }
            if (n < xn) {
                return 1;
            }
            if (n > xn) {
                return -1;
            }
            for (int j = 0; j < n; j++) {
                byte b = (j < xn) ? x.bytes[j] : (byte) 0;
                if (bytes[j] < b) {
                    return -1;
                }
                if (bytes[j] > b) {
                    return 1;
                }
            }          
        } else
        {
            if (x.bytes.length == 0) {
                return 1;
            }
            if (x.bytes[0] < 0) {
                return 1;
            }
            if (n < xn) {
                return -1;
            }
            if (n > xn) {
                return 1;
            }
            for (int j = 0; j < n; j++) {
                byte b = (j < xn) ? x.bytes[j] : (byte) 0;
                if (bytes[j] < b) {
                    return -1;
                }
                if (bytes[j] > b) {
                    return 1;
                }
            }          
        }
        return 0;
    }
    int compareTo(Bigint x,int shift)
    {
        int n = bytes.length;
        int xn = x.bytes.length;
        int j;
        if (bytes.length == 0) {
            if (x.bytes.length == 0) {
                return 0;
            }
            if (x.bytes[0] >= 0) {
                return -1;
            }
            return 1;
        } else if (bytes[0] < 0) {
            if (x.bytes.length == 0) {
                return -1;
            }
            if (x.bytes[0] >= 0) {
                return -1;
            }
            if (n < xn + shift) {
                return 1;
            }
            if (n > xn + shift) {
                return -1;
            }
            for (j = 0; j < n; j++) {
                byte b = (j < xn) ? x.bytes[j] : (byte) 0;
                if (bytes[j] < b) {
                    return 1;
                }
                if (bytes[j] > b) {
                    return -1;
                }
            }
        } else {
            if (x.bytes.length == 0) {
                return 1;
            }
            if (x.bytes[0] < 0) {
                return 1;
            }
            if (n < xn + shift) {
                return -1;
            }
            if (n > xn + shift) {
                return 1;
            }
            for (j = 0; j < n; j++) {
                byte b = (j < xn) ? x.bytes[j] : (byte) 0;
                if (bytes[j] < b) {
                    return -1;
                }
                if (bytes[j] > b) {
                    return 1;
                }
            }
        }
        return 0;      
    }
    static Bigint Pow10(int n)
    {
        if (n<pow10.length)
            return new Bigint(pow10[n]);
        var np = new byte[n+1][];
        int j;
        for (j=0;j<pow10.length;j++)
            np[j] = pow10[j];
        for (;j<n+1;j++)
            np[j] = new Bigint(np[j-1]).Times10().bytes;
        pow10 = np;
        return new Bigint(pow10[n]);
    }
    Bigint Shift8() {
        var nb = new byte[bytes.length + 1];
        for (int i = 0; i < bytes.length; i++) {
            nb[i] = bytes[i];
        }
        return new Bigint(nb);
    }
    public Bigint Plus(Bigint b) { return Add(b,0); }
    public Bigint Minus(Bigint b) { return Plus(b.Negate()); }
    public Bigint Times(Bigint b)
    {
        Bigint r = Zero;
        int s = 0;
        for (int j=b.bytes.length-1;j>=0;j--,s++)
            r = r.Add(Times(b.bytes[j]),s);
        return r;
    }
    public Bigint Divide(Bigint b) { return Divide(this,b); }  
    public Bigint Remainder(Bigint b) { return Minus(Divide(b).Times(b)); }
    static Bigint Divide(Bigint a,Bigint b)
    {
            boolean sa = a.getSign();
            boolean sb = b.getSign();
            boolean s = (sa != sb);
            if (sa)
                a = a.Negate();
            if (sb)
                b = b.Negate();
            if (b.bytes.length==0 || a.compareTo(b)<0)
                return Zero;
            if (b == a)
                return One;
            var ds = new ArrayList<Bigint>();
            for (; ;)
            {
                ds.add(b);
                Bigint c = b.Shift8();
                if (c.compareTo(a)>0)
                    break;
                b = c;
            }
            int d = 0;
            // first work out the most significant digit
            while (b.compareTo(a)<0)
            {
                a = a.Minus(b); // b is ds[ds.Count-1]
                d++;
            }
            // fix the sign
            int j = (d > 127) ? 1 : 0;
            var nb = new byte[ds.size() + j];
            nb[j++] = (byte)d;
            // now do the rest of the digits
            for (int i = ds.size() - 2; i >= 0; i--)
            {
                var dv = ds.get(i);
                d = 0;
                while (dv.compareTo(a)<0)
                {
                    a = a.Minus(dv);
                    d++;
                }
                nb[j++] = (byte)d;
            }
            var r = new Bigint(nb);
            if (s)
                r = r.Negate();
            return r;        
    }
    public static Bigint Parse(String str)
    {
        if (str.charAt(0) == '+') {
            str = str.substring(1);
        }
        boolean sgn = str.charAt(0) == '-';
        if (sgn) {
            str = str.substring(1);
        }
        Bigint r = Zero;
        int n = str.length() - 1;
        int j = 0;
        while (n >= 0) {
            byte d = (byte) (str.charAt(j) - '0');
            r = r.Plus(Pow10(n).Times(d));
            n--;
            j++;
        }
        if (sgn) {
            r = r.Negate();
        }
        return r;
    }
    @Override
    public String toString()
    {
        if (bytes.length==0)
            return "0";
        String r = getSign()?"-":""; 
        Bigint a = Abs();
        int n = 0;
        while (a.compareTo(Pow10(n))>=0)
            n++;
        n--;
        while (n>0)
        {
            int d =0;
            Bigint m = Pow10(n);
            while (a.compareTo(m)>=0)
            {
                a = a.Minus(m);
                d++;
            }
            r += d;
            n--;
        }
        r += a.bytes[0];
        return r;
    }
}
