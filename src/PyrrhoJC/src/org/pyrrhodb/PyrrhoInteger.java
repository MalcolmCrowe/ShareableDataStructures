package org.pyrrhodb;
import java.util.*;
import java.io.*; // for testing

public class PyrrhoInteger {
    byte[] bytes = null;
    static byte[][] pow10 = new byte[][]
    {
        new byte[]	{	1 },
        new byte[]	{	10 },
        new byte[]	{	100 },
        new byte[]	{ 3, -24},//{	3,232 },
        new byte[] { 39,16},
        new byte[] { 1, -112, -96 },//{ 1,134,160},
        new byte[] { 15,66,64},
        new byte[] { 0, -104, -106, -128 },//{ 0,152,150,128}, // without the 0 it is a negative number!
        new byte[] { 5, -11, -31, 0 },//{ 5,245,225,0},
        new byte[] { 59, -102, -54, 0 }, //{ 59,154,202,0},
        new byte[] { 2,84,11,-28,0 }, //{ 2,84,11,228,0},
        new byte[] { 23,72,11,-24,0}, //{ 23,72,118,232,0},
        new byte[] { 0, -24,-44,-101,16,0}, //{ 0,232,212,165,16,0},
        new byte[] { 9, 24, 78, 114, -96,0}, //{ 9,24,78,114,160,0},
        new byte[] { 90, -13, 16, 122, 64, 0 }, //{ 90,243,16,122,64,0},
        new byte[] { 3, -115, 126, -92, -58, -128, 0 }, //{ 3,141,126,164,198,128,0},
        new byte[] { 35, -122, -14, 111, -63,0,0}, //{ 35,134,242,111,193,0,0},
        new byte[] { 1, 99, 69, 120, 93, -118, 0, 0 }//{ 1,99,69,120,93,138,0,0},
    };
    PyrrhoInteger(byte[] b) {
        bytes = b;
    }
    PyrrhoInteger(int iVal) {
        if (iVal == 0)
            bytes = new byte[0];
        else if (iVal >= -127 && iVal <= 127) {
            bytes = new byte[1];
            bytes[0] = (byte)iVal;
        } else {
            ArrayList v = new ArrayList();
            int n = iVal;
            byte b = (byte)((n < 0) ? 0 : -1);
            while (n != 0 && n != -1) {
                b = (byte)(n & 0xff);
                v.add(new Byte(b));
                n >>= 8;
            }
            if (n == -1 && b >=0)
                v.add(new Byte((byte)-1));
            if (n == 0 && b <0 )
                v.add(new Byte((byte)0));
            bytes = new byte[v.size()];
            int len = 0;
            for (int j = v.size() - 1; j >= 0; j--)
                bytes[len++] = ((Byte)v.get(j)).byteValue();
        }
    }
    public PyrrhoInteger negate() {
        byte[] c = new byte[bytes.length];
        int j;
        for (j = 0; j < bytes.length; j++) {
            int x = (int)bytes[j];
            if (x<0)
                x += 256;
            c[j] = (byte)(255 - x);
        }
        byte r = 1;
        for (j=bytes.length-1;j>=0 && r==1;j--) {
            if (c[j]== -1)
                c[j] = 0;
            else {
                c[j] += 1;
                r = 0;
            }
        }
        return new PyrrhoInteger(c);
    }
    public PyrrhoInteger times10() {
        return times((byte)10);
    }
    public boolean Sign() {
        return bytes.length > 0 && bytes[0] < 0;
    }
    public static PyrrhoInteger abs(PyrrhoInteger x) {
        if (x.Sign())
            x = x.negate();
        return x;
    }
    public PyrrhoInteger times(byte m) {
        byte[] c = new byte[bytes.length];
        int j;
        int r = 0;
        boolean s = Sign();
        for (j=bytes.length-1;j>=0;j--) {
            int d = (int)bytes[j];
            if (d < 0)
                d += 256;
            d = d*m + r;
            c[j] = (byte)(d&0xff);
            r = d>>8;
        }
        if (r>0 || (s && c[0]>=0) || ((!s)&& c[0]<0)) {
            byte[] a = new byte[bytes.length+1];
            for (j=0;j<bytes.length;j++)
                a[j+1] = c[j];
            a[0] = (byte)(r+(s?-1:0));
            c = a;
        }
        return new PyrrhoInteger(c);
    }
    public static PyrrhoInteger Pow10(int n) {
        if (n<pow10.length)
            return new PyrrhoInteger(pow10[n]);
        byte[][]np = new byte[n+1][];
        int j=0;
        while(j<pow10.length) {
            np[j] = pow10[j];
            j++;
        }
        while (j<n+1) {
            np[j] = new PyrrhoInteger(np[j-1]).times10().bytes;
            j++;
        }
        pow10 = np;
        return new PyrrhoInteger(pow10[n]);
    }
    public static PyrrhoInteger add(PyrrhoInteger a,PyrrhoInteger b) {
        return a.add(b,0);
    }
    public PyrrhoInteger add(PyrrhoInteger b,int shift) {
        int off=0,boff=0,m;
        int h=0;
        int n = bytes.length, bn = b.bytes.length + shift;
        boolean s = Sign(), bs = b.Sign();
        if (n>bn) {
            boff = n-bn;
            m = n;
        } else {
            off = bn-n;
            m = bn;
        }
        byte[] t = new byte[m+1];
        int j;
        int r = 0;
        for (j=m-1;j>= -1;j--) {
            int d = r;
            int dd = 0;
            if (j-off>=0)
                dd = (int)bytes[j-off];
            else if (s)
                dd = -1;
            if (dd < 0)
                dd += 256;
            d += dd;
            if (j-boff>=0)
                dd = (int)b.bytes[j-boff];
            else if (bs)
                dd = -1;
            if (dd < 0)
                dd += 256;
            d += dd;
            t[j+1] = (byte)(d&0xff);
            r = d>>8;
        }
        if (t[0]== -1)
            h = -1;
        j = 0;
        while (j<m+1 && t[j]==h)
            j++;
        if (j==m+1 || (h== -1 && t[j]>=0) || (h==0 && t[j]<0))
            j--;
        byte[] c = new byte[m+1-j];
        for (int k=0;k<c.length;k++)
            c[k] = t[j+k];
        return new PyrrhoInteger(c);
    }
    public int CompareTo(PyrrhoInteger x) {
        return CompareTo(x, 0);
    }
    public int CompareTo(PyrrhoInteger x,int shift) {
        int n = bytes.length;
        int xn = x.bytes.length;
        int j;
        if (bytes.length==0) {
            if (x.bytes.length==0)
                return 0;
            if (x.bytes[0]>0)
                return -1;
            return 1;
        } else if (bytes[0]<0) {
            if (x.bytes.length==0)
                return -1;
            if (x.bytes[0]>=0)
                return -1;
            if (n<xn+shift)
                return 1;
            if (n>xn+shift)
                return -1;
            for (j=0;j<n;j++) {
                int b = (j<xn)?x.bytes[j]:(byte)0;
                if (b < 0)
                    b += 256;
                int a = bytes[j];
                if (a < 0)
                    a += 256;
                if (a<b)
                    return 1;
                if (a>b)
                    return -1;
            }
        } else {
            if (x.bytes.length==0)
                return 1;
            if (x.bytes[0]<0)
                return 1;
            if (n<xn+shift)
                return -1;
            if (n>xn+shift)
                return 1;
            for (j=0;j<n;j++) {
                int b = (j<xn)?x.bytes[j]:(byte)0;
                if (b < 0)
                    b += 256;
                int a = bytes[j];
                if (a < 0)
                    a += 256;
                if (a < b)
                    return -1;
                if (a > b)
                    return 1;
            }
        }
        return 0;
    }
    public String toString() {
        if (bytes.length == 0)
            return "0";
        String r = "";
        if (bytes[0]<0)
            r = "-";
        PyrrhoInteger a = abs(this);
        int n = 0;
        while (a.CompareTo(Pow10(n)) >= 0)
            n++;
        n--;
        while (n > 0) {
            int d = 0;
            PyrrhoInteger m = Pow10(n);
            while (a.CompareTo(m) >= 0) {
                a = a.add(m.negate(),0);
                d++;
            }
            r += d;
            n--;
        }
        r += a.bytes[0];
        return r;
    }
    public static PyrrhoInteger Parse(String str) {
        boolean sgn = str.charAt(0)=='-';
        if (sgn)
            str = str.substring(1);
        PyrrhoInteger r = new PyrrhoInteger(0);
        int n = str.length()-1;
        int j = 0;
        while (n>=0) {
            byte d = Byte.parseByte(str.substring(j,1));
            r = r.add(Pow10(n).times(d),0);
            n--;
            j++;
        }
        return r;
    }
    public static PyrrhoInteger times(PyrrhoInteger a,PyrrhoInteger b) {
        PyrrhoInteger r = new PyrrhoInteger(0);
        int s=0;
        for (int j=b.bytes.length-1;j>=0;j--,s++)
            r = r.add(a.times(b.bytes[j]),s);
        return r;
    }
    public PyrrhoInteger high() {
        if (bytes.length<=8)
            return this;
        byte[] c = new byte[8];
        for (int j=0;j<8;j++)
            c[j] = bytes[j];
        return new PyrrhoInteger(c);
    }
    public byte last() {
        return bytes[bytes.length-1];
    }
}
