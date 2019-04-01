using System;
using System.Collections.Generic;
#nullable enable
namespace Shareable
{
    /// <summary>
    /// This namespace has its own concept of Integer
    /// Uses signed radix-256 arithmetic.
    /// Integers are values, so no Mutators
    /// </summary>
	public class Integer : IComparable 
	{
        /// <summary>
        /// The radix-256 digits in the Integer
        /// </summary>
		public readonly byte[] bytes; // high to low, first bit is sign bit
        /// <summary>
        /// A table of powers of 10 to start off the hp
        /// </summary>
		static byte[][] pow10 = new byte[][] 
			{
				new byte[]	{	1 },
				new byte[]	{	10 },
				new byte[]	{	100 },
				new byte[]	{	3,232 },
				new byte[] { 39,16},
				new byte[] { 1,134,160},
				new byte[] { 15,66,64},
				new byte[] { 0,152,150,128}, // without the 0 it is a negative number!
				new byte[] { 5,245,225,0},
				new byte[] { 59,154,202,0},
				new byte[] { 2,84,11,228,0},
				new byte[] { 23,72,118,232,0},
				new byte[] { 0,232,212,165,16,0},
				new byte[] { 9,24,78,114,160,0},
				new byte[] { 90,243,16,122,64,0},
				new byte[] { 3,141,126,164,198,128,0},
				new byte[] { 35,134,242,111,193,0,0},
				new byte[] { 1,99,69,120,93,138,0,0},
				new byte[] { 13,224,182,179,167,100,0,0},
				new byte[] { 0,138,199,35,4,137,232,0,0},
				new byte[] { 5,107,199,94,45,99,16,0,0},
				new byte[] { 54,53,201,173,197,222,160,0,0},
				new byte[] { 2,30,25,224,201,186,178,64,0,0},
				new byte[] { 21,45,2,199,225,74,246,128,0,0},
				new byte[] { 0,211,194,27,206,204,237,161,0,0,0},
				new byte[] { 8,69,149,22,20,1,72,74,0,0,0},
				new byte[] { 82,183,210,220,200,12,210,228,0,0,0},
				new byte[] { 3,59,46,60,159,208,128,60,232,0,0,0},
				new byte[] { 32,79,206,94,62,37,2,97,16,0,0,0},
				new byte[] { 1,67,30,15,174,109,114,23,202,160,0,0,0},
				new byte[] { 12,159,44,156,208,70,116,237,234,64,0,0,0},
				new byte[] { 126,55,190,32,34,192,145,75,38,128,0,0,0},
				new byte[] { 4,238,45,109,65,91,133,172,239,129,0,0,0,0},
				new byte[] { 49,77,198,68,141,147,56,193,91,10,0,0,0,0},
				new byte[] { 1,237,9,190,173,135,192,55,141,142,100,0,0,0,0},
				new byte[] { 19,66,97,114,199,77,130,43,135,143,232,0,0,0,0},
				new byte[] { 0,192,151,206,123,201,7,21,179,75,159,16,0,0,0,0},
				new byte[] { 7,133,238,16,213,218,70,217,0,244,54,160,0,0,0,0},
				new byte[] { 75,59,76,168,90,134,196,122,9,138,34,64,0,0,0,0},
				new byte[] { 2,240,80,254,147,137,67,172,196,95,101,86,128,0,0,0,0},
				new byte[] { 29,99,41,241,195,92,164,191,171,185,245,97,0,0,0,0,0},
				new byte[] { 1,37,223,163,113,161,158,111,124,181,67,149,202,0,0,0,0,0},
				new byte[] { 11,122,188,98,112,80,48,90,223,20,163,217,228,0,0,0,0,0},
				new byte[] { 114,203,91,216,99,33,227,140,182,206,102,130,232,0,0,0,0,0},
				new byte[] { 4,123,241,150,115,223,82,227,127,36,16,1,29,16,0,0,0,0,0},
				new byte[] { 44,215,111,224,134,185,60,226,247,104,160,11,34,160,0,0,0,0,0},
				new byte[] { 1,192,106,94,197,67,60,96,221,170,22,64,111,90,64,0,0,0,0,0},
				new byte[] { 17,132,39,179,180,160,91,200,168,164,222,132,89,134,128,0,0,0,0,0},
				new byte[] { 0,175,41,141,5,14,67,149,214,150,112,177,43,127,65,0,0,0,0,0,0},
				new byte[] { 6,215,159,130,50,142,163,218,97,224,102,235,178,248,138,0,0,0,0,0,0},
				new byte[] { 68,108,59,21,249,146,102,135,210,196,5,52,253,181,100,0,0,0,0,0,0},
				new byte[] { 2,172,58,78,219,191,184,1,78,59,168,52,17,233,21,232,0,0,0,0,0,0},
				new byte[] { 26,186,71,20,149,125,48,13,14,84,146,8,179,26,219,16,0,0,0,0,0,0},
				new byte[] { 1,11,70,198,205,214,227,224,130,143,77,180,86,255,12,142,160,0,0,0,0,0,0},
				new byte[] { 10,112,195,196,10,100,230,197,25,153,9,11,101,246,125,146,64,0,0,0,0,0,0},
		};
        public static readonly Integer intMax = new Integer(int.MaxValue);
        public static readonly Integer intMin = new Integer(int.MinValue);
        public static readonly Integer longMax = new Integer(long.MaxValue);
        public static readonly Integer longMin = new Integer(int.MinValue);

        /// <summary>
        /// Constructor: An Integer from an array of radix-256 digits
        /// </summary>
        /// <param name="b">The array of digits</param>
		public Integer(byte[] b)
		{
			bytes = b;
		}
        /// <summary>
        /// Constructor: An Integer from an int
        /// </summary>
        /// <param name="iVal">The int to use</param>
		public Integer(int iVal)
		{
			if (iVal==0)
				bytes = new byte[0];
			else if (iVal>=-127 && iVal<=127)
			{
				bytes = new byte[1];
				bytes[0] = (byte)iVal;
			} 
			else 
			{
				var v = new List<byte>();
				int n = iVal;
				byte b = (byte)((n<0)?0:255);
				while (n!=0 && n!=-1)
				{
					b = (byte)(n&0xff);
					v.Add(b);
					n >>= 8;
				}
				if (n== -1 && b<128)
					v.Add((byte)255);
				if (n== 0 && b>127)
					v.Add((byte)0);
				bytes = new byte[v.Count];
				int len = 0;
				for (int j=v.Count-1;j>=0;j--)
					bytes[len++] = (byte)v[j];
			}
		}
        /// <summary>
        /// Constructor: An Integer from a long
        /// </summary>
        /// <param name="i64Val">The long to use</param>
		public Integer(long i64Val)
		{
			if (i64Val==0)
				bytes = new byte[0];
			else if (i64Val>=-127 && i64Val<=127)
			{
				bytes = new byte[1];
				bytes[0] = (byte)i64Val;
			} 
			else 
			{
				var v = new List<byte>();
				long n = i64Val;
				byte b = (byte)((n<0)?0:255);
				while (n!=0 && n!=-1)
				{
					b = (byte)(n&0xff);
					v.Add(b);
					n >>= 8;
				}
				if (n== -1 && b<128)
					v.Add((byte)255);
				if (n== 0 && b>127)
					v.Add((byte)0);
				bytes = new byte[v.Count];
				int len = 0;
				for (int j=v.Count-1;j>=0;j--)
					bytes[len++] = (byte)v[j];
			}
		}
        public static Integer Zero = new Integer(0);
        public static Integer One = new Integer(1);
        public bool IsZero()
        {
            for (int j = 0; j < bytes.Length; j++)
                if (bytes[j] != 0)
                    return false;
            return true;
        }
        public int BitsNeeded()
        {
            if (bytes.Length == 0)
                return 0;
            var r = bytes.Length*8-7;
            int b = bytes[0];
            if (b < 0)
                b = -b;
            while (b > 0)
            {
                r++;
                b = b >> 1;
            }
            return r;
        }
        /// <summary>
        /// Cast: An int from an Integer
        /// </summary>
        /// <param name="x">The Integer to cast</param>
        /// <returns>The corresponding int value</returns>
        static public explicit operator int(Integer x)
		{
			int n = x.bytes.Length;
            if (n > 4)
            {
                Console.WriteLine("PE13");
                for (var i = 0; i < n; i++)
                    Console.Write(" " + x.bytes[i]);
                Console.WriteLine();
                throw (new Exception("PE13 " + Reader.pe13));
            }
			int j=0;
			int iVal = (n>0 && x.bytes[0]>127)?-1:0;
			for (;j<n;j++)
				iVal = (iVal<<8)|(int)x.bytes[j];
			return iVal;
		}
        /// <summary>
        /// Cast: A long from an Integer
        /// </summary>
        /// <param name="x">The Integer to cast</param>
        /// <returns>The corresponding long value</returns>
		static public explicit operator long(Integer x)
		{
			int n = x.bytes.Length;
			if (n>8)
				throw(new Exception("PE14"));
			long i64Val = (n>0 && x.bytes[0]>127)?-1L:0L;
			for (int j=0;j<x.bytes.Length;j++)
				i64Val = (i64Val<<8)|(long)x.bytes[j];
			return i64Val;
		}
        /// <summary>
        /// Cast: a double from an Integer
        /// </summary>
        /// <param name="x">The Integer to cast</param>
        /// <returns>The corresponding double value</returns>
		static public explicit operator double(Integer x)
		{
			int n = x.bytes.Length;
			double dVal = (n>0 && x.bytes[0]>127)?-1.0:0.0;
			for (int j=0;j<x.bytes.Length;j++)
				dVal = (dVal*256.0)+(double)x.bytes[j];
			return dVal;
		}
        /// <summary>
        /// Creator: returns -this
        /// </summary>
        /// <returns>The new Integer</returns>
		internal Integer Negate()
		{
			byte[] c = new byte[bytes.Length];
			int j;
			for (j=0;j<bytes.Length;j++)
				c[j] = (byte)(255-(uint)bytes[j]);
			byte r = 1;
			for (j=bytes.Length-1;j>=0 && r==1;j--)
			{
				if (c[j]==255)
					c[j] = 0;
				else 
				{
					c[j] += 1;
					r = 0;
				}
			}
			return new Integer(c);
		}
        /// <summary>
        /// Creator: returns 10*this
        /// </summary>
        /// <returns>The new Integer</returns>
		internal Integer Times10()
		{
			return Times((byte)10);
		}
        /// <summary>
        /// Creator: returns m*this
        /// </summary>
        /// <param name="m">A byte multiplier</param>
        /// <returns></returns>
        public Integer Times(byte m)
        {
            if (bytes.Length == 0)
                return this;
            bool s = Sign;
            var th = s ? -this : this;
            byte[] c = new byte[th.bytes.Length];
            int j;
            uint r = 0;
            for (j = th.bytes.Length - 1; j >= 0; j--)
            {
                uint d = th.bytes[j];
                d = d * m + r;
                c[j] = (byte)(d & 0xff);
                r = d >> 8;
            }
            if (r > 0 || c[0] > 127)
            {
                var a = new byte[th.bytes.Length + 1];
                for (j = 0; j < th.bytes.Length; j++)
                    a[j + 1] = c[j];
                a[0] = (byte)r;
                c = a;
            }
            var rs = new Integer(c);
            return s ? -rs : rs;
        }
        /// <summary>
        /// Creator: performs this/q (PRE: q>0)
        /// </summary>
        /// <param name="q">A byte divisor</param>
        /// <param name="r">ref remainder</param>
        /// <returns></returns>
		internal Integer Quotient(byte d,ref int r) //q>0
		{
			Integer a = this;
			r = 0;
			if (a.bytes.Length==0)
				return a;
			bool sgn = Sign;
			if (sgn)
				a = a.Negate();
			byte[] qu = new byte[a.bytes.Length];
			for (int j=0;j<a.bytes.Length;j++)
			{
				int n = (r<<8) + a.bytes[j];
				qu[j] = (byte)(n/d);
				r = n%d;
			}
			if (qu[0]==0)
			{
				byte[] b = new byte[a.bytes.Length-1];
				for (int j=0;j<b.Length;j++)
					b[j]=qu[j+1];
				qu = b;
			}
			if (qu.Length>0 && qu[0]>127)
			{
				byte[] b = new byte[qu.Length+1];
				for (int j=0;j<qu.Length;j++)
					b[j+1] = qu[j];
				b[0] = (byte)0;
				qu = b;
			} 
			a = new Integer(qu);
			if (sgn)
				a = a.Negate();
			return a;
		}
        /// <summary>
        /// Gets an Integer power of 10
        /// </summary>
        /// <param name="n">The exponent</param>
        /// <returns>The new Integer</returns>
		internal static Integer Pow10(int n)
		{
			if (n<pow10.Length)
				return new Integer(pow10[n]);
			byte[][]np = new byte[n+1][];
			int j=0;
			while(j<pow10.Length)
			{
				np[j] = pow10[j];
				j++;
			}
			while (j<n+1)
			{
				np[j] = new Integer(np[j-1]).Times10().bytes;
				j++;
			}
			pow10 = np;
			return new Integer(pow10[n]);
		}
        /// <summary>
        /// Creator: Sum of two Integers
        /// </summary>
        /// <param name="a">An Integer</param>
        /// <param name="b">An Integer</param>
        /// <returns></returns>
		public static Integer operator+(Integer a,Integer b)
		{
			return a.Add(b,0);
		}
        /// <summary>
        /// Creator: Add a shifted Integer to this
        /// </summary>
        /// <param name="b">An Integer</param>
        /// <param name="shift">Number of radix-256 digits to shift</param>
        /// <returns></returns>
		internal Integer Add(Integer b,int shift)
		{
			int off=0,boff=0,m;
			int h=0;
			int n = bytes.Length, bn = b.bytes.Length + shift;
			bool s = Sign, bs = b.Sign;
			if (n>bn) 
			{
				boff = n-bn;
				m = n;
			}
			else 
			{
				off = bn-n;
				m = bn;
			}
			byte[] t = new byte[m+1];
			int j;
			uint r = 0;
			for (j=m-1;j>= -1;j--)
			{
				uint d = r;
				if (j-off>=0)
					d += (uint)bytes[j-off];
				else if (s)
					d += 255;
				if (j-boff>=b.bytes.Length)
					d += 0;
				else if (j-boff>=0)
					d += (uint)b.bytes[j-boff];
				else if (bs)
					d += 255;
				t[j+1] = (byte)(d&0xff);
				r = d>>8;
			}
			if (t[0]==255)
				h = 255;
			j = 0;
			while (j<m+1 && t[j]==h)
				j++;
			if (j==m+1 || (h==255 && t[j]<=127) || (h==0 && t[j]>127))
				j--;
			byte[] c = new byte[m+1-j];
			for (int k=0;k<c.Length;k++)
				c[k] = t[j+k];
			return new Integer(c);
        }
        Integer Shift8()
        {
            var nb = new byte[bytes.Length + 1];
            for (int i = 0; i < bytes.Length; i++)
                nb[i] = bytes[i];
            return new Integer(nb);
        }
        public static bool operator<(Integer a,Integer b)
        {
            return a.CompareTo(b)<0;
        }
        public static bool operator <=(Integer a, Integer b)
        {
            return a.CompareTo(b) <= 0;
        }
        public static bool operator >(Integer a, Integer b)
        {
            return a.CompareTo(b) > 0;
        }
        public static bool operator >=(Integer a, Integer b)
        {
            return a.CompareTo(b) >= 0;
        }
        public static bool operator ==(Integer a, Integer b)
        {
            if (((object)b) == null)
                return ((object)a) == null;
            return a.CompareTo(b)== 0;
        }
        public static bool operator !=(Integer a, Integer b)
        {
            if (((object)b) == null)
                return ((object)a) != null; 
            return a.CompareTo(b) != 0;
        }
        /// <summary>
        /// Creator: - Integer
        /// </summary>
        /// <param name="a">The Integer</param>
        /// <returns>A new Integer</returns>
		public static Integer operator-(Integer a)
		{
			return a.Negate();
		}
        /// <summary>
        /// Creator: Integer - Integer
        /// </summary>
        /// <param name="a">First Integer</param>
        /// <param name="b">Second Integer</param>
        /// <returns></returns>
		public static Integer operator-(Integer a,Integer b)
		{
			return a.Add(b.Negate(),0);
		}
        /// <summary>
        /// Creator: Integer * Integer
        /// </summary>
        /// <param name="a">First Integer</param>
        /// <param name="b">Second Integer</param>
        /// <returns></returns>
		public static Integer operator*(Integer a,Integer b)
		{
			Integer r = new Integer(0);
			int s=0;
            for (int j = b.bytes.Length - 1; j >= 0; j--, s++)
                r = r.Add(a.Times(b.bytes[j]), s);
			return r;
		}
        /// <summary>
        /// Creator: Integer / Integer
        /// </summary>
        /// <param name="a">Dividend</param>
        /// <param name="b">Divisor</param>
        /// <returns></returns>
		public static Integer operator /(Integer a, Integer b)
        {
            bool sa = a.Sign;
            bool sb = b.Sign;
            bool s = (sa != sb);
            if (sa)
                a = a.Negate();
            if (sb)
                b = b.Negate();
            if (b.IsZero() || a < b)
                return Zero;
            if (b == a)
                return One;
            var ds = new List<Integer>();
            for (; ;)
            {
                ds.Add(b);
                Integer c = b.Shift8();
                if (c > a)
                    break;
                b = c;
            }
            int d = 0;
            // first work out the most significant digit
            while (b <= a)
            {
                a = a - b; // b is ds[ds.Count-1]
                d++;
            }
            // fix the sign
            int j = (d > 127) ? 1 : 0;
            var nb = new byte[ds.Count + j];
            nb[j++] = (byte)d;
            // now do the rest of the digits
            for (int i = ds.Count - 2; i >= 0; i--)
            {
                var dv = ds[i];
                d = 0;
                while (dv <= a)
                {
                    a = a - dv;
                    d++;
                }
                nb[j++] = (byte)d;
            }
            var r = new Integer(nb);
            if (s)
                r = r.Negate();
            return r;
        }
        /// <summary>
        /// Gets the sign of the Integer
        /// </summary>
		public bool Sign { get { return bytes.Length>0 && bytes[0]>127; }}
        /// <summary>
        /// Creator: abs(Integer)
        /// </summary>
        /// <param name="x">The Integer</param>
        /// <returns>The new Integer</returns>
		public static Integer Abs(Integer x)
		{
			if (x.Sign)
				x = x.Negate();
			return x;
		}
        /// <summary>
        /// Cast: Integer to string
        /// </summary>
        /// <param name="x"></param>
        /// <returns></returns>
		public static implicit operator string(Integer x)
		{
			return x.ToString();
		}
        /// <summary>
        /// Accessor: The string value of an Integer
        /// </summary>
        /// <returns>The string</returns>
		public override string ToString()
		{
			if (bytes.Length==0)
				return "0";
			string r = "";
			if (Sign) 
				r = "-";
			Integer a = Abs(this);
			int n=0;
			while (a.CompareTo(Pow10(n))>=0)
				n++;
			n--;
			while (n>0)
			{
				int d = 0;
				Integer m = Pow10(n);
				while (a.CompareTo(m)>=0)
				{
					a = a-m;
					d++;
				}
				r += d;
				n--;
			}
			r += a.bytes[0];
			return r;
		}
        /// <summary>
        /// Creator: The Integer value of a string
        /// </summary>
        /// <param name="str">The string to parse</param>
        /// <returns>The Integer value</returns>
		public static Integer Parse(string str)
		{
            if (str[0] == '+')
                str = str.Substring(1);
            bool sgn = str[0] == '-';
			if (sgn)
				str = str.Substring(1);
			Integer r = new Integer(0);
			int n = str.Length-1;
			int j = 0;
			while (n>=0)
			{
				byte d = byte.Parse(str.Substring(j,1));
				r = r + Pow10(n).Times(d);
				n--;
				j++;
			}
			if (sgn)
				r = -r;
			return r;
		}

#region IComparable Members
        /// <summary>
        /// Accessor: Compare two Integers
        /// </summary>
        /// <param name="obj">Something to compare</param>
        /// <returns>-1,0,1 according as this is less than, equal to, or greater than obj</returns>
		public int CompareTo(object obj)
		{
			Integer x;
			if (obj is Integer)
				x = (Integer)obj;
			else if (obj is long)
				x = new Integer((long)obj);
			else
				return new Numeric((int)this).CompareTo(obj);
			int n = bytes.Length; // code copied from next routine (shift = 0) following profiling
			int xn = x.bytes.Length;
			int j;
			if (bytes.Length==0)
			{
				if (x.bytes.Length==0)
					return 0;
				if (x.bytes[0]<=127)
					return -1;
				return 1;
			}
			else if (bytes[0]>127)
			{
				if (x.bytes.Length==0)
					return -1;
				if (x.bytes[0]<=127)
					return -1;
				if (n<xn)
					return 1;
				if (n>xn)
					return -1;
				for (j=0;j<n;j++) 
				{
					byte b = (j<xn)?x.bytes[j]:(byte)0;
					if (bytes[j]<b)
						return -1;
					if (bytes[j]>b)
						return 1;
				}
			} 
			else
			{
				if (x.bytes.Length==0)
					return 1;
				if (x.bytes[0]>127)
					return 1;
				if (n<xn)
					return -1;
				if (n>xn)
					return 1;
				for (j=0;j<n;j++) 
				{
					byte b = (j<xn)?x.bytes[j]:(byte)0;
					if (bytes[j]<b)
						return -1;
					if (bytes[j]>b)
						return 1;
				}
			}
			return 0;
		}

#endregion
        /// <summary>
        /// Accessor: Compare with a shifted Integer
        /// </summary>
        /// <param name="x">The Integer to compare</param>
        /// <param name="shift">The number of radix-256 digits to shift</param>
        /// <returns>-1, 0 or 1 as the shifted parameter is less than, equal to, or greater than this</returns>
		internal int CompareTo(Integer x,int shift)
		{
			int n = bytes.Length;
			int xn = x.bytes.Length;
			int j;
			if (bytes.Length==0)
			{
				if (x.bytes.Length==0)
					return 0;
				if (x.bytes[0]<=127)
					return -1;
				return 1;
			}
			else if (bytes[0]>127)
			{
				if (x.bytes.Length==0)
					return -1;
				if (x.bytes[0]<=127)
					return -1;
				if (n<xn+shift)
					return 1;
				if (n>xn+shift)
					return -1;
				for (j=0;j<n;j++) 
				{
					byte b = (j<xn)?x.bytes[j]:(byte)0;
					if (bytes[j]<b)
						return 1;
					if (bytes[j]>b)
						return -1;
				}
			} 
			else
			{
				if (x.bytes.Length==0)
					return 1;
				if (x.bytes[0]>127)
					return 1;
				if (n<xn+shift)
					return -1;
				if (n>xn+shift)
					return 1;
				for (j=0;j<n;j++) 
				{
					byte b = (j<xn)?x.bytes[j]:(byte)0;
					if (bytes[j]<b)
						return -1;
					if (bytes[j]>b)
						return 1;
				}
			}
			return 0;
		}
        /// <summary>
        /// Calculate a power of 2
        /// </summary>
        /// <param name="p">power</param>
        /// <returns>the Integer</returns>
        internal static Integer Pow2(int p)
        {
            byte[] pw = new byte[(p+1) / 8 + 1]; // e.g. 
            for (int j = 0; j < pw.Length; j++)
                pw[j] = 0;
            if (p % 8 == 7)
                pw[1] = 128;
            else
                pw[0] = (byte)(1 << (p % 8));
            return new Integer(pw);
        }
        /// <summary>
        /// Check a bitmask
        /// </summary>
        /// <param name="p">the bit to check</param>
        /// <returns>whether the bit is on</returns>
        internal bool Has(int p)
        {
            if (bytes.Length < (p + 1) / 8 + 1)
                return false;
            if (p % 8 == 7)
                return (bytes[1] & 128) != 0;
            return (bytes[0] & (1 << (p % 8))) != 0;
        }
        /// <summary>
        /// Formal equals operator
        /// </summary>
        /// <param name="obj">Integer to compare</param>
        /// <returns>whether they are equal</returns>
        public override bool Equals(object obj)
        {
            return CompareTo(obj)==0;
        }
        /// <summary>
        /// Formal hash code computation
        /// </summary>
        /// <returns>a hash code</returns>
        public override int GetHashCode()
        {
            return bytes.GetHashCode();
        }
    }
    /// <summary>
    /// An arbitrary-precision decimal class
    /// Decimals are Integers together with an instruction about the decimal point. 
    /// Immutable: no Mutators.
    /// </summary>
    public class Numeric : IComparable
	{
        /// <summary>
        /// Ignore the decimal point for the mantissa
        /// </summary>
		public readonly Integer mantissa;
        /// <summary>
        /// The position of the decimal point (digits from right): 0, no shifting.
        /// </summary>
		public readonly int scale;
        /// <summary>
        /// The number of radix-256 digits to retain for division
        /// sticky: set by Domain when assigned
        /// </summary>
		public readonly int precision;
        /// <summary>
        /// Constructor: given a mantissa and scale
        /// </summary>
        /// <param name="m">The Integer mantissa</param>
        /// <param name="s">The int scale</param>
		public Numeric(Integer m,int s,int p=0)
        {
			mantissa = m;
			scale = s;
            precision = p;
		}
        public Numeric(long m, int s, int p = 0)
        {
            mantissa = new Integer(m);
            scale = s;
            precision = p;
        }
        /// <summary>
        /// Constructor: given a long
        /// </summary>
        /// <param name="n">The long value</param>
		internal Numeric(long n)
		{
			mantissa = new Integer(n);
			scale = 0;
		}
        /// <summary>
        /// Constructor: given a double
        /// </summary>
        /// <param name="q">The double value</param>
		public Numeric(double d)
		{
			Numeric a = Parse(d.ToString());
			mantissa = a.mantissa;
			scale = a.scale;
		}
        /// <summary>
        /// The constant Decimal 0
        /// </summary>
		internal static Numeric Zero = new Numeric(0L);
        /// <summary>
        /// Creator: Remove trailing 0s from the mantissa by adjusting the scale
        /// </summary>
        /// <returns>The new Decimal</returns>
		Numeric Normalise()
		{
			int n = 0;
			Integer m = mantissa;
			if (m.bytes.Length==0)
				return Zero;
			bool sg = m.Sign;
			if (sg)
				m = -m;
            for (; ; )
            {
                int r = 0;
                Integer q = m.Quotient((byte)10, ref r);
                if (r != 0)
                    break;
                m = q;
                if (m.bytes.Length == 0)
                    return Zero;
                n++;
            }
			if (sg)
				m = -m;
			return new Numeric(m,scale-n);
		}
        public static Numeric Ceiling(Numeric x)
        {
            Integer m = x.mantissa;
            bool sg = m.Sign;
            if (sg)
                m = -m;
            Integer d = Integer.Pow10(x.scale);
            Integer n = (m / d) * d;
            if (sg)
                n = -n;
            if (n < x.mantissa)
                n = n + d;
            return new Numeric(n, x.scale);
        }
        public static Numeric Floor(Numeric x)
        {
            Integer m = x.mantissa;
            bool sg = m.Sign;
            if (sg)
                m = -m;
            Integer d = Integer.Pow10(x.scale);
            Integer n = (m / d) * d;
            if (sg)
                n = -n;
            if (n > x.mantissa)
                n = n - d;
            return new Numeric(n, x.scale);
        }
        public Numeric Round(int sc)
        {
            if (scale <= sc)
                return Denormalise(sc - scale);
            Integer m = mantissa;
            bool sg = m.Sign;
            if (sg)
                m = -m;
            Integer d = Integer.Pow10(scale-sc);
            Integer n = (m / d) * d;
            Integer r = m - n;
            if (r.Times(2).CompareTo(d)>=0)
                n = n + d;
            if (sg)
                n = -n;
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
            Integer m = mantissa;
            if (m.bytes.Length != 0)
                for (int j = 0; j < n; j++)
                    m = m.Times10();
            return new Numeric(m, scale + n);
        }
        public static bool operator <(Numeric a, Numeric b)
        {
            return a.CompareTo(b) < 0;
        }
        public static bool operator <=(Numeric a, Numeric b)
        {
            return a.CompareTo(b) <= 0;
        }
        public static bool operator >(Numeric a, Numeric b)
        {
            return a.CompareTo(b) > 0;
        }
        public static bool operator >=(Numeric a, Numeric b)
        {
            return a.CompareTo(b) >= 0;
        }
        public static bool operator ==(Numeric a, Numeric b)
        {
            return a.CompareTo(b) == 0;
        }
        public static bool operator !=(Numeric a, Numeric b)
        {
            return a.CompareTo(b) != 0;
        }
 
        /// <summary>
        /// Creator: - Decimal
        /// </summary>
        /// <param name="a">The Decimal to negate</param>
        /// <returns>The new Decimal</returns>
		public static Numeric operator-(Numeric a)
		{
			return new Numeric(-a.mantissa,a.scale);
		}
        /// <summary>
        /// Addition of two Decimals
        /// </summary>
        /// <param name="a">The first Decimal</param>
        /// <param name="b">The second Decimal</param>
        /// <returns>The sum (a new Decimal)</returns>
		public static Numeric operator+(Numeric a,Numeric b)
		{
			int na = a.scale, nb = b.scale;
			a = a.Denormalise(nb-na);
			b = b.Denormalise(na-nb);
			return new Numeric(a.mantissa+b.mantissa,a.scale).Normalise();
		}
        /// <summary>
        /// Negate a Decimal
        /// </summary>
        /// <param name="a">The Decimal to negate</param>
        /// <returns>The resulting Decimal</returns>
        public Numeric Negate()
        {
            return new Numeric(mantissa.Negate(), scale);
        }
        /// <summary>
        /// Subtraction of two Decimals
        /// </summary>
        /// <param name="a">The first Decimal</param>
        /// <param name="b">The second Decimal</param>
        /// <returns>The difference (a new Decimal)</returns>
		public static Numeric operator-(Numeric a,Numeric b)
		{
			return a+(-b);
		}
        /// <summary>
        /// Multiplication of two Decimals
        /// </summary>
        /// <param name="a">The first Decimal</param>
        /// <param name="b">The second Decimal</param>
        /// <returns>The product (a new Decimal)</returns>
		public static Numeric operator*(Numeric a,Numeric b)
		{
			return new Numeric(a.mantissa*b.mantissa,a.scale+b.scale).Normalise();
		}
        /// <summary>
        /// Creator: Decimal division given a precision
        /// </summary>
        /// <param name="a">The dividend</param>
        /// <param name="b">The divisor</param>
        /// <param name="prec">The precision allowed</param>
        /// <returns>The new quotient</returns>
		internal static Numeric Divide(Numeric a,Numeric b,int prec)
		{
			// we want at least prec bytes in the result, i.e. len(a)-len(b)>prec+1
			if (prec==0)
				prec = 13; // if no precision specified
			a = a.Denormalise(b.mantissa.bytes.Length-a.mantissa.bytes.Length+prec+1);
			return new Numeric(a.mantissa/b.mantissa,a.scale-b.scale).Normalise();
		}
        public static Numeric operator%(Numeric a,Numeric b)
        {
            int p = 0;
            if (a.precision>0)
                p = a.precision;
            if (b.precision>p)
                p = b.precision;
            if (p==0)
                p = 13;
            return a - Divide(a, b, p) * b;
        }
        /// <summary>
        /// Cast: a double from a Decimal
        /// </summary>
        /// <param name="x">The Decimal</param>
        /// <returns>The double value</returns>
		public static implicit operator double(Numeric x)
		{
			return ((double)x.mantissa)*Math.Pow(10.0,-(double)x.scale);
		}
#region IComparable Members
        /// <summary>
        /// Comparison of Decimals
        /// </summary>
        /// <param name="obj">Something to compare with</param>
        /// <returns>-1,0,1 according as this is less than, equal to, greater than obj</returns>
		public int CompareTo(object obj)
		{
			Numeric a = this;
			Numeric b;
			if (obj==null)
				return 1;
            if (obj is int)
                b = new Numeric((long)(int)obj);
            else if (obj is Numeric)
                b = (Numeric)obj;
            else if (obj is double)
                b = new Numeric((double)obj);
            else if (obj is long)
                b = new Numeric((long)obj);
            else if (obj is Integer)
                b = new Numeric((Integer)obj, 0);
            else
                throw new Exception("22201");
			int na = a.scale, nb = b.scale;
			a = Denormalise(nb-na);
			b = b.Denormalise(na-nb);
			return a.mantissa.CompareTo(b.mantissa);
		}

#endregion
        /// <summary>
        /// Conversion to string representation
        /// </summary>
        /// <returns>The string representation of this</returns>
		public override string ToString()
		{
			string m = mantissa.ToString();
			int n = m.Length;
			if (scale==0)
				return m;
			if (scale<0)
				return m+new string('0',-scale);
			if (m[0]=='-' && scale>n-1)
				return "-0."+new string('0',scale-n+1)+m.Substring(1);
			if (scale>=n)
				return "0."+new string('0',scale-n)+m;
			return m.Substring(0,n-scale)+"."+m.Substring(n-scale);
		}
        public string DoubleFormat()
        {
            string m = mantissa.ToString();
            int n = m.Length;
            if (n==1)
                return ""+m[0]+".E"+(n-1-scale);
            return m[0] + "." + m.Substring(1) + "E" + (n-1-scale);
        }

        public bool TryConvert(ref Integer r)
        {
            var n = Normalise();
            if (n.scale > 0)
                return false;
            n = n.Denormalise(-n.scale);
            r = n.mantissa;
            return true;
        }
        public bool TryConvert(ref long r)
        {
            Integer n = new Integer(0);
            if (!TryConvert(ref n))
                return false;
            if (n.bytes.Length > 8)
                return false;
            r = (long)n;
            return true;
        }
        public bool TryConvert(ref int r)
        {
            Integer n = new Integer(0);
            if (!TryConvert(ref n))
                return false;
            if (n.bytes.Length > 4)
                return false;
            r = (int)n;
            return true;
        }
        /// <summary>
        /// Creator: from a string representation
        /// </summary>
        /// <param name="s">The string to parse</param>
        /// <returns>the Decimal value</returns>
		public static Numeric Parse(string s)
		{
			int m = s.IndexOf('e');
			if (m<0)
				m = s.IndexOf('E');
			int exp = 0;
			if (m>0)
			{
				exp = int.Parse(s.Substring(m+1));
				s = s.Substring(0,m);
			}
			int n = s.IndexOf('.');
			if (n<0)
				return new Numeric(Integer.Parse(s),-exp);
			int ln = s.Length;
			return new Numeric(Integer.Parse(s.Substring(0,n)+s.Substring(n+1)),ln-n-1-exp);
		}
        public override bool Equals(object obj)
        {
            return CompareTo(obj)==0;
        }
        public override int GetHashCode()
        {
            return mantissa.GetHashCode();
        }
	}
}
