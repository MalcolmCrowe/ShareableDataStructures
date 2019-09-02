using System;
using System.Text;
using System.Collections;

namespace Tpcc
{
	/// <summary>
	/// Summary description for util.
	/// </summary>
	public class util
	{
		public static int c;
		public int minLen;
		public struct ByteArray
		{
			public byte[] bytes;
			public ByteArray(int n)
			{
				bytes = new byte[n];
			}
			public ByteArray(string n)
			{
				bytes = new ASCIIEncoding().GetBytes(n);
			}
		}
		public ByteArray[] strx,stry;
		static int a_c_last =255,a_c_id =1023,a_ol_i_id=8191;
		static int c_c_last,c_c_id,c_ol_i_id;
		static util()
		{
			nameBits = new ByteArray[10];
			nameBits[0] = new ByteArray("BAR");
			nameBits[1] = new ByteArray("OUGHT");
			nameBits[2] = new ByteArray("ABLE");
			nameBits[3] = new ByteArray("PRI");
			nameBits[4] = new ByteArray("PRES");
			nameBits[5] = new ByteArray("ESE");
			nameBits[6] = new ByteArray("ANTI");
			nameBits[7] = new ByteArray("CALLY");
			nameBits[8] = new ByteArray("ATION");
			nameBits[9] = new ByteArray("EING");
			rnd = new Random(0);
			c_c_last = random(0,a_c_last);
			c_c_id = random(0,a_c_id);
			c_ol_i_id = random(0,a_ol_i_id);
		}
		public util(int x,int y)
		{
			minLen = x;
			strx = new ByteArray[10];
			stry = new ByteArray[10];
			for (int j=0;j<10;j++)
			{
				strx[j] = new ByteArray(x);
				stry[j] = new ByteArray(random(0,y-x));
				int k;
				for (k=0;k<x;k++)
					strx[j].bytes[k]=randchar();
				for (k=0;k<stry[j].bytes.Length;k++)
					stry[j].bytes[k]=randchar();
			}
		}
		public byte[] NextAString() // new util(x,y) sets up a generator for random strings(x..y). NextAString() gives a random string
		{							// from this sequence
			byte[] a = strx[rnd.Next(0,9)].bytes;
			byte[] b = stry[rnd.Next(0,9)].bytes;
			int n = b.Length;
			byte[] r = new byte[minLen+n];
			for (int j=0;j<minLen;j++)
				r[j] = a[j];
			for (int j=0;j<n;j++)
				r[j+minLen] = b[j];
			return r;
		}
		static ByteArray[] nameBits;
		public static byte[] Surname(int m)
		{
			byte[] a = nameBits[m/100].bytes;
			byte[] b = nameBits[m/10%10].bytes;
			byte[] c = nameBits[m%10].bytes;
			byte[] r = new byte[a.Length+b.Length+c.Length];
			int n,j;
			for (j=0,n=0;j<a.Length;j++)
				r[n++] = a[j];
			for (j=0;j<b.Length;j++)
				r[n++] = b[j];
			for (j=0;j<c.Length;j++)
				r[n++] = c[j];
			return r;
		}
		public static byte[] NextLast(int n)
		{
			if (n<1000)
				return Surname(n);
			else
				return Surname(NURandCLast());
		}
		public static int[] Permute(int n) // gives random permutation of 0..n-1
		{
			bool[] a = new bool[n];
			for (int j=0;j<n;j++)
				a[j] = false;
			int[] r = new int[n];
			int m=n;
			while (m>0)
			{
				int b = rnd.Next(n);
				if (!a[b])
				{
					r[--m] = b;
					a[b] = true;
				}
			}
			return r;
		}
		public static byte[] NZip()
		{
			byte[] r = new byte[8];
			for (int j=0;j<4;j++)
				r[j] = (byte)rnd.Next(48,58);
			for (int j=4;j<8;j++)
				r[j] = (byte)49;
			return r;
		}
		public static byte[] NString(int ln)
		{
			byte[] r = new byte[ln];
			bool in0 = true;
			for (int j=0;j<ln;j++)
			{
				int d  = rnd.Next(48,58);
				r[j] = (byte)d;
				if (d==48 && in0)
					r[j] = (byte)32;
				else
					in0 = false;
			}
			return r;
		}
		public static byte[] NextNString(int min,int max,int scale)
		{
			int k=0,n = rnd.Next(min,max);
			ArrayList a = new ArrayList();
			while (n>0)
			{
				a.Add(n%10);
				n = n/10;
			}
			n = a.Count;
			byte[] r;
			if (n<=scale)
			{
				r = new byte[scale+2];
				r[k++] = (byte)'0';
				r[k++] = (byte)'.';
				for (int j=0;j<scale-n;j++)
					r[k++] = (byte)'0';
				for (int j=n-1;j>=0;j--)
					r[k++] = (byte)(((int)a[j])+48);
			}
			else if (scale>0)
			{
				r = new byte[n+1];
				for (int j=n-1;j>=0;j--)
				{
					r[k++] = (byte)(((int)a[j])+48);
					if (j==scale)
						r[k++] = (byte)'.';
				}
			} 
			else
			{
				r = new byte[n];
				for (int j=n-1;j>=0;j--)
					r[k++] = (byte)(((int)a[j])+48);
			}
			return r;
		}
		static ByteArray orig = new ByteArray("ORIGINAL");
		public static byte[] fixStockData(byte[] s)
		{
			int n=rnd.Next(1,10);
			if (n!=1)
				return s;
			n = rnd.Next(0,s.Length-9);
			for (int j=0;j<8;j++)
				s[j+n] = orig.bytes[j];
			return s;
		}
		static byte _lastchar = 32;
		static Random rnd;
		public static byte randchar()
		{
			_lastchar = (byte)rnd.Next((_lastchar==32)?65:60,90);
			if (_lastchar<65) _lastchar = 32;
			return _lastchar;
		}
		public static byte[] randchar(int n)
		{
			byte[] r = new byte[n];
			for (int j=0;j<n;j++)
				r[j] = randchar();
			return r;
		}
		public static int random(int x,int y)
		{
			return rnd.Next(x,y);
		}
		public static int random(int x,int y,int z)
		{
			// between x and y but not =z : presume x<=z<=y
			int r = random(x,y-1);
			if (r>=z)
				r++;
			return r;
		}
		public static int NURand(int a,int c,int x,int y)
		{
			return (((random(0,a)|random(x,y)+c)%(y-x+1))+x);
		}
		public static int NURandCLast()
		{
			return NURand(a_c_last,c_c_last,0,999);
		}
		public static int NURandCID()
		{
			return NURand(a_c_id,c_c_id,1,3000);
		}
		public static int NURandOLID()
		{
			return NURand(a_ol_i_id,c_ol_i_id,1,100000);
		}
/*		public static void Main(string[] args)
		{
			for (;;) 
			{
				byte[] b = util.NextNString(0,2000,2);
				string s = new ASCIIEncoding().GetString(b);
				Console.WriteLine(s);
				Console.Read();
			}
		} */
	}
}
