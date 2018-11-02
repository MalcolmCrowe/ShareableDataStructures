using System;
using System.Collections;
using System.Text;
using Shareable;
using StrongLink;

namespace Tpcc
{
    class Program
    {
        static StrongConnect conn;
        static Encoding enc = new ASCIIEncoding();
        static void Main(string[] args)
        {
            conn = new StrongConnect("127.0.0.1", 50433, "Tpcc");
        }
        public void BuildTpcc()
        {
            CreationScript();
            FillItems();
        }
        public void CreationScript()
        {
            conn.CreateTable("WAREHOUSE",
                new SColumn("W_ID", Types.SInteger),
                new SColumn("W_NAME", Types.SString),
                new SColumn("W_STREET_1", Types.SString),
                new SColumn("W_STREET_2", Types.SString),
                new SColumn("W_CITY", Types.SString),
                new SColumn("W_STATE", Types.SString),
                new SColumn("W_ZIP", Types.SString),
                new SColumn("W_TAX", Types.SNumeric),
                new SColumn("W_YTD", Types.SNumeric)
            );
            conn.CreateIndex("WAREHOUSE", IndexType.Primary, null, "W_ID");
            conn.CreateTable("DISTRICT",
                new SColumn("D_ID", Types.SInteger),
                new SColumn("D_W_ID", Types.SInteger),
                new SColumn("D_NAME", Types.SString),
                new SColumn("D_STREET_1", Types.SString),
                new SColumn("D_STREET_2", Types.SString),
                new SColumn("D_CITY", Types.SString),
                new SColumn("D_STATE", Types.SString),
                new SColumn("D_ZIP", Types.SString),
                new SColumn("D_TAX", Types.SNumeric),
                new SColumn("D_YTD", Types.SNumeric),
                new SColumn("D_NEXT_O_ID", Types.SInteger)
                );
            conn.CreateIndex("DISTRICT", IndexType.Primary, null, "D_W_ID,", "D_ID");
            conn.CreateIndex("DISTRICT", IndexType.Reference, "WAREHOUSE", "D_W_ID");
            conn.CreateTable("CUSTOMER",
                new SColumn("C_ID", Types.SInteger),
                new SColumn("C_D_ID", Types.SInteger),
                new SColumn("C_W_ID", Types.SInteger),
                new SColumn("C_FIRST", Types.SString),
                new SColumn("C_MIDDLE", Types.SString),
                new SColumn("C_LAST", Types.SString),
                new SColumn("C_STREET_1", Types.SString),
                new SColumn("C_STREET_2", Types.SString),
                new SColumn("C_CITY", Types.SString),
                new SColumn("C_STATE", Types.SString),
                new SColumn("C_ZIP", Types.SString),
                new SColumn("C_PHONE", Types.SString),
                new SColumn("C_SINCE", Types.SDate),
                new SColumn("C_CREDIT", Types.SString),
                new SColumn("C_CREDIT_LIM", Types.SNumeric),
                new SColumn("C_DISCOUNT", Types.SNumeric),
                new SColumn("C_BALANCE", Types.SNumeric),
                new SColumn("C_YTD_PAYMENT", Types.SNumeric),
                new SColumn("C_PAYMENT_CNT", Types.SNumeric),
                new SColumn("C_DELIVERY_CNT", Types.SNumeric),
                new SColumn("C_DATA", Types.SString)
                );
            conn.CreateIndex("CUSTOMER", IndexType.Primary, null, "C_W_ID", "C_D_ID", "C_ID");
            conn.CreateIndex("CUSTOMER", IndexType.Reference, "DISTRICT", "C_W_ID", "C_D_ID");
            conn.CreateTable("HISTORY",
                new SColumn("H_C_ID", Types.SInteger),
                new SColumn("H_C_D_ID", Types.SInteger),
                new SColumn("H_C_W_ID", Types.SInteger),
                new SColumn("H_D_ID", Types.SInteger),
                new SColumn("H_W_ID", Types.SInteger),
                new SColumn("H_DATE", Types.SDate),
                new SColumn("H_AMOUNT", Types.SNumeric),
                new SColumn("H_DATA", Types.SString)
                );
            conn.CreateIndex("HISTORY", IndexType.Reference, "CUSTOMER", "H_C_W_ID", "H_C_D_ID", "H_C_ID");
            conn.CreateIndex("HISTORY", IndexType.Reference, "DISTRICT", "H_W_ID", "H_D_ID");
            conn.CreateTable("ORDER",
                new SColumn("O_ID", Types.SInteger),
                new SColumn("O_D_ID", Types.SInteger),
                new SColumn("O_W_ID", Types.SInteger),
                new SColumn("O_C_ID", Types.SInteger),
                new SColumn("O_ENTRY_D", Types.SDate),
                new SColumn("O_CARRIER_ID", Types.SInteger),
                new SColumn("O_OL_CNT", Types.SInteger),
                new SColumn("O_ALL_LOCAL", Types.SNumeric)
                );
            conn.CreateIndex("ORDER", IndexType.Primary, null, "O_W_ID", "O_D_ID", "O_ID");
            conn.CreateIndex("ORDER", IndexType.Reference, "CUSTOMER", "O_W_ID", "O_D_ID", "O_C_ID");
            conn.CreateTable("NEW_ORDER",
                new SColumn("NO_O_ID", Types.SInteger),
                new SColumn("NO_D_ID", Types.SInteger),
                new SColumn("NO_W_ID", Types.SInteger)
                );
            conn.CreateIndex("NEW_ORDER", IndexType.Primary, null, "NO_W_ID", "NO_D_ID", "NO_O_ID");
            conn.CreateIndex("NEW_ORDER", IndexType.Reference, "ORDER", "NO_W_ID", "NO_D_ID", "NO_O_ID");
            conn.CreateTable("ITEM",
                new SColumn("I_ID", Types.SInteger),
                new SColumn("I_IM_ID", Types.SInteger),
                new SColumn("I_NAME", Types.SString),
                new SColumn("I_PRICE", Types.SNumeric),
                new SColumn("I_DATA", Types.SString)
                );
            conn.CreateIndex("ITEM", IndexType.Primary, null, "I_ID");
            conn.CreateTable("STOCK",
                new SColumn("S_I_ID", Types.SInteger),
                new SColumn("S_W_ID", Types.SInteger),
                new SColumn("S_QUANTITY", Types.SNumeric),
                new SColumn("S_DIST_01", Types.SString),
                new SColumn("S_DIST_02", Types.SString),
                new SColumn("S_DIST_03", Types.SString),
                new SColumn("S_DIST_04", Types.SString),
                new SColumn("S_DIST_05", Types.SString),
                new SColumn("S_DIST_06", Types.SString),
                new SColumn("S_DIST_07", Types.SString),
                new SColumn("S_DIST_08", Types.SString),
                new SColumn("S_DIST_09", Types.SString),
                new SColumn("S_DIST_10", Types.SString),
                new SColumn("S_YTD", Types.SNumeric),
                new SColumn("S_ORDER_CNT", Types.SInteger),
                new SColumn("S_REMOTE_CNT", Types.SInteger),
                new SColumn("S_DATA", Types.SString)
                );
            conn.CreateIndex("STOCK", IndexType.Primary, null, "S_W_ID", "S_I_ID");
            conn.CreateIndex("STOCK", IndexType.Reference, "ITEM", "S_I_ID");
            conn.CreateIndex("STOCK", IndexType.Reference, "WAREHOUSE", "S_W_ID");
            conn.CreateTable("ORDER_LINE",
                new SColumn("OL_O_ID", Types.SInteger),
                new SColumn("OL_D_ID", Types.SInteger),
                new SColumn("OL_W_ID", Types.SInteger),
                new SColumn("OL_NUMBER", Types.SInteger),
                new SColumn("OL_I_ID", Types.SInteger),
                new SColumn("OL_SUPPLY_W_ID", Types.SInteger),
                new SColumn("OL_DELIVERY_D", Types.SDate),
                new SColumn("OL_QUANTITY", Types.SNumeric),
                new SColumn("OL_AMOUNT", Types.SNumeric),
                new SColumn("OL_DIST_INFO", Types.SString)
                );
            conn.CreateIndex("ORDER_LINE", IndexType.Primary, null, "OL_W_ID", "OL_D_ID", "OL_O_ID", "OL_NUMBER");
            conn.CreateIndex("ORDER_LINE", IndexType.Reference, "ORDER", "OL_W_ID", "OL_D_ID", "OL_O_ID");
            conn.CreateIndex("ORDER_LINE", IndexType.Reference, "STOCK", "OL_SUPPLY_W_ID", "OL_I_ID");
            conn.CreateTable("DELIVERY",
                new SColumn("DL_W_ID", Types.SInteger),
                new SColumn("DL_ID", Types.SInteger),
                new SColumn("DL_CARRIER_ID", Types.SInteger),
                new SColumn("DL_DONE", Types.SInteger),
                new SColumn("DL_SKIPPED", Types.SInteger)
                );
            conn.CreateIndex("DELIVERY", IndexType.Primary, null, "DL_W_ID", "DL_ID");

        }
        SString NextData()
        {
            byte[] b = uData.NextAString();
            int n = util.random(0, 10);
            if (n == 0)
            {
                byte[] orig = enc.GetBytes("ORIGINAL");
                n = util.random(0, b.Length - 8);
                for (int j = 0; j < 8; j++)
                    b[j + n] = orig[j];
            }
            return GetString(b);
        }
        SString GetString(byte[] b)
        {
            return new SString(enc.GetString(b));
        }
        util uData = new util(26, 50);
        public void FillItems()
        {
            util ut = new util(14, 24);
            var cols = new string[] { "I_ID", "I_IM_ID", "I_NAME", "I_PRICE", "I_DATA" };
#if TRY
            for (int j=1;j<=10;j++)
#else
            for (int j = 1; j <= 100000; j++)
#endif
                conn.Insert("ITEM", cols, new Serialisable[][] { new Serialisable[] {
#if TRY
                    new SInteger(j),new SNumeric(util.random(1,10)),
#else
                    new SInteger(j), new SInteger(util.random(1, 10000)),
#endif
                    GetString(ut.NextAString()),
                    GetString(util.NextNString(100, 10000, 2)),
                    NextData() } });
        }
        public void FillWarehouse(int w)
        {
            util u1 = new util(6, 10);
            util u2 = new util(10, 20);
            var cols = new string[] {"W_ID","W_NAME","W_STREET_1","W_STREET_2",
            "W_CITY","W_STATE","W_ZIP","W_TAX","W_YTD"};
            Console.WriteLine("Starting warehouse " + w);
            conn.Insert("WAREHOUSE", cols, new Serialisable[][] { new Serialisable[] {
                new SInteger(w), GetString(u1.NextAString()),
                GetString(u2.NextAString()),
                GetString(u2.NextAString()),
                GetString(u2.NextAString()),
                new SString(""+(char)util.random(65, 90) + (char)util.random(65, 90)),
                GetString(util.NZip()),
                GetString(util.NextNString(0, 2000, 4)),
                new SNumeric(30000000,12,2) } });
            FillStock(w);
        }
        public void FillStock(int wid)
        {
            Console.WriteLine("filling stock");
            var cols = new string[] {  "S_I_ID","S_W_ID","S_QUANTITY",
                "S_DIST_01","S_DIST_02","S_DIST_03","S_DIST_04","S_DIST_05",
                "S_DIST_06","S_DIST_07","S_DIST_08","S_DIST_09","S_DIST_10",
                "S_YTD","S_ORDER_CNT","S_REMOTE_CNT","S_DATA" };
#if TRY
            for (int siid=1;siid<=10;siid++)
#else
            for (int siid = 1; siid <= 100000; siid++)
#endif
                conn.Insert("STOCK", cols, new Serialisable[][] { StockVals(siid, wid) });
        }
        Serialisable[] StockVals(int siid, int wid)
        {
            util u = new util(26, 50);
            var r = new Serialisable[17];
            r[0] = new SInteger(siid);
            r[1] = new SInteger(wid);
            r[2] = new SNumeric(util.random(10, 100), 4, 0);
            for (var i = 3; i < 13; i++)
                r[i] = GetString(util.randchar(24));
            r[13] = SInteger.Zero;
            r[14] = SInteger.Zero;
            r[15] = SInteger.Zero;
            r[16] = GetString(util.fixStockData(u.NextAString()));
            return r;
        }
        public void FillDistricts(int wid)
        {
            for (int did = 1; did <= 10; did++)
                FillDistrict(wid, did);
        }
        public void FillDistrict(int wid, int did)
        {
            util us = new util(10, 20);
            util un = new util(6, 10);
            var cols = new string[] { "D_ID","D_W_ID","D_NAME","D_STREET_1","D_STREET_2",
            "D_CITY","D_STATE","D_ZIP","D_TAX","D_YTD","D_NEXT_O_ID" };
            conn.Insert("DISTRICT", cols, new Serialisable[][] { new Serialisable[] {
                new SInteger(did),new SInteger(wid),
                GetString(un.NextAString()),
                GetString(un.NextAString()),
                GetString(un.NextAString()),
                GetString(un.NextAString()),
                new SString(""+ (char)util.random(65, 90) + (char)util.random(65, 90)),
                new SNumeric(300000,12,2), new SInteger(3001)
            } });
            FillCustomer(wid, did);
            FillOrder(wid, did);
        }
        public void FillCustomer(int wid, int did)
        {
            Console.WriteLine("starting customer (" + wid + ") using " + did);
            util uf = new util(8, 16);
            util us = new util(10, 20);
            util ud = new util(300, 500);
            util uh = new util(12, 24);
            var ccols = new string[] { "C_ID", "C_D_ID", "C_W_ID", "C_FIRST", "C_MIDDLE", "C_LAST",
                "C_STREET_1","C_STREET_2","C_CITY","C_STATE","C_ZIP","C_PHONE",
                "C_SINCE","C_CREDIT","C_CREDIT_LIM","C_DISCOUNT","C_BALANCE",
                "C_YTD_PAYMENT","C_PAYMENT_CNT","C_DELIVERY_CNT","C_DATA"
            };
            var hcols = new string[] { "H_C_ID", "H_C_D_ID", "H_C_W_ID", "H_D_ID",
                "H_W_ID", "H_DATE", "H_AMOUNT", "H_DATA"
            };
#if TRY
            for (int cid=1;cid<=3;cid++)
#else
            for (int cid = 1; cid <= 3000; cid++)
#endif
            {
                conn.Insert("CUSTOMER", ccols, new Serialisable[][] { new Serialisable[] {
                    new SInteger(cid),new SInteger(did),new SInteger(wid),
                    GetString(util.NextLast(cid)),new SString("OE"),
                    GetString(uf.NextAString()),
                    GetString(uf.NextAString()),
                    GetString(uf.NextAString()),
                    GetString(uf.NextAString()),
                    new SString(""+(char)util.random(65, 90) + (char)util.random(65, 90)),
                    GetString(util.NZip()), GetString(util.NString(16)),
                    new SDate(DateTime.Now),new SString(credit()),new SNumeric(500000,12,2),
                    new SNumeric(util.random(0,5000),4,4),
                    new SNumeric(-1000,12,2),
                    new SNumeric(1000,12,2),
                    new SNumeric(1,4,0),
                    new SNumeric(0,4,0),
                    GetString(ud.NextAString())
                } });
                conn.Insert("HISTORY", hcols, new Serialisable[][] { new Serialisable[] {
                    new SInteger(cid), new SInteger(did), new SInteger(wid),
                    new SInteger(did), new SInteger(wid), new SDate(DateTime.Now),
                    new SNumeric(1000,6,2), GetString(uh.NextAString())
                } });
            }
        }
        static string credit()
        {
            int n = util.random(1, 10);
            if (n == 1)
                return "BC";
            else
                return "GC";
        }
        void FillOrder(int wid, int did)
        {
            var ocols = new string[] { "O_ID", "O_D_ID", "O_W_ID", "O_C_ID",
                "O_ENTRY_D","O_CARRIER_ID","O_OL_CNT","O_ALL_LOCAL" };
            var ncols = new string[] { "NO_O_ID", "NO_D_ID", "NO_W_ID" };
#if TRY
            int[] perm = util.Permute(3);//3000);
			for (int oid=1;oid<=3;oid++)//3000;oid++)
#else
            int[] perm = util.Permute(3000);
            for (int oid = 1; oid <= 3000; oid++)
#endif
            {
                int cnt = util.random(5, 15);
                conn.Insert("ORDER", ocols, new Serialisable[][] { new Serialisable[] {
                        new SInteger(oid),new SInteger(perm[oid - 1] + 1),new SInteger(did),
                        new SInteger(wid),new SDate(DateTime.Now),new SInteger(cnt),new SInteger(1)
                    } });
                if (oid > 2100)
                    conn.Insert("NEW_ORDER", ncols, new Serialisable[][] { new Serialisable[] {
                        new SInteger(oid),new SInteger(did),new SInteger(wid)
                    } });
                FillOrderLine(wid, did, oid, cnt);
            }
        }
        void FillOrderLine(int wid, int did, int oid, int cnt)
        {
            var cols = new string[] { "OL_O_ID", "OL_D_ID", "OL_W_ID", "OL_NUMBER",
            "OL_I_ID", "OL_SUPPLY_W_ID", "OL_DELIVERY_D", "OL_QUANTITY", "OL_AMOUNT","OL_DIST_INFO"};
            for (int j = 1; j <= cnt; j++)
                if (oid < 2101)
                    conn.Insert("ORDER_LINE", cols, new Serialisable[][] { new Serialisable[] {
                        new SInteger(oid),new SInteger(did),new SInteger(wid), new SInteger(j),
#if TRY
                        new SInteger(util.random(1, 10)),
#else
                        new SInteger(util.random(1, 100000)),
#endif
                        new SInteger(wid),
                        new SDate(DateTime.Now),new SNumeric(5,2,0), new SNumeric(0,6,2),
                        GetString(util.randchar(24))
                    } });
                else
                    conn.Insert("ORDER_LINE", cols, new Serialisable[][] { new Serialisable[] {
                        new SInteger(oid),new SInteger(did),new SInteger(wid), new SInteger(j),
#if TRY
                        new SInteger(util.random(1, 10)),
#else
                        new SInteger(util.random(1, 100000)),
#endif
                        new SInteger(wid),new SDate(DateTime.Now),new SNumeric(5,2,0),
                        new SNumeric(util.random(1,999999),6,2),GetString(util.randchar(24))
                    } });
        }
    }
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
        public ByteArray[] strx, stry;
        static int a_c_last = 255, a_c_id = 1023, a_ol_i_id = 8191;
        static int c_c_last, c_c_id, c_ol_i_id;
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
            rnd = new Random();
            c_c_last = random(0, a_c_last);
            c_c_id = random(0, a_c_id);
            c_ol_i_id = random(0, a_ol_i_id);
        }
        public util(int x, int y)
        {
            minLen = x;
            strx = new ByteArray[10];
            stry = new ByteArray[10];
            for (int j = 0; j < 10; j++)
            {
                strx[j] = new ByteArray(x);
                stry[j] = new ByteArray(random(0, y - x));
                int k;
                for (k = 0; k < x; k++)
                    strx[j].bytes[k] = randchar();
                for (k = 0; k < stry[j].bytes.Length; k++)
                    stry[j].bytes[k] = randchar();
            }
        }
        public byte[] NextAString() // new util(x,y) sets up a generator for random strings(x..y). NextAString() gives a random string
        {                           // from this sequence
            byte[] a = strx[rnd.Next(0, 9)].bytes;
            byte[] b = stry[rnd.Next(0, 9)].bytes;
            int n = b.Length;
            byte[] r = new byte[minLen + n];
            for (int j = 0; j < minLen; j++)
                r[j] = a[j];
            for (int j = 0; j < n; j++)
                r[j + minLen] = b[j];
            return r;
        }
        static ByteArray[] nameBits;
        public static byte[] Surname(int m)
        {
            byte[] a = nameBits[m / 100].bytes;
            byte[] b = nameBits[m / 10 % 10].bytes;
            byte[] c = nameBits[m % 10].bytes;
            byte[] r = new byte[a.Length + b.Length + c.Length];
            int n, j;
            for (j = 0, n = 0; j < a.Length; j++)
                r[n++] = a[j];
            for (j = 0; j < b.Length; j++)
                r[n++] = b[j];
            for (j = 0; j < c.Length; j++)
                r[n++] = c[j];
            return r;
        }
        public static byte[] NextLast(int n)
        {
            if (n < 1000)
                return Surname(n);
            else
                return Surname(NURandCLast());
        }
        public static int[] Permute(int n) // gives random permutation of 0..n-1
        {
            bool[] a = new bool[n];
            for (int j = 0; j < n; j++)
                a[j] = false;
            int[] r = new int[n];
            int m = n;
            while (m > 0)
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
            for (int j = 0; j < 4; j++)
                r[j] = (byte)rnd.Next(48, 58);
            for (int j = 4; j < 8; j++)
                r[j] = (byte)49;
            return r;
        }
        public static byte[] NString(int ln)
        {
            byte[] r = new byte[ln];
            bool in0 = true;
            for (int j = 0; j < ln; j++)
            {
                int d = rnd.Next(48, 58);
                r[j] = (byte)d;
                if (d == 48 && in0)
                    r[j] = (byte)32;
                else
                    in0 = false;
            }
            return r;
        }
        public static byte[] NextNString(int min, int max, int scale)
        {
            int k = 0, n = rnd.Next(min, max);
            ArrayList a = new ArrayList();
            while (n > 0)
            {
                a.Add(n % 10);
                n = n / 10;
            }
            n = a.Count;
            byte[] r;
            if (n <= scale)
            {
                r = new byte[scale + 2];
                r[k++] = (byte)'0';
                r[k++] = (byte)'.';
                for (int j = 0; j < scale - n; j++)
                    r[k++] = (byte)'0';
                for (int j = n - 1; j >= 0; j--)
                    r[k++] = (byte)(((int)a[j]) + 48);
            }
            else if (scale > 0)
            {
                r = new byte[n + 1];
                for (int j = n - 1; j >= 0; j--)
                {
                    r[k++] = (byte)(((int)a[j]) + 48);
                    if (j == scale)
                        r[k++] = (byte)'.';
                }
            }
            else
            {
                r = new byte[n];
                for (int j = n - 1; j >= 0; j--)
                    r[k++] = (byte)(((int)a[j]) + 48);
            }
            return r;
        }
        static ByteArray orig = new ByteArray("ORIGINAL");
        public static byte[] fixStockData(byte[] s)
        {
            int n = rnd.Next(1, 10);
            if (n != 1)
                return s;
            n = rnd.Next(0, s.Length - 9);
            for (int j = 0; j < 8; j++)
                s[j + n] = orig.bytes[j];
            return s;
        }
        static byte _lastchar = 32;
        static Random rnd;
        public static byte randchar()
        {
            _lastchar = (byte)rnd.Next((_lastchar == 32) ? 65 : 60, 90);
            if (_lastchar < 65) _lastchar = 32;
            return _lastchar;
        }
        public static byte[] randchar(int n)
        {
            byte[] r = new byte[n];
            for (int j = 0; j < n; j++)
                r[j] = randchar();
            return r;
        }
        public static int random(int x, int y)
        {
            return rnd.Next(x, y);
        }
        public static int random(int x, int y, int z)
        {
            // between x and y but not =z : presume x<=z<=y
            int r = random(x, y - 1);
            if (r >= z)
                r++;
            return r;
        }
        public static int NURand(int a, int c, int x, int y)
        {
            return (((random(0, a) | random(x, y) + c) % (y - x + 1)) + x);
        }
        public static int NURandCLast()
        {
            return NURand(a_c_last, c_c_last, 0, 999);
        }
        public static int NURandCID()
        {
            return NURand(a_c_id, c_c_id, 1, 3000);
        }
        public static int NURandOLID()
        {
            return NURand(a_ol_i_id, c_ol_i_id, 1, 100000);
        }
    }
}

