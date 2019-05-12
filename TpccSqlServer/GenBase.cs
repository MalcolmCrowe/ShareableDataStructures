using System;
using System.Collections;
using System.Text;
using System.Data.SqlClient;

namespace Tpcc
{
    public class GenBase
    {
        public SqlConnection conn;
        static Encoding enc = new ASCIIEncoding();
        public GenBase(SqlConnection c)
        {
            conn = c;
        }
        public void BuildTpcc()
        {
            CreationScript();
            FillItems();
        }
        public void CreationScript()
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText="create table WAREHOUSE(" +
                "W_ID int primary key,W_NAME varchar(10),W_STREET_1 varchar(20)," +
                "W_STREET_2 varchar(20),W_CITY varchar(20),W_STATE char(2)," +
                "W_ZIP char(9),W_TAX numeric(4,4),W_YTD numeric(12,2))";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table DISTRICT(D_ID int," +
                "D_W_ID int references WAREHOUSE(W_ID),D_NAME varchar(10),D_STREET_1 varchar(20)," +
                "D_STREET_2 varchar(20),D_CITY varchar(20),D_STATE char(2)," +
                "D_ZIP char(9),D_TAX numeric(4,4),D_YTD numeric(12,2),D_NEXT_O_ID int," +
                "primary key (D_W_ID,D_ID))";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CUSTOMER(" +
                "C_ID int,C_D_ID int,C_W_ID int,"+
                "C_FIRST varchar(16),C_MIDDLE char(2),C_LAST varchar(16)," +
                "C_STREET_1 varchar(20),C_STREET_2 varchar(20),C_CITY varchar(20)," +
                "C_STATE char(2),C_ZIP char(9),C_PHONE varchar(16),C_SINCE date," +
                "C_CREDIT char(2),C_CREDIT_LIM numeric(12,2),C_DISCOUNT numeric(4,4),"+
                "C_BALANCE numeric(12,2),C_YTD_PAYMENT numeric(12,2),C_PAYMENT_CNT numeric(4),"+
                "C_DELIVERY_CNT numeric(4),C_DATA varchar(500),"+
                "primary key(C_W_ID,C_D_ID,C_ID),"+
                "foreign key(C_W_ID,C_D_ID) references DISTRICT(D_W_ID,D_ID))";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table HISTORY(" +
                "H_C_ID int,H_C_D_ID int,H_C_W_ID int,"+
                "H_D_ID int,H_W_ID int,H_DATE date,"+
                "H_AMOUNT numeric(6,2),H_DATA varchar(24),"+
                "foreign key (H_C_W_ID, H_C_D_ID, H_C_ID) references CUSTOMER(C_W_ID,C_D_ID,C_ID),"+
                "foreign key (H_W_ID,H_D_ID) references DISTRICT(D_W_ID,D_ID))";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table [ORDER](O_ID int," +
                "O_D_ID int,O_W_ID int,O_C_ID int,O_ENTRY_D date,"+
                "O_CARRIER_ID int,O_OL_CNT int,O_ALL_LOCAL bit,"+
                "primary key(O_W_ID,O_D_ID,O_ID),"+
                "foreign key(O_W_ID, O_D_ID,O_C_ID) references CUSTOMER(C_W_ID,C_D_ID,C_ID))";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table NEW_ORDER(" +
                "NO_O_ID int,NO_D_ID int,NO_W_ID int,"+
                "primary key(NO_W_ID, NO_D_ID, NO_O_ID),"+
                "foreign key(NO_W_ID, NO_D_ID, NO_O_ID) references [ORDER](O_W_ID,O_D_ID,O_ID))";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table ITEM(" +
                "I_ID int primary key,I_IM_ID int,I_NAME varchar(24),I_PRICE numeric(5,2),"+
                "I_DATA varchar(50))";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table STOCK(" +
                "S_I_ID int references ITEM(I_ID),S_W_ID int references WAREHOUSE(W_ID),S_QUANTITY numeric(4)," +
                "S_DIST_01 char(24),S_DIST_02 char(24),S_DIST_03 char(24)," +
                "S_DIST_04 char(24),S_DIST_05 char(24),S_DIST_06 char(24)," +
                "S_DIST_07 char(24),S_DIST_08 char(24),S_DIST_09 char(24)," +
                "S_DIST_10 char(24),S_YTD numeric(8),S_ORDER_CNT numeric(4)," +
                "S_REMOTE_CNT numeric(4),S_DATA varchar(50)," +
                "primary key(S_W_ID,S_I_ID))";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table ORDER_LINE(" +
                "OL_O_ID int,OL_D_ID int,OL_W_ID int,"+
                "OL_NUMBER int,OL_I_ID int,OL_SUPPLY_W_ID int,"+
                "OL_DELIVERY_D date,OL_QUANTITY numeric(2),OL_AMOUNT numeric(6,2),"+
                "OL_DIST_INFO char(24)," +
                "primary key(OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER),"+
                "foreign key(OL_W_ID,OL_D_ID,OL_O_ID) references [ORDER](O_W_ID,O_D_ID,O_ID),"+
                "foreign key(OL_SUPPLY_W_ID,OL_I_ID) references STOCK(S_W_ID,S_I_ID))";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table DELIVERY(" +
                "DL_W_ID int,DL_ID int,DL_CARRIER_ID int," +
                "DL_DONE int,DL_SKIPPED int,"+
                "primary key(DL_W_ID,DL_ID))";
            cmd.ExecuteNonQuery();
        }
        string NextData()
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
        string GetString(byte[] b)
        {
            return "'"+enc.GetString(b)+"'";
        }
        util uData = new util(26, 50);
        public void FillItems()
        {
            Console.WriteLine("Adding Items: "+DateTime.Now);
            util ut = new util(14, 24);
#if TRY
            for (int j=1;j<=10;j++)
#else
            for (int j = 1; j <= 100000; j++)
            {
                var cmd = conn.CreateCommand();
#endif
                cmd.CommandText ="insert into ITEM(I_ID,I_IM_ID,I_NAME,I_PRICE,I_DATA)" +
                    " values(" +
#if TRY
                    j+","+util.random(1,10)+
#else
                    j + "," + util.random(1, 10000) +
#endif
                    "," + GetString(ut.NextAString()) + "," +
                    util.NextNString(100, 10000, 2) + "," + NextData() + ")";
                cmd.ExecuteNonQuery();
            }
            Console.WriteLine("Items done: Fill stock?"+DateTime.Now);
        }
        public void FillWarehouse(int w)
        {
            util u1 = new util(6, 10);
            util u2 = new util(10, 20);
            Console.WriteLine("Starting warehouse " + w+ " " + DateTime.Now);
            var cmd = conn.CreateCommand();
            cmd.CommandText="insert into WAREHOUSE(W_ID,W_NAME,W_STREET_1,W_STREET_2,"+
            "W_CITY,W_STATE,W_ZIP,W_TAX,W_YTD) values(" +
                w+"," + GetString(u1.NextAString())+","+
                GetString(u2.NextAString())+","+
                GetString(u2.NextAString())+","+
                GetString(u2.NextAString())+",'"+
                (char)util.random(65, 90) + (char)util.random(65, 90)+"',"+
                GetString(util.NZip())+","+
                util.NextNString(0, 2000, 4)+",300000.00)";
            cmd.ExecuteNonQuery();
            FillStock(w);
        }
        public void FillStock(int wid)
        {
            Console.WriteLine("filling stock " + DateTime.Now);
#if TRY
            for (int siid=1;siid<=10;siid++)
#else
            for (int siid = 1; siid <= 100000; siid++)
#endif
            { 
                util u = new util(26, 50);
                var cmd = conn.CreateCommand();
                cmd.CommandText="insert into STOCK(" +
                 "S_I_ID,S_W_ID,S_QUANTITY,S_DIST_01,S_DIST_02," +
                 "S_DIST_03,S_DIST_04,S_DIST_05,S_DIST_06," +
                 "S_DIST_07,S_DIST_08,S_DIST_09,S_DIST_10," +
                 "S_YTD,S_ORDER_CNT,S_REMOTE_CNT,S_DATA) values (" +
                 siid + "," + wid + "," + util.random(10, 100) + "," + //0,1,2
                 GetString(util.randchar(24)) + "," +    // 3
                 GetString(util.randchar(24)) + "," + // 4
                 GetString(util.randchar(24)) + "," + // 5
                 GetString(util.randchar(24)) + "," + // 6
                 GetString(util.randchar(24)) + "," + // 7
                 GetString(util.randchar(24)) + "," + // 8
                 GetString(util.randchar(24)) + "," + // 9
                 GetString(util.randchar(24)) + "," + // 10
                 GetString(util.randchar(24)) + "," + // 11
                 GetString(util.randchar(24)) + "," + // 12
                 "0.0,0,0," + GetString(util.fixStockData(u.NextAString())) + ")";
                cmd.ExecuteNonQuery();
            }
            Console.WriteLine("Done filling stock " + DateTime.Now);
        }
        public void FillDistricts(int wid)
        {
            for (int did = 1; did <= 10; did++)
                FillDistrict(wid, did);
        }
        public void FillDistrict(int wid, int did)
        {
            Console.WriteLine("Filling District " + did+" " + DateTime.Now);
            util us = new util(10, 20);
            util un = new util(6, 10);
            var cmd = conn.CreateCommand();
            cmd.CommandText="insert into DISTRICT(D_ID,D_W_ID,D_NAME,D_STREET_1,D_STREET_2,"+
            "D_CITY,D_STATE,D_ZIP,D_TAX,D_YTD,D_NEXT_O_ID) values("+
                did+","+wid+","+GetString(un.NextAString())+","+
                GetString(un.NextAString())+","+
                GetString(un.NextAString())+","+
                GetString(un.NextAString())+",'"+
                (char)util.random(65, 90) + (char)util.random(65, 90)+"',"+
                GetString(util.NZip())+","+util.NextNString(0, 2000, 4)+","+
                "3000.00,3001)";
            cmd.ExecuteNonQuery();
            FillCustomer(wid, did);
            FillOrder(wid, did);
            Console.WriteLine("Done Filling District " + DateTime.Now);
        }
        public void FillCustomer(int wid, int did)
        {
            Console.WriteLine("starting customer w=" + wid + " d=" + did+" " + DateTime.Now);
            util uf = new util(8, 16);
            util us = new util(10, 20);
            util ud = new util(300, 500);
            util uh = new util(12, 24);
#if TRY
            for (int cid=1;cid<=3;cid++)
#else
            for (int cid = 1; cid <= 3000; cid++)
#endif
            {
                var cmd = conn.CreateCommand();
                cmd.CommandText="insert into CUSTOMER(" +
                    "C_ID,C_D_ID,C_W_ID,C_FIRST,C_MIDDLE,C_LAST," +
                "C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP,C_PHONE," +
                "C_SINCE,C_CREDIT,C_CREDIT_LIM,C_DISCOUNT,C_BALANCE," +
                "C_YTD_PAYMENT,C_PAYMENT_CNT,C_DELIVERY_CNT,C_DATA) values (" +
                cid + "," + did + "," + wid + "," +
                GetString(util.NextLast(cid)) + ",'OE'," +
                GetString(uf.NextAString()) + "," +
                GetString(uf.NextAString()) + "," +
                GetString(uf.NextAString()) + "," +
                GetString(uf.NextAString()) + ",'" +
                (char)util.random(65, 90) + (char)util.random(65, 90) + "'," +
                GetString(util.NZip()) + ",'" + util.NString(16) + "','"+
                DateTime.Now.ToString("o") +"','"+ credit() + "',5000.00," +
                util.randomA(0, 5000, 10000.0) + ",-10.00,10.00,1,0," +
                GetString(ud.NextAString()) + ")";
                cmd.ExecuteNonQuery();
                cmd.CommandText= "insert into HISTORY(" +
                    "H_C_ID,H_C_D_ID,H_C_W_ID,H_D_ID,H_W_ID,H_DATE,H_AMOUNT,H_DATA) values(" +
                    cid + "," + did + "," + wid + "," + did + "," + wid + ",'" + DateTime.Now.ToString("o") + "',10.00," +
                    GetString(uh.NextAString()) + ")";
                cmd.ExecuteNonQuery();
            }
            Console.WriteLine("Customers done " + DateTime.Now);
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
#if TRY
            int[] perm = util.Permute(3);//3000);
			for (int oid=1;oid<=3;oid++)//3000;oid++)
#else
            int[] perm = util.Permute(3000);
            for (int oid = 1; oid <= 3000; oid++)
#endif
            {
                int cnt = util.random(5, 15);
                if (oid < 2101)
                {
                    var cmd = conn.CreateCommand();
                    cmd.CommandText="insert into [ORDER](" +
                        "O_ID,O_C_ID,O_D_ID,O_W_ID,O_ENTRY_D,O_CARRIER_ID,O_OL_CNT,O_ALL_LOCAL) values(" +
                        oid + "," + (perm[oid - 1] + 1) + "," + did + "," +
                        wid + ",'"+DateTime.Now.ToString("o") + "'," + util.random(1, 10) + "," +
                        util.random(1, 15) + ",1)";
                    cmd.ExecuteNonQuery();
                }
                else
                {
                    var cmd = conn.CreateCommand();
                    cmd.CommandText="insert into [ORDER](" +
                        "O_ID,O_C_ID,O_D_ID,O_W_ID,O_ENTRY_D,O_OL_CNT,O_ALL_LOCAL) values(" +
                        oid + "," + (perm[oid - 1] + 1) + "," + did + "," +
                        wid + ",'"+DateTime.Now.ToString("o") + "'," + cnt + ",1)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "insert into NEW_ORDER(NO_O_ID,NO_D_ID,NO_W_ID) values(" +
                        oid + "," + did + "," + wid + ")";
                    cmd.ExecuteNonQuery();
                }
                FillOrderLine(wid, did, oid, cnt);
            }
        }
        void FillOrderLine(int wid, int did, int oid, int cnt)
        {
            for (int j = 1; j <= cnt; j++)
                if (oid < 2101)
                {
                    var cmd = conn.CreateCommand();
                    cmd.CommandText = "insert into ORDER_LINE(" +
                        "OL_O_ID,OL_D_ID,OL_W_ID,OL_NUMBER," +
                        "OL_I_ID,OL_SUPPLY_W_ID,OL_DELIVERY_D,OL_QUANTITY," +
                        "OL_AMOUNT,OL_DIST_INFO) values(" +
                        oid + "," + did + "," + wid + "," + j + "," +
#if TRY
                        util.random(1, 10)+","+
#else
                        util.random(1, 100000) + "," +
#endif
                        wid + ",'" + DateTime.Now.ToString("o") + "',5,0.00," +
                        GetString(util.randchar(24)) + ")";
                    cmd.ExecuteNonQuery();
                }
                else
                {
                    var cmd = conn.CreateCommand();
                    cmd.CommandText = "insert into ORDER_LINE(" +
                        "OL_O_ID,OL_D_ID,OL_W_ID,OL_NUMBER," +
                        "OL_I_ID,OL_SUPPLY_W_ID,OL_DELIVERY_D,OL_QUANTITY," +
                        "OL_AMOUNT,OL_DIST_INFO) values(" +
                        oid + "," + did + "," + wid + "," + j + "," +
#if TRY
                        util.random(1, 10)+","+
#else
                        util.random(1, 100000) + "," +
#endif
                        wid + ",'" + DateTime.Now.ToString("o") + "',5," +
                        util.randomA(1, 999999, 100.0) + "," + GetString(util.randchar(24)) + ")";
                    cmd.ExecuteNonQuery();
                }
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
            rnd = new Random(0);
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
        public static string NString(int ln)
        {
            var r = new char[ln];
            for (int j = 0; j < ln; j++)
                r[j] = (char)('0'+rnd.Next(0, 9));
            return r.ToString();
        }
        public static decimal NextNString(int min, int max, int scale)
        {
            decimal n = rnd.Next(min, max);
            for (var i = 0; i < scale; i++)
                n = n / 10;
            return n;
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
        public static Random rnd;
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
        public static string randomA(int x,int y,double z)
        {
            var r = (rnd.Next(x, y) / z).ToString();
            if (r.Contains("."))
                return r;
            return r + ".00";
        }
        public static int random(int x, int y, int z)
        {
            // between x and y but not =z : presume x<=z<=y
            int r = random(x, y - 1);
            if (r >= z)
                r++;
            return r;
        }
        public static decimal GetDecimal(object ob)
        {
            if (ob is int)
                return ((int)ob) * 1.0M;
            if (ob is long)
                return ((long)ob) * 1.0M;
            if (ob is double)
                return (decimal)(double)ob;
            if (ob is decimal)
                return (decimal)ob;
            if (ob is string)
                return decimal.Parse((string)ob);
            throw new Exception("unknown type " + ob.GetType().Name);
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

