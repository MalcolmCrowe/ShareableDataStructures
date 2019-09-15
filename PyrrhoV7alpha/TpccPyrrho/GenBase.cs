using System;
using System.Collections;
using System.Text;
using System.IO;
using System.Data;
using Pyrrho;

namespace Tpcc
{
    public class GenBase
    {
        static public PyrrhoConnect db;
        static Encoding enc = new ASCIIEncoding();
        public GenBase(PyrrhoConnect d)
        {
            db = d;
        }
        public void BuildTpcc()
        {
            CreationScript();
            FillItems();
        }
        public void DeleteDatabase()
        {
            File.Delete(@"..\..\..\PyrrhoSvr\bin\Debug\Tpcc.pfl");
        }
        public void CreationScript()
        {
            DropTables();
            string[] script = new string[] {
                "create table warehouse(w_id int primary key, w_name nchar(10), w_street_1 nchar( 20), w_street_2 nchar(20), w_city nchar(20), w_state nchar(2), 	w_zip nchar(9), w_tax numeric(4,4), w_ytd numeric(12,2));",
"create table district(d_id int, d_w_id int, d_name nchar(10), d_street_1 nchar(20), d_street_2 nchar(20), d_city nchar(20), d_state nchar(2), d_zip nchar(9), d_tax numeric(4,4), d_ytd numeric(12,2), d_next_o_id int, primary key (d_w_id,d_id), foreign key (d_w_id) references warehouse);",
"create table customer( c_id int, c_d_id int, c_w_id int, c_first nchar(16), c_middle nchar(2), c_last nchar(16), c_street_1 nchar(20),"+
            "c_street_2 nchar(20), c_city nchar(20), c_state nchar(2), c_zip nchar(9), c_phone nchar(16), c_since date, c_credit nchar(2), c_credit_lim numeric(12,2), "+
            "c_discount numeric(4,4), c_balance numeric(12,2), c_ytd_payment numeric(12,2), c_payment_cnt numeric(4,0), c_delivery_cnt numeric(4,0), c_data nchar(500),"+
            "primary key(c_w_id,c_d_id,c_id), foreign key (c_w_id,c_d_id) references district);",
"create table history( h_c_id int, h_c_d_id int, h_c_w_id int, h_d_id int, h_w_id int, h_date date, h_amount numeric(6,2), h_data nchar(24),foreign key (h_c_w_id,h_c_d_id,h_c_id) references customer,foreign key (h_w_id,h_d_id) references district);",
"create table \"ORDER\"(o_id int,o_d_id int,o_w_id int,o_c_id int,o_entry_d date, o_carrier_id int, o_ol_cnt int,o_all_local numeric(1,0), primary key(o_w_id,o_d_id,o_id),foreign key (o_w_id,o_d_id,o_c_id) references customer);",
"create table new_order( no_o_id int,no_d_id int,no_w_id int,primary key (no_w_id,no_d_id,no_o_id),foreign key (no_w_id,no_d_id,no_o_id) references \"ORDER\");",
"create table item(i_id int primary key,i_im_id int,i_name nchar(24),i_price numeric(5,2),i_data nchar(50));",
"create table stock(s_i_id int references item,s_w_id int references warehouse,s_quantity numeric(4,0),s_dist_01 nchar(24),s_dist_02 nchar(24),s_dist_03 nchar(24),s_dist_04 nchar(24),s_dist_05 nchar(24),s_dist_06 nchar(24),s_dist_07 nchar(24),s_dist_08 nchar(24),s_dist_09 nchar(24),s_dist_10 nchar(24),s_ytd numeric(8,2),s_order_cnt numeric(4,0), s_remote_cnt numeric(4,0),s_data nchar(50),primary key(s_w_id,s_i_id));",
"create table order_line( ol_o_id int, ol_d_id int, ol_w_id int, ol_number int, ol_i_id int, ol_supply_w_id int, ol_delivery_d date, ol_quantity numeric(2,0), ol_amount numeric(6,0), ol_dist_info nchar(24),primary key(ol_w_id,ol_d_id,ol_o_id,ol_number),foreign key (ol_w_id,ol_d_id,ol_o_id) references \"ORDER\",foreign key (ol_supply_w_id,ol_i_id) references stock);",
"create table delivery(dl_w_id int,dl_id int,dl_carrier_id int,dl_done int,dl_skipped int,primary key(dl_w_id,dl_id));"
            };
            foreach (string s in script)
                Exec(s);
        }
        public void DropTables()
        {
            string[] script = new string[] {
                "drop table delivery;",
                "drop table order_line;",
               "drop table stock;",
                "drop table item;",
               "drop table new_order;",
                "drop table \"ORDER\";",
                "drop table history;",
                "drop table customer;",
                "drop table district;",
                "drop table warehouse;",
             };
            foreach (string s in script)
                try
                {
                    Exec(s);
                }
                catch (Exception)
                { }
        }
        static void Exec(string s)
        {
            db.ActTrace(s);
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
            return enc.GetString(b);
        }
        util uData = new util(26, 50);
        public void FillItems()
        {
            Console.WriteLine("Adding Items: " + DateTime.Now);
            util ut = new util(14, 24);
#if TRY
            for (int j=1;j<=10;j++)
#else
            for (int j = 1; j <= 100000; j++)
#endif
            {
                string s = "insert into item(i_id,i_im_id,i_name,i_price,i_data) values (" +
#if TRY
                    j+","+util.random(1,10)
#else
                    j + "," + util.random(1, 10000)
#endif
                    + ",'" + enc.GetString(ut.NextAString()) +
                    "'," + enc.GetString(util.NextNString(100, 10000, 2)) + ",'" +
                    NextData() + "')";
                Exec(s);
            }
            Console.WriteLine("Items done: Fill stock?" + DateTime.Now);
        }
        public void FillWarehouse(int w)
        {
            util u1 = new util(6, 10);
            util u2 = new util(10, 20);
            Console.WriteLine("Starting warehouse " + w + " " + DateTime.Now);
            string s = "insert into warehouse(w_id,w_name,w_street_1," +
                "w_street_2,w_city,w_state,w_zip,w_tax,w_ytd) values (" +
                w + ",'" + enc.GetString(u1.NextAString()) + "','" +
                enc.GetString(u2.NextAString()) + "','" +
                enc.GetString(u2.NextAString()) + "','" +
                enc.GetString(u2.NextAString()) + "','" +
                (char)util.random(65, 90) + (char)util.random(65, 90) + "','" +
                enc.GetString(util.NZip()) + "'," +
                enc.GetString(util.NextNString(0, 2000, 4)) + ",300000.00)";
            Exec(s);
            FillStock(w);
        }
        void FillStock(int wid)
        {
            Console.WriteLine("filling stock " + DateTime.Now);
            util u = new util(26, 50);
#if TRY
            for (int siid=1;siid<=10;siid++)
#else
            for (int siid = 1; siid <= 100000; siid++)
#endif
            {
                string s = "insert into stock(s_i_id,s_w_id,s_quantity";
                string v = "'";
                for (int j = 1; j <= 9; j++)
                {
                    s += ",s_dist_0" + j;
                    v += enc.GetString(util.randchar(24)) + "','";
                }
                s += ",s_dist_10,s_ytd,s_order_cnt,s_remote_cnt,s_data) values ("
                    + siid + "," + wid + "," + enc.GetString(util.NextNString(10, 100, 0)) +
                    "," + v + enc.GetString(util.randchar(24)) + "',0,0,0,'" +
                    enc.GetString(util.fixStockData(u.NextAString())) + "')";
                Exec(s);
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
            Console.WriteLine("Filling District " + did + " " + DateTime.Now);
            util us = new util(10, 20);
            util un = new util(6, 10);
            string s = "insert into district(d_id,d_w_id,d_name,d_street_1,d_street_2,d_city,d_state,d_zip,d_tax,d_ytd,d_next_o_id)values(" +
                did + "," + wid + ",'" + enc.GetString(un.NextAString()) +
                "','" + enc.GetString(us.NextAString()) +
                "','" + enc.GetString(us.NextAString()) +
                "','" + enc.GetString(us.NextAString()) +
                "','" + (char)util.random(65, 90) + (char)util.random(65, 90) + "','" +
                enc.GetString(util.NZip()) + "'," + enc.GetString(util.NextNString(0, 2000, 4)) +
                ",30000.00,3001)";
            Exec(s);
            FillCustomer(wid, did);
            FillOrder(wid, did);
            Console.WriteLine("Done Filling District " + DateTime.Now);
        }
        public static void FillCustomer(int wid, int did)
        {
            Console.WriteLine("starting customer (" + wid + ") using " + did + " " + DateTime.Now);
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
                string s = "insert into customer(c_id,c_d_id,c_w_id,c_last,c_middle,c_first,c_street_1,c_street_2,c_city,c_state," + //10
                    "c_zip,c_phone,c_since,c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,c_payment_cnt,c_delivery_cnt,c_data) values (" + //21
                    cid + "," + did + "," + wid + ",'" + enc.GetString(util.NextLast(cid)) + "','OE','" +
                    enc.GetString(uf.NextAString()) +
                    "','" + enc.GetString(us.NextAString()) +
                    "','" + enc.GetString(us.NextAString()) +
                    "','" + enc.GetString(us.NextAString()) +
                    "','" + (char)util.random(65, 90) + (char)util.random(65, 90) + "','" + // 10
                    enc.GetString(util.NZip()) + "','" + enc.GetString(util.NString(16)) + "',date'" +
                    DateTime.Now.ToString("yyyy-MM-dd") + "','" + credit() + "',50000.00," +
                    enc.GetString(util.NextNString(0, 5000, 4)) + ",-10.0,10.0,1,0,'" +
                    enc.GetString(ud.NextAString()) + "')";
                Exec(s);
                s = "insert into history(h_c_id,h_c_d_id,h_c_w_id,h_d_id,h_w_id,h_date,h_amount,h_data) values (" +
                    cid + "," + did + "," + wid + "," + did + "," + wid + ",date'" + DateTime.Now.ToString("yyyy-MM-dd") +
                    "',10,'" + enc.GetString(uh.NextAString()) + "')";
                Exec(s);
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
                string s;
                int cnt = util.random(5, 15);
                if (oid < 2101)
                    s = "insert into \"ORDER\"(o_id,o_c_id,o_d_id,o_w_id,o_entry_d,o_carrier_id,o_ol_cnt,o_all_local) values (" +
                        oid + "," + (perm[oid - 1] + 1) + "," + did + "," + wid + ",date'" + DateTime.Now.ToString("yyyy-MM-dd") +
                        "'," + util.random(1, 10) + "," + util.random(5, 15) + ",1)";
                else
                {
                    s = "insert into \"ORDER\"(o_id,o_c_id,o_d_id,o_w_id,o_entry_d,o_ol_cnt,o_all_local) values (" +
                        oid + "," + (perm[oid - 1] + 1) + "," + did + "," + wid + ",date'" + DateTime.Now.ToString("yyyy-MM-dd") +
                        "'," + cnt + ",1)";
                    Exec(s);
                    s = "insert into new_order(no_o_id,no_d_id,no_w_id) values (" +
                        oid + "," + did + "," + wid + ")";
                }
                Exec(s);
                FillOrderLine(wid, did, oid, cnt);
            }
        }
        void FillOrderLine(int wid, int did, int oid, int cnt)
        {
            for (int j = 1; j <= cnt; j++)
            {
                string s;
                if (oid < 2101)
                    s = "insert into order_line(ol_o_id,ol_d_id,ol_w_id," +
                        "ol_number,ol_i_id,ol_supply_w_id," +
                        "ol_delivery_d,ol_quantity,ol_amount," +
                        "ol_dist_info) values(" +
                        oid + "," + did + "," + wid + "," +
#if TRY
                        j + "," + util.random(1, 10) + "," + wid + ",date'" + DateTime.Now.ToString("yyyy-MM-dd") + "',5,0,'" +
#else
                        j + "," + util.random(1, 100000) + "," + wid + ",date'" + DateTime.Now.ToString("yyyy-MM-dd") + "',5,0,'" +
#endif
                        enc.GetString(util.randchar(24)) + "')";
                else
                    s = "insert into order_line(ol_o_id,ol_d_id,ol_w_id," +
                        "ol_number,ol_i_id,ol_supply_w_id," +
                        "ol_quantity,ol_amount,ol_dist_info) values(" +
                        oid + "," + did + "," + wid + "," +
#if TRY
                                                j + ","+util.random(1,10)+","+wid+
#else
                                                j + "," + util.random(1, 100000) + "," + wid +
#endif
                        ",5," + enc.GetString(util.NextNString(1, 999999, 2)) + ",'" + enc.GetString(util.randchar(24)) + "')";
                Exec(s);
            }
        }

    }
}

