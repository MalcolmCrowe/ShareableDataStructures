using System;
using System.Collections;
using System.Data;
using Pyrrho;
using System.Threading;

namespace Tpcc
{
    /// <summary>
    /// Summary description for Deferred.
    /// </summary>
    public class Deferred
    {
        public PyrrhoConnect db;
        public int wid;

        public Deferred(int w)
        {
            db = new PyrrhoConnect("Files = Tpcc");

            wid = w;


            bool Schedule(int did, int carid)
            {
                int oid = 0;
                int ocid = 0;
                var cmd = db.CreateCommand();
                cmd.CommandText = "select no_o_id from new_order where no_w_id=" + wid + " and no_d_id=" + did;
                var rdr = cmd.ExecuteReader();
                try
                {
                    if (!rdr.Read())
                        return false;
                    oid = (int)(long)rdr[0];
                }
                finally
                {
                    rdr.Close();
                }
                db.ActTrace("delete from new_order where no_w_id=" + wid + " and no_d_id=" + did + " and no_o_id=" + oid);
                cmd.CommandText = "select o_c_id from \"ORDER\" where o_w_id=" + wid + " and o_d_id=" + did + " and o_id=" + oid;
                ocid = (int)(long)cmd.ExecuteScalar();
                db.ActTrace("update \"ORDER\" set o_carrier_id=" + carid + " where o_w_id=" + wid + " and o_d_id=" + did + " and o_id=" + oid);
                db.ActTrace("update order_line set ol_delivery_d=date'" + DateTime.Now.ToString("yyyy-MM-dd") + "' where ol_w_id=" + wid + " and ol_d_id=" + did + " and ol_o_id=" + oid);
                decimal amount = 0.0M;
                cmd.CommandText = "select sum(ol_amount) from order_line where ol_w_id=" + wid + " and ol_d_id=" + did + " and ol_o_id=" + oid;
                amount = (decimal)cmd.ExecuteScalar();
                db.ActTrace("update customer set c_balance=c_balance+" + amount + ",c_delivery_cnt=c_delivery_cnt+1 where c_w_id=" + wid + " and c_d_id=" + did + " and c_id=" + ocid);
                return true;
            }

            void Carrier(int carid)
            {
                int done = 0, skipped = 0;
                db.BeginTransaction();
                for (int d = 1; d <= 10; d++)
                    if (Schedule(d, carid))
                        done++;
                    else
                        skipped++;
            }

            void Run()
            {
                ArrayList al = new ArrayList();
                for (; ; )
                {
                    var cmd = db.CreateCommand();
                    cmd.CommandText = "select dl_carrier_id from delivery where dl_w_id=" + wid + " and dl_done is null order by dl_id";
                    var rdr = cmd.ExecuteReader();
                    try
                    { 
                    while (rdr.Read())
                        al.Add((int)(long)rdr[0]);
                    }
                    finally
                    {
                        rdr.Close();
                    }
                    foreach (int k in al)
                        Carrier(k);
                    Thread.Sleep(30000); // 30 sec
                }
            }
        }
    }
}
