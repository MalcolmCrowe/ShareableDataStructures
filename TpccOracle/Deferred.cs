using System;
using System.Collections;
using Oracle.DataAccess.Client;
using System.Threading;

namespace Tpcc
{
	/// <summary>
	/// Summary description for Deferred.
	/// </summary>
	public class Deferred
	{
		public OracleConnection db;
		public int wid,fid,tid;

		public Deferred(int w)
		{
            db = new OracleConnection("Data Source=MyDb;User Id=c##Tpcc;Password=adminadmin");
            db.Open();
            wid = w;
		}

		bool Schedule(int did,int carid,OracleTransaction tr)
		{
			int oid = 0;
			int ocid = 0;
            var cmd = db.CreateCommand();
            cmd.Transaction = tr;
			cmd.CommandText = "select NO_O_ID from NEW_ORDER where NO_W_ID="+wid+" and NO_D_ID="+did;
            Form1.RecordRequest(cmd, fid, tid);
            var s = cmd.ExecuteReader();
            if (!s.Read())
            {
                s.Close();
                return false;
            }
			oid = (int)s[0];
            s.Close();
            cmd.CommandText ="delete NEW_ORDER where NO_W_ID="+wid+" and NO_D_ID="+did+" and NO_O_ID="+oid;
            Form1.RecordRequest(cmd, fid, tid);
            cmd.ExecuteNonQuery();
			cmd.CommandText="select O_C_ID from ORDER where O_W_ID="+wid+" and O_D_ID="+did+" and O_ID="+oid;
            Form1.RecordRequest(cmd, fid, tid);
            s = cmd.ExecuteReader();
            s.Read();
		    ocid = (int)(decimal)s[0];
            s.Close();
            cmd.CommandText="update \"ORDER\" where O_W_ID="+wid+" and O_D_ID="+did+" and O_ID="+oid + " set O_CARRIER_ID = "+carid;
            Form1.RecordRequest(cmd, fid, tid);
            cmd.ExecuteNonQuery();
			cmd.CommandText = "update ORDER_LINE  where OL_W_ID="+wid+" and OL_D_ID="+did+" and OL_O_ID="+oid+ " set OL_DELIVERY_DATE='" + DateTime.Now.ToString("dd-MMM-yyyy") + "'";
            Form1.RecordRequest(cmd, fid, tid);
            cmd.ExecuteNonQuery();
            decimal amount = 0.0M;
			cmd.CommandText = "select sum(OL_AMOUNT) from ORDER_LINE where OL_W_ID="+wid+" and OL_D_ID="+did+" and OL_O_ID="+oid;
            s = cmd.ExecuteReader();
            s.Read();
		    amount = util.GetDecimal(s[0]);
            s.Close();
			cmd.CommandText = "update CUSTOMER  where C_W_ID=" + wid + " and C_D_ID=" + did + " and C_ID=" + ocid+" set C_BALANCE =C_BALANCE+"+amount+",C_DELIVERY_CNT=C_DELIVERY_CNT+1";
            Form1.RecordRequest(cmd, fid, tid);
            s = cmd.ExecuteReader();
            return true;
		}

        void Carrier(int carid)
        {
            int done = 0, skipped = 0;
            var tr = db.BeginTransaction(System.Data.IsolationLevel.Serializable);
            tid = ++Form1._tid;
            try
            {
                for (int d = 1; d <= 10; d++)
                    if (Schedule(d, carid, tr))
                        done++;
                    else
                        skipped++;
                tr.Commit();
                Form1.commits++;
            } catch(Exception ex)
            {
                Form1.RecordResponse(ex, fid, tid);
            }
        }

		public void Run()
		{
			ArrayList al = new ArrayList();
            for (; ; )
            {
                var cmd = db.CreateCommand();
                cmd.CommandText="select DL_CARRIER_ID from DELIVERY where DL_W_ID=" + wid + " and DL_DONE is null order by DL_ID";
                var s = cmd.ExecuteReader();
                while(s.Read())
                    al.Add((int)(decimal)s[0]);
                s.Close();
                foreach (int k in al)
                    Carrier(k);
                Thread.Sleep(30000); // 30 sec
            }
		}
	}
}
