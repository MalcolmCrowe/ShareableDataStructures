using System;
using System.Collections;
using System.Data;
using Pyrrho;
using System.Threading;

namespace tpcc
{
	/// <summary>
	/// Summary description for Deferred.
	/// </summary>
	public class Deferred
	{
		public IDbConnection db;
		public int wid;

		public Deferred(IDbConnection d,int w)
		{
			db = new PyrrhoConnect(d.ConnectionString);
			db.Open();
			wid = w;
		}

		bool Schedule(int did,int carid,IDbTransaction tr)
		{
			int oid = 0;
			int ocid = 0;
			using (IDbCommand cmd = db.CreateCommand())
			{
				cmd.CommandText = "select no_o_id from new_order where no_w_id="+wid+" and no_d_id="+did;
				cmd.Transaction = tr;
				using (IDataReader rdr=cmd.ExecuteReader())
				{
					if (!rdr.Read())
						return false;
					oid = (int)(long)rdr[0];
				}
			}
			using (IDbCommand cmd = db.CreateCommand())
			{
				cmd.CommandText = "delete from new_order where no_w_id="+wid+" and no_d_id="+did+" and no_o_id="+oid;
				cmd.Transaction = tr;
				cmd.ExecuteNonQuery();
			}
			using (IDbCommand cmd = db.CreateCommand())
			{
				cmd.CommandText = "select o_c_id from \"ORDER\" where o_w_id="+wid+" and o_d_id="+did+" and o_id="+oid;
				cmd.Transaction = tr;
				ocid = (int)(long)cmd.ExecuteScalar();
			}
			using (IDbCommand cmd = db.CreateCommand())
			{
				cmd.CommandText = "update \"ORDER\" set o_carrier_id="+carid+" where o_w_id="+wid+" and o_d_id="+did+" and o_id="+oid;
				cmd.Transaction = tr;
				cmd.ExecuteNonQuery();
			}
			using (IDbCommand cmd = db.CreateCommand())
			{
				cmd.CommandText = "update order_line set ol_delivery_d=date'"+DateTime.Now.ToString("yyyy-MM-dd")+"' where ol_w_id="+wid+" and ol_d_id="+did+" and ol_o_id="+oid;
				cmd.Transaction = tr;
				cmd.ExecuteNonQuery();
			}
			decimal amount = 0.0M;
			using (IDbCommand cmd = db.CreateCommand())
			{
				cmd.CommandText = "select sum(ol_amount) from order_line where ol_w_id="+wid+" and ol_d_id="+did+" and ol_o_id="+oid;
				cmd.Transaction = tr;
				amount = (decimal)cmd.ExecuteScalar();
			}
			using (IDbCommand cmd = db.CreateCommand())
			{
				cmd.CommandText = "update customer set c_balance=c_balance+"+amount+",c_delivery_cnt=c_delivery_cnt+1 where c_w_id="+wid+" and c_d_id="+did+" and c_id="+ocid;
				cmd.Transaction = tr;
				cmd.ExecuteNonQuery();
			}
			return true;
		}

		void Carrier(int carid)
		{
			int done=0,skipped=0;
			using (IDbTransaction tr = db.BeginTransaction())
			{
				for (int d=1;d<=10;d++)
					if (Schedule(d,carid,tr))
						done++;
					else
						skipped++;
			}
            Form1.commits++;
		}

		public void Run()
		{
			ArrayList al = new ArrayList();
			for (;;)
			{
				using(IDbCommand cmd = db.CreateCommand())
				{
					cmd.CommandText = "select dl_carrier_id from delivery where dl_w_id="+wid+" and dl_done is null order by dl_id";
					using (IDataReader rdr = cmd.ExecuteReader())
					{
						while (rdr.Read())
							al.Add((int)(long)rdr[0]);
					}
				}
				foreach(int k in al)
					Carrier(k);
				Thread.Sleep(30000); // 30 sec
			}
		}
	}
}
