using System;
using System.Collections;
using System.Data;
using Shareable;
using StrongLink;
using System.Threading;

namespace Tpcc
{
	/// <summary>
	/// Summary description for Deferred.
	/// </summary>
	public class Deferred
	{
		public StrongConnect db;
		public int wid;

		public Deferred(int w)
		{
			db = new StrongConnect("127.0.0.1", 50433, "Tpcc");
			wid = w;
		}

		bool Schedule(int did,int carid)
		{
			int oid = 0;
			int ocid = 0;
			var s = db.ExecuteQuery("select NO_O_ID from NEW_ORDER where NO_W_ID="+wid+" and NO_D_ID="+did);
			if (s.items.Count==0)
				return false;
			oid = (int)s[0][0];
			db.ExecuteNonQuery("delete NEW_ORDER where NO_W_ID="+wid+" and NO_D_ID="+did+" and NO_O_ID="+oid);
			s = db.ExecuteQuery("select O_C_ID from ORDER where O_W_ID="+wid+" and O_D_ID="+did+" and O_ID="+oid);
		    ocid = (int)s[0][0];
			db.ExecuteNonQuery("update ORDER where O_W_ID="+wid+" and O_D_ID="+did+" and O_ID="+oid + " set O_CARRIER_ID = "+carid);
			db.ExecuteNonQuery("update ORDER_LINE  where OL_W_ID="+wid+" and OL_D_ID="+did+" and OL_O_ID="+oid+ " set OL_DELIVERY_DATE=date'" + DateTime.Now.ToString("yyyy-MM-dd") + "'");
			decimal amount = 0.0M;
			s = db.ExecuteQuery("select sum(OL_AMOUNT) from ORDER_LINE where OL_W_ID="+wid+" and OL_D_ID="+did+" and OL_O_ID="+oid);
		    amount = util.GetDecimal(s[0][0]);
			db.ExecuteNonQuery("update CUSTOMER  where C_W_ID=" + wid + " and C_D_ID=" + did + " and C_ID=" + ocid+" set C_BALANCE =C_BALANCE+"+amount+",C_DELIVERY_CNT=C_DELIVERY_CNT+1");
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

		public void Run()
		{
			ArrayList al = new ArrayList();
            for (; ; )
            {
                var s = db.ExecuteQuery("select DL_CARRIER_ID from DELIVERY where DL_W_ID=" + wid + " and DL_DONE is null order by DL_ID");
                for (var i = 0; i < s.items.Count; i++)
                    al.Add((int)s.items[i].fields[0].Value);
                foreach (int k in al)
                    Carrier(k);
                Thread.Sleep(30000); // 30 sec
            }
		}
	}
}
