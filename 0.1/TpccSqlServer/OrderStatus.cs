using System;
using System.Text;
using System.Drawing;
using System.Collections;
using System.ComponentModel;
using System.Windows.Forms;
using System.Data.SqlClient;

namespace Tpcc
{
	/// <summary>
	/// Summary description for OrderStatus.
	/// </summary>
	public class OrderStatus : VTerm
	{
		/// <summary>
		/// Required designer variable.
		/// </summary>
		private System.ComponentModel.Container components = null;
		public SqlConnection db;
        SqlTransaction tr;
        int wid;
        public int did;
		decimal c_balance;
		string c_first;
		string c_middle;
		int cid = -1;
		string clast = "";
		public Label status;
		Encoding enc = new ASCIIEncoding();
        bool Check(string c)
		{
			if (c!="")
			{
				Set(130,c);
				Invalidate(true);
				return true;
			}
			return false;
		}
		bool FetchDistrict(ref string mess)
		{
			PutBlanks();
			Set(0,wid);
			Set(1,did);
			return false;
		}
        bool FetchCustFromId(ref string mess)
        {
            var cmd = db.CreateCommand();
            cmd.Transaction = tr;
            cmd.CommandText="select C_BALANCE,C_FIRST,C_MIDDLE,C_LAST from CUSTOMER where C_W_ID=" + wid + " and C_D_ID=" + did + " and C_ID=" + cid;
            var s = cmd.ExecuteReader();
            if (!s.Read())
            {
                s.Close();
                return true;
            }
            clast = (string)s[3];
            c_balance = util.GetDecimal(s[0]);
            c_first = (string)s[1];
            c_middle = (string)s[2];
            s.Close();
            return false;
        }
        bool FetchCustFromLast(ref string mess)
        {
            ArrayList cids = new ArrayList();
            var cmd = db.CreateCommand();
            cmd.Transaction = tr;
            cmd.CommandText = "select C_ID from CUSTOMER where C_W_ID=" + wid + " and C_D_ID=" + did + " and C_LAST='" + clast + "' order by C_FIRST";
            var s = cmd.ExecuteReader();
            while (s.Read())
                cids.Add((long)s[0]);
            s.Close();
            if (cids.Count == 0)
                cid = 1;
            else
                cid = (int)cids[(cids.Count + 1) / 2];
            cmd.CommandText = "select C_BALANCE,C_FIRST,C_MIDDLE from CUSTOMER  where C_W_ID=" + wid + " and C_D_ID=" + did + " and C_ID=" + cid;
            s = cmd.ExecuteReader();
            if (!s.Read())
            {
                s.Close();
                return true;
            }
            c_balance = (decimal)s[0];
            c_first = (string)s[1];
            c_middle = (string)s[2];
            s.Close();
            return false;
        }
        bool DoDisplay(ref string mess)
        {
            Set(2, cid);
            Set(3, c_first);
            Set(4, c_middle);
            Set(5, clast);
            Set(6, "$" + c_balance.ToString("F2"));
            int oid = -1;
            var cmd = db.CreateCommand();
            cmd.Transaction = tr;
            cmd.CommandText = "select max(O_ID) from [ORDER] where O_W_ID=" + wid + " and O_D_ID=" + did + " and O_C_ID=" + cid;
            var s = cmd.ExecuteReader();
            if (!s.Read())
            {
                s.Close();
                return true;
            }
            oid = (int)s[0];
            s.Close();
            cmd.CommandText = "select O_ENTRY_D,O_CARRIER_ID from [ORDER] where O_W_ID=" + wid + " and O_D_ID=" + did + " and O_ID=" + oid;
            s = cmd.ExecuteReader();
            if (!s.Read())
            {
                s.Close();
                return true;
            }
            Set(7, oid);
            var str = ((DateTime)s[0]).ToShortDateString();
            Set(8, str);
            if (!(s[1] == DBNull.Value))
                Set(9, (int)s[1]);
            s.Close();
            int k = 10;
            cmd.CommandText = "select OL_I_ID,OL_SUPPLY_W_ID,OL_QUANTITY,OL_AMOUNT,OL_DELIVERY_D from ORDER_LINE where OL_W_ID="
                + wid + " and OL_D_ID=" + did + " and OL_O_ID=" + oid;
            s = cmd.ExecuteReader();
            while(s.Read())
            {
                Set(k++, (int)s[1]);
                Set(k++, (int)s[0]);
                Set(k++, (int)(decimal)s[2]);
                Set(k++, String.Format("${0,8:F2}", util.GetDecimal(s[3])));
                if (s[4] != DBNull.Value)
                    Set(k++, ((DateTime)s[4]).ToShortDateString());
                else
                    k++;
            }
            s.Close();
            return false;
        }
		public void Single()
		{
			PutBlanks();
			did = util.random(1,10);
			int y=util.random(1,100);
			if (y<=60)
				clast = enc.GetString(util.NextLast(util.NURandCLast()));
			else
				cid = util.NURandCID();
			bool done = false;
			int count=0;
			string mess = "";
			while (!done && count++<1000)
			{
                if (tr==null)
				tr = db.BeginTransaction(System.Data.IsolationLevel.Serializable);
				if (cid>0)
				{
					if (FetchCustFromId(ref mess))
						goto bad;
				}
				else
				{
					if (FetchCustFromLast(ref mess))
						goto bad;
				}
				DoDisplay(ref mess);
				Invalidate(true);
				done = true;
                tr.Commit();
                Form1.commits++;
                tr = null;
			}
			bad:;
		}

		public OrderStatus(SqlConnection c, int w)
        {
            db = c;
            wid = w;
            //
            // Required for Windows Form Designer support
            //
            InitializeComponent();
			VTerm vt1 = this;
			Width = vt1.Width;
			Height = vt1.Height+50;
			vt1.Put(36,1,"Order-Status");
			vt1.Put(1,2,"Warehouse: 9999   District: 99");
			vt1.AddField(12,2,4);
			vt1.AddField(29,2,2,true);
			vt1.Put(1,3,"Customer: 9999   Name: ");
			vt1.AddField(11,3,4,true);
			vt1.AddField(24,3,16);
			vt1.AddField(41,3,2);
			vt1.AddField(44,3,16,true);
			vt1.Put(1,4,"Cust-Balance: ");
			vt1.AddField(16,4,9);
			vt1.Put(1,6,"Order-Number: 99999999   Entry-Date: DD-HH-YYY hh:mm:ss   Carrier-Number: 99");
			vt1.AddField(15,6,8);
			vt1.AddField(38,6,18);
			vt1.AddField(75,6,2);
			vt1.Put(1,7,"Supply-W     Item-Id    Qty       Amount      Delivery-Date");
			for (int j=8;j<=22;j++)
			{
				vt1.AddField(3,j,4);
				vt1.AddField(14,j,6);
				vt1.AddField(25,j,2);
				vt1.AddField(33,j,9);
				vt1.AddField(48,j,10);
			}
			vt1.AddField(79,22,1,true);
			vt1.PutBlanks();
		}

		/// <summary>
		/// Clean up any resources being used.
		/// </summary>
		protected override void Dispose( bool disposing )
		{
			if( disposing )
			{
				if(components != null)
				{
					components.Dispose();
				}
			}
			base.Dispose( disposing );
		}
		public override void Activate()
		{
			PutBlanks();
			Field f = (Field)fields[0];
			f.Put(""+wid);
			SetCurField(1);
		}
		protected override void EnterField(int fn)
		{
			Field f = (Field)fields[fn];
			string s = f.ToString();
			try
			{
				switch(fn)
				{
					case 1: did = int.Parse(s); 
						FetchDistrict(ref s);
						break;
					case 2: cid = int.Parse(s);
						FetchCustFromId(ref s);
						DoDisplay(ref s);
						SetCurField(85);
						break;
					case 5: clast = s;
						FetchCustFromLast(ref s);
						DoDisplay(ref s);
						break;
				}
			} 
			catch(Exception ex)
			{
				s = ex.Message;
                Console.WriteLine(s);
                Form1.wconflicts++;
			}
			SetCurField(curField);
			status.Text = s;
			Invalidate(true);
		}

		#region Windows Form Designer generated code
		/// <summary>
		/// Required method for Designer support - do not modify
		/// the contents of this method with the code editor.
		/// </summary>
		private void InitializeComponent()
		{
			this.components = new System.ComponentModel.Container();
			this.Size = new System.Drawing.Size(300,300);
			this.Text = "OrderStatus";
		}
		#endregion
	}
}
