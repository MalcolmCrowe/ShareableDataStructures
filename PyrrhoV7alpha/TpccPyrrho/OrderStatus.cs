using System;
using System.Text;
using System.Drawing;
using System.Collections;
using System.ComponentModel;
using System.Windows.Forms;
using System.Data;
using Pyrrho;

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
        public Form1 form;
        public int wid;
        public int did;
		decimal c_balance;
		string c_first;
		string c_middle;
		int cid = -1;
		string clast = "";
		public Label status;
		Encoding enc = new ASCIIEncoding();
        bool Check(PyrrhoCommand c)
		{
			if (c.CommandText!="")
			{
				Set(130,c.CommandText);
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
            var cmd = form.conn.CreateCommand();
                cmd.CommandText = "select c_balance,c_first,c_middle,c_last from customer where c_w_id=" + wid + " and c_d_id=" + did + " and c_id=" + cid;
            var rdr = cmd.ExecuteReader();
            try { 
                if (!rdr.Read())
                    return true;
                clast = (string)rdr[3];
                c_balance = (decimal)rdr[0];
                c_first = (string)rdr[1];
                c_middle = (string)rdr[2];
            }
            catch (Exception)
            {
				PyrrhoConnect.reqs.WriteLine("OrderStatus exception 1");
				form.Rollback();
            }
            finally
            {
                rdr.Close();
            }
            return false;
        }
        bool FetchCustFromLast(ref string mess)
        {
            ArrayList cids = new ArrayList();
            var cmd = form.conn.CreateCommand();
            cmd.CommandText = "select c_id from customer where c_w_id=" + wid + " and c_d_id=" + did + " and c_last='" + clast + "' order by c_first";
            var rdr = cmd.ExecuteReader();
            try { 
                while (rdr.Read())
                    cids.Add((long)rdr[0]);
                cid = (int)(long)cids[(cids.Count + 1) / 2];
            }
            catch (Exception ex)
            {
				PyrrhoConnect.reqs.WriteLine("OrderStatus exception 2 " + ex.Message);
				form.Rollback();
            }
            finally
            {
                rdr.Close();
            }
            cmd.CommandText = "select c_balance,c_first,c_middle from customer  where c_w_id=" + wid + " and c_d_id=" + did + " and c_id=" + cid;
            rdr = cmd.ExecuteReader();
            try { 
                if (!rdr.Read())
                    return true;
                c_balance = (decimal)rdr[0];
                c_first = (string)rdr[1];
                c_middle = (string)rdr[2];
            }
            finally
            {
                rdr.Close();
            }
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
            var cmd = form.conn.CreateCommand();
            cmd.CommandText = "select max(o_id) from \"ORDER\" where o_w_id=" + wid + " and o_d_id=" + did + " and o_c_id=" + cid;
            var rdr = cmd.ExecuteReader();
            try { 
                if (!rdr.Read())
                    return true;
                oid = (int)(long)rdr[0];
            }
            catch (Exception ex)
            {
				PyrrhoConnect.reqs.WriteLine("OrderStatus exception 3 " + ex.Message);
				form.Rollback();
            }
            finally
            {
                rdr.Close();
            }
            cmd.CommandText = "select o_entry_d,o_carrier_id from \"ORDER\" where o_w_id=" + wid + " and o_d_id=" + did + " and o_id=" + oid;
            rdr = cmd.ExecuteReader();
            try { 
                if (!rdr.Read())
                    return true;
                Set(7, oid);
                Set(8, "" + (Date)rdr[0]);
                if (!(rdr[1] is DBNull))
                    Set(9, (int)(long)rdr[1]);
            }
            finally
            {
                rdr.Close();
            }
            int k = 10;
            cmd.CommandText = "select ol_i_id,ol_supply_w_id,ol_quantity,ol_amount,ol_delivery_d from order_line where ol_w_id=" + wid + " and ol_d_id=" + did + " and ol_o_id=" + oid;
            rdr = cmd.ExecuteReader();
            try { 
                while (rdr.Read())
                {
                    Set(k++, (int)(long)rdr[1]);
                    Set(k++, (int)(long)rdr[0]);
                    Set(k++, (int)(decimal)rdr[2]);
                    Set(k++, String.Format("${0,8:F2}", (decimal)rdr[3]));
                    if (!(rdr[4] is DBNull))
                        Set(k++, ((Date)rdr[4]).ToString());
                    else
                        k++;
                }
            }
            finally
            {
                rdr.Close();
            }
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
				form.BeginTransaction();
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
			}
        bad:
            form.Rollback();
		}

		public OrderStatus(Form1 f, int w)
        {
            form = f;
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
