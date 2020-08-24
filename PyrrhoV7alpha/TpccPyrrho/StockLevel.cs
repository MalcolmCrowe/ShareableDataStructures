using System;
using System.Drawing;
using Pyrrho;
using System.Collections;
using System.ComponentModel;
using System.Windows.Forms;

namespace Tpcc
{
	/// <summary>
	/// Summary description for StockLevel.
	/// </summary>
	public class StockLevel : VTerm
	{
		/// <summary>
		/// Required designer variable.
		/// </summary>
		private System.ComponentModel.Container components = null;
        public Form1 form;
		public Label status;
		public int wid;
		public int did;
		public int thresh;
        bool DoThresh(ref string mess)
        {
            int nextoid = 0;
            form.BeginTransaction();
            nextoid = (int)(long)form.conn.ExecuteScalar("FetchDistrict1",""+wid,""+did);
			int n = 0;
            try
            {
                n = (int)(long)(form.conn.ExecuteScalar("StockEnquiry",""+wid,""+did,""+(nextoid-20),""+wid,""+thresh) ?? 0L);
            }
			catch (TransactionConflict ex)
			{
				PyrrhoConnect.reqs.WriteLine("Stocklevel exception " + ex.Message
					+ " " + ex.info["WITH"]);
				form.Rollback();
			}
			catch (Exception ex) {
				PyrrhoConnect.reqs.WriteLine("StockLevel exception " + ex.Message);
			}
			Set(4, n);
            form.Commit("Stocklevel");
            return false;
        }

		public void Single()
		{
			string mess = "";
			thresh = util.random(10,20);
			DoThresh(ref mess);
			status.Text = mess;
		}

		public StockLevel(Form1 f, int w)
		{
			form = f;
			wid = w;
			//
			// Required for Windows Form Designer support
			//
			InitializeComponent();
			Put(36, 1, "Stock-Level");
			Put(1, 2, "Warehouse:        District:");
			AddField(12, 2, 4);
			AddField(29, 2, 2);
			Put(1, 4, "Stock Level Threshold:");
			AddField(24, 4, 2, true);
			Put(1, 6, "low stock:");
			AddField(12, 6, 3);
			AddField(18, 6, 1, true);
		}
		internal void PrepareStatements()
		{ 
			form.conn.Prepare("FetchDistrict1", "select d_next_o_id from district where d_w_id=? and d_id=?");
			form.conn.Prepare("StockEnquiry", "select count(*) from stock inner join (select distinct ol_i_id from order_line where ol_w_id=? and ol_d_id=? and ol_o_id>=?) on s_i_id=ol_i_id where s_w_id=? and s_quantity<?");
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
			Set(0,wid);
			Set(1,did);
			SetCurField(2);
		}
		protected override void EnterField(int fn)
		{
			Field f = (Field)fields[fn];
			string s = f.ToString();
			try
			{
				switch(fn)
				{
					case 2: thresh = int.Parse(s); 
						DoThresh(ref s);
						break;
				}
			} 
			catch(Exception ex)
			{
				s = ex.Message;
                if (s.Contains("with read"))
                    Form1.rconflicts++;
                else
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
			this.Text = "StockLevel";
		}
		#endregion
	}
}
