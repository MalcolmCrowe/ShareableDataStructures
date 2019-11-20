using System;
using System.Drawing;
using System.Data;
using System.Collections;
using System.ComponentModel;
using System.Windows.Forms;

namespace tpcc
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
		public IDbConnection db;
		public Label status;
		public int wid;
		public int did;
		public int thresh;

		bool DoThresh(ref string mess)
		{
			int nextoid = 0;
			using (IDbTransaction tr = db.BeginTransaction())
			{
				using (IDbCommand cmd = db.CreateCommand())
				{
					cmd.CommandText = "select d_next_o_id from district where d_w_id="+wid+" and d_id="+did;
					cmd.Transaction = tr;
					nextoid = (int)(long)cmd.ExecuteScalar();
				}
				using (IDbCommand cmd = db.CreateCommand())
				{
					cmd.CommandText = "select count(s_i_id) from stock where s_w_id="+wid+" and s_i_id in (select distinct ol_i_id from order_line where ol_w_id="+wid+" and ol_d_id="+did+" and ol_o_id>="+(nextoid-20)+") and s_quantity<"+thresh;
					cmd.Transaction = tr;
					int n = (int)(long)cmd.ExecuteScalar();
					Set(4,n);
				}
			}
			return false;
		}

		public void Single()
		{
			string mess = "";
			thresh = util.random(10,20);
			DoThresh(ref mess);
			status.Text = mess;
		}

		public StockLevel()
		{
			//
			// Required for Windows Form Designer support
			//
			InitializeComponent();
			Put(36,1,"Stock-Level");
			Put(1,2,"Warehouse:        District:");
			AddField(12,2,4);
			AddField(29,2,2);
			Put(1,4,"Stock Level Threshold:");
			AddField(24,4,2,true);
			Put(1,6,"low stock:");
			AddField(12,6,3);
			AddField(18,6,1,true);
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
