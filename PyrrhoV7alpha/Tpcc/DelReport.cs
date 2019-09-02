using System;
using System.Collections;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Windows.Forms;

namespace tpcc
{
	/// <summary>
	/// Summary description for UserControl1.
	/// </summary>
	public class DelReport : VTerm
	{
		/// <summary> 
		/// Required designer variable.
		/// </summary>
		private System.ComponentModel.Container components = null;
		public IDbConnection db;
		public Label status;
		public int wid;
		public int carid;

		public bool FetchCarrier(ref string mess)
		{
			using (IDbCommand cmd = db.CreateCommand())
			{
				cmd.CommandText = "select dl_done,dl_skipped from delivery where dl_w_id="+wid+" and dl_carrier_id="+carid+" order by dl_id desc";
				using (IDataReader rdr=cmd.ExecuteReader())
				{
					if (!rdr.Read())
						return true;
					Set(3,(int)(long)rdr[0]);
					Set(4,(int)(long)rdr[1]);
				}
			}
			return false;
		}

		public DelReport()
		{
			// This call is required by the Windows.Forms Form Designer.
			InitializeComponent();

			Put(34,1,"Delivery Report");
			Put(1,2,"Warehouse: ");
			AddField(12,2,4);
			Put(1,4,"Carrier Number:");
			AddField(17,4,2,true);
			Put(1,6,"No of Deliveries:");
			AddField(26,6,4);
			Put(1,7,"No of Districts Skipped:");
			AddField(26,7,4);
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
					case 1: carid = int.Parse(s); 
						FetchCarrier(ref s);
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


		#region Component Designer generated code
		/// <summary> 
		/// Required method for Designer support - do not modify 
		/// the contents of this method with the code editor.
		/// </summary>
		private void InitializeComponent()
		{
			// 
			// UserControl1
			// 
			this.Name = "UserControl1";
			this.Size = new System.Drawing.Size(424, 312);

		}
		#endregion
	}
}
