using System;
using System.Drawing;
using System.Data;
using Shareable;
using StrongLink;
using System.Collections;
using System.ComponentModel;
using System.Windows.Forms;

namespace Tpcc
{
	/// <summary>
	/// Summary description for Delivery.
	/// </summary>
	public class Delivery : VTerm
	{
		/// <summary>
		/// Required designer variable.
		/// </summary>
		private System.ComponentModel.Container components = null;
		public StrongConnect db;
		public Label status;
		public int wid;
		public int carid;

		public bool DoCarrier(ref string mess)
		{
			db.ExecuteNonQuery("insert DELIVERY(DL_W_ID,DL_ID,DL_CARRIER_ID) select "+wid+",max(a.DL_ID)+1,"+
					carid+" from DELIVERY a where a.DL_W_ID="+wid);
			Set(2,carid);
			Set(3,"Delivery has been scheduled");
			return false;
		}

		public void Single()
		{
			string mess = "";
			carid = util.random(1,10);
			DoCarrier(ref mess);
			status.Text = mess;
		}

		public Delivery()
		{
			//
			// Required for Windows Form Designer support
			//
			InitializeComponent();
			Put(38,1,"Delivery");
			Put(1,2,"Warehouse: ");
			AddField(12,2,4);
			Put(1,4,"Carrier Number:");
			AddField(17,4,2,true);
			Put(1,6,"Execution Status:");
			AddField(19,6,24);
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
						DoCarrier(ref s);
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
			this.Text = "Delivery";
		}
		#endregion
	}
}
