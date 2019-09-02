using System;
using System.Collections;
using System.ComponentModel;
using System.Drawing;
using Npgsql;
using System.Windows.Forms;

namespace Tpcc
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
		public Form1 form;
		public Label status;
		public int wid = 1;
		public int carid;
        public int fid, tid;
        public bool FetchCarrier(ref string mess)
        {
            var cmd = form.conn.CreateCommand();
            cmd.CommandText = "select DL_DONE,DK_SKIPPED from DELIVERY where DL_W_ID=" + wid + " and DL_CARRIER_ID=" + carid + " order by DL_ID desc";
            Form1.RecordRequest(cmd, fid, tid);
            var s = cmd.ExecuteReader();
            try
            {
                var r = s.Read();
                if (r)
                {
                    Set(3, (int)s[0]);
                    Set(4, (int)s[1]);
                }
                return !r;
            }
            catch (Exception)
            {
                form.Rollback();
            } finally
            {
                s.Close();
            }
            return false;
        }

		public DelReport(Form1 fm,int w)
		{
            form = fm;
            wid = w;
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
                Form1.RecordResponse(ex, fid, tid);
                Form1.rconflicts++;
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
