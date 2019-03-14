using System;
using System.Drawing;
using System.Collections;
using System.ComponentModel;
using System.Windows.Forms;
using System.Text;
using Shareable;
using StrongLink;

namespace Tpcc
{
	/// <summary>
	/// Summary description for Payment.
	/// </summary>
	public class Payment : VTerm
	{
		/// <summary>
		/// Required designer variable.
		/// </summary>
		private System.ComponentModel.Container components = null;
		public int wid,activewh;
		public StrongConnect db;
		public int did,cwid,cdid,cid;
		public string clast;
		decimal ytd,dytd,c_balance,c_amount,c_ytd_payment;
        string cdata, c_credit;
		int count = 0,c_payment_cnt;
		public Label status;
		Encoding enc = new ASCIIEncoding();

        bool FetchDistrict()
        {
            db.BeginTransaction();
            Set(42, DateTime.Now.ToString());
            var s = db.ExecuteQuery("select W_NAME,W_STREET_1,W_STREET_2,W_CITY,W_STATE,W_ZIP,W_YTD from WAREHOUSE where W_ID=" + wid);
            if (s.IsEmpty)
                return true;
            var rdr = s[0];
            Set(1, (string)s[0][0]);
            Set(2, (string)rdr[1]);
            Set(3, (string)rdr[3]);
            Set(4, (string)rdr[4]);
            Set(5, (string)rdr[5]);
            ytd = util.GetDecimal(s[0][6]);
            s = db.ExecuteQuery("select D_NAME,D_STREET_1,D_STREET_2,D_CITY,D_STATE,D_ZIP,D_YTD from DISTRICT where D_W_ID=" + wid + " and D_ID=" + did);
            if (!s.IsEmpty)
                return true;
            rdr = s[0];
            Set(8, (string)rdr[0]);
            Set(9, (string)rdr[1]);
            Set(10, (string)rdr[3]);
            Set(11, (string)rdr[4]);
            Set(12, (string)rdr[5]);
            dytd = util.GetDecimal(s[0][6]);
            Set(0, wid);
            Set(7, did);
            return false;
        }
        bool FetchCustFromLast(ref string mess)
        {
            ArrayList custs = new ArrayList();
            //				cmd.CommandText="select c_first,c_middle,c_last,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_since,c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment from customer where c_wid="+cwid+" and c_d_id="+cdid+" and c_last='"+c_last+"' order by c_first";
            var s = db.ExecuteQuery("select C_ID from CUSTOMER where C_WID=" + cwid + " and C_D_ID=" + cdid + " and C_LAST='" + clast + "' orderby C_FIRST");
            for (var i = 0; i < s.Length; i++)
                custs.Add((long)s[i][0]);
            if (custs.Count == 0)
                return true;
            cid = (int)custs[(custs.Count + 1) / 2];
            Set(14, cid);
            Set(15, cwid);
            Set(16, cdid);
            s = db.ExecuteQuery("select C_ID,C_FIRST,C_MIDDLE,C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP,C_PHONE,C_SINCE,C_CREDIT,C_CREDIT_LIM,C_DISCOUNT,C_BALANCE,C_YTD_PAYMENT,C_PAYMENT_CNT from CUSTOMER where C_WID=" + cwid + " and C_D_ID=" + cdid + " and  C_LAST='" + clast + "' orderby C_FIRST");
            if (s.IsEmpty)
                return true;
            var rdr = s[0];
            Set(17, (string)rdr[1]);
            Set(18, (string)rdr[2]);
            Set(20, (string)rdr[3]); // c_street_1
            Set(21, (string)rdr[4]); // c_street_2
            Set(22, (string)rdr[5]); // c_city
            Set(23, (string)rdr[6]); // c_state
            Set(24, (string)rdr[7]); // c_zip
            Set(26, rdr[9].ToString()); // c_since
            c_credit = (string)rdr[10];
            Set(29, c_credit);
            Set(37, (util.GetDecimal(rdr[11])).ToString("F2"));
            Set(30, (util.GetDecimal(rdr[12])).ToString("F4").Substring(1)); // c_discount
            Set(31, (string)rdr[8]); // c_phone
            c_balance = (decimal)rdr[13];
            c_ytd_payment = (decimal)rdr[14];
            c_payment_cnt = (int)rdr[15];
            return false;
        }
        bool FetchCustFromId(ref string mess)
        {
            var s = db.ExecuteQuery("select C_FIRST,C_MIDDLE,C_LAST,C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP,C_PHONE,C_SINCE,C_CREDIT,C_CREDIT_LIM,C_DISCOUNT,C_BALANCE,C_YTD_PAYMENT,C_PAYMENT_CNT from CUSTOMER where C_W_ID=" + cwid + " and C_D_ID=" + cdid + " and C_ID=" + cid);
            Set(14, cid);
            Set(15, cwid);
            Set(16, cdid);
            SetCurField(35);
            if (s.IsEmpty)
                return true;
            var rdr = s[0];
            Set(17, (string)rdr[0]); // c_first
            Set(18, (string)rdr[1]); // c_middle
            Set(19, (string)rdr[2]); // c_last
            Set(20, (string)rdr[3]); // c_street_1
            Set(21, (string)rdr[4]); // c_street_2
            Set(22, (string)rdr[5]); // c_city
            Set(23, (string)rdr[6]); // c_state
            Set(24, (string)rdr[7]); // c_zip
            Set(26, rdr[9].ToString()); // c_since
            c_credit = (string)rdr[10];
            Set(29, c_credit);
            Set(37, ((decimal)rdr[11]).ToString("F2"));
            Set(30, ((decimal)rdr[12]).ToString("F4").Substring(1)); // c_discount
            Set(31, (string)rdr[8]); // c_phone
            c_balance = util.GetDecimal(rdr[13]);
            c_ytd_payment = util.GetDecimal(rdr[14]);
            c_payment_cnt = (int)rdr[15];
            return false;
        }
        bool DoPayment(ref string mess)
        {
            Set(35, c_amount.ToString());
            db.ExecuteNonQuery("update DISTRICT where D_W_ID=" + wid + " and D_ID=" + did + " set D_YTD=" + (dytd + c_amount));
            Set(36, (c_balance + c_amount).ToString("F2"));
            db.ExecuteNonQuery("update CUSTOMER where C_W_ID = " + cwid + " and C_D_ID = " + cdid + " and C_ID = " + cid + " set C_BALANCE=" + (c_amount + c_balance) + ",C_YTD_PAYMENT=" + (c_amount + c_ytd_payment) + ",C_PAYMENT_CNT=" + (c_payment_cnt + 1));
            db.ExecuteQuery("update WAREHOUSE where W_ID=" + wid + " set W_YTD=" + (ytd + c_amount));
            if (c_credit == "BC")
            {
                var s = db.ExecuteQuery("select C_DATA from CUSTOMER where C_W_ID=" + cwid + " and C_D_ID=" + cdid + " and C_ID=" + cid);
                if (s.IsEmpty)
                    return true;
                cdata = (string)s[0][0];
                cdata = "" + cid + "," + cdid + "," + cwid + "," + did + "," + wid + "," + c_amount + ";" + cdata;
                if (cdata.Length > 500)
                    cdata = cdata.Substring(0, 500);
                db.ExecuteNonQuery("update CUSTOMER where C_W_ID=" + cwid + " and C_D_ID=" + cdid + " and C_ID=" + cid + " set c_data='" + cdata + "'");

                Set(38, cdata.Substring(0, 50));
                if (cdata.Length > 50)
                    Set(39, cdata.Substring(50, 50));
                if (cdata.Length > 100)
                    Set(40, cdata.Substring(100, 50));
                if (cdata.Length > 150)
                    Set(41, cdata.Substring(150, 50));
            }
            return false;
        }
		public void Single()
		{
			PutBlanks();
			did = util.random(1,10);
			if (FetchDistrict())
				goto bad;
			cwid = wid;
			cdid = did;
			cid = -1;
			clast="";
			if (activewh>1)
			{
				int x = util.random(1,100);
				if (x>85)
				{
					cdid = util.random(1,10);
					cwid = util.random(1,activewh,wid);
				}
			}
			int y = util.random(1,100);
			if (y<=60) // select by random last name
				clast = enc.GetString(util.NextLast(9999));
			else
				cid = util.NURandCID();
			c_amount = decimal.Parse(util.NextNString(1,500000,2).ToString());
			bool done = false;
			string mess="";
			while (!done && count++<1000)
			{
				try
				{
					if (cid<0)
						FetchCustFromLast(ref mess);
					else
						FetchCustFromId(ref mess);
					DoPayment(ref mess);
					Invalidate(true);
					db.Commit();
					done = true;
				} 
				catch(Exception)
				{
				}
			}
			return;
			bad: ;
		}

		public Payment()
		{
			//
			// Required for Windows Form Designer support
			//
			InitializeComponent();
			VTerm vt1 = this;
			Width = vt1.Width;
			Height = vt1.Height+50;
			vt1.Put(38,1,"Payment");
			vt1.Put(1,2,"Date: DD-MM-YYYY hh:mm:ss");
			vt1.Put(1,4,"Warehouse: 9999");
			vt1.AddField(12,4,4);
	//		vt1.Put(1,5,new string('X',20));
			vt1.AddField(1,5,20);
	//		vt1.Put(1,6,new string('X',20));
			vt1.AddField(1,6,20);
	//		vt1.Put(1,7,new string('X',20));
			vt1.AddField(1,7,20);
			vt1.Put(22,7,"XX XXXXX-XXXX");
			vt1.AddField(22,7,2);
			vt1.AddField(25,7,5);
			vt1.AddField(31,7,4);
			vt1.Put(42,4,"District: 99");
			vt1.AddField(52,4,2,true); //7
	//		vt1.Put(42,5,new string('X',20));
			vt1.AddField(42,5,20);
	//		vt1.Put(42,6,new string('X',20));
			vt1.AddField(42,6,20);
	//		vt1.Put(42,7,new string('X',20));
			vt1.AddField(42,7,20);
			vt1.Put(64,7,"XX XXXXX-XXXX");
			vt1.AddField(64,7,2);
			vt1.AddField(67,7,5);
			vt1.AddField(73,7,4);
			vt1.Put(1,9,"Customer: 9999  Cust-Warehouse: 9999  Cust-District: 99");
			vt1.AddField(11,9,4,true); // 14
			vt1.AddField(33,9,4,true); // 15
			vt1.AddField(54,9,2,true); // 16
			vt1.Put(1,10,"Name:");
	//		vt1.Put(9,10,new string('X',16));
			vt1.AddField(9,10,16);
	//		vt1.Put(26,10,new string('X',2));
			vt1.AddField(26,10,2);
	//		vt1.Put(29,10,new string('X',16));
			vt1.AddField(29,10,16,true); // 19
	//		vt1.Put(9,11,new string('X',20));
			vt1.AddField(9,11,20);
	//		vt1.Put(9,12,new string('X',20));
			vt1.AddField(9,12,20);
	//		vt1.Put(9,13,new string('X',20));
			vt1.AddField(9,13,20);
			vt1.Put(30,13,"XX XXXXX-XXXX");
			vt1.AddField(30,13,2);
			vt1.AddField(33,13,5);
			vt1.AddField(39,13,4);
			vt1.Put(50,10,"Since:  DD-MM-YYYY");
			vt1.AddField(58,10,2);
			vt1.AddField(61,10,2);
			vt1.AddField(64,10,4);
			vt1.Put(50,11,"Credit: XX");
			vt1.AddField(58,11,2);
			vt1.Put(50,12,"%Disc:  99.99");
			vt1.AddField(58,12,5);
			vt1.Put(50,13,"Phone:  XXXXXX-XXX-XXX-XXXX");
			vt1.AddField(58,13,6);
			vt1.AddField(65,13,3);
			vt1.AddField(69,13,3);
			vt1.AddField(73,13,4);
			vt1.Put(1,15,"Amount Paid:");
			vt1.Put(23,15,"$9999.99");
			vt1.AddField(24,15,7,true); // 35
			vt1.Put(36,15,"New-Cust-Balance: $-9999999999.99");
			vt1.AddField(55,15,14);
			vt1.Put(1,16,"Credit Limit:   $9999999999.99");
			vt1.AddField(18,16,13);
			vt1.Put(1,18,"Cust-Data:");
	//		vt1.Put(12,18,new string('X',50));
			vt1.AddField(12,18,50);
	//		vt1.Put(12,19,new string('X',50));
			vt1.AddField(12,19,50);
	//		vt1.Put(12,20,new string('X',50));
			vt1.AddField(12,20,50);
	//		vt1.Put(12,21,new string('X',50));
			vt1.AddField(12,21,50);
			vt1.AddField(7,2,20);
			vt1.AddField(75,21,1,true);
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
		protected override void EnterField(int fn)
		{
			Field f = (Field)fields[fn];
			string s = f.ToString();
			try
			{
				switch(fn)
				{
					case 7:
						did = int.Parse(s);
						FetchDistrict(); break;
					case 14: 
						cid = int.Parse(s);
						if (cid>0 && cwid>0 && cdid>0)
							FetchCustFromId(ref s); 
						break;
					case 15: cwid=int.Parse(s); 
						if (cid>0 && cwid>0 && cdid>0)
							FetchCustFromId(ref s); 
						break;
					case 16: cdid=int.Parse(s); 
						if (cid>0 && cwid>0 && cdid>0)
							FetchCustFromId(ref s); 
						break;
					case 19: 
						clast = s;
						FetchCustFromLast(ref s); break;
					case 35:	
						c_amount = decimal.Parse(s);
						DoPayment(ref s); break;
				}
			}
			catch (Exception ex)
			{
				s = ex.Message;
			}
			status.Text = s;
			SetCurField(curField);
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
			this.Text = "Payment";
		}
		#endregion
	}
}
