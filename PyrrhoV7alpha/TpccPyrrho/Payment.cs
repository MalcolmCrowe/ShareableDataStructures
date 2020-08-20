using System;
using System.Collections;
using System.Text;
using System.Windows.Forms;
using Pyrrho;

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
		public int wid;
        Form1 form;
		public int did,cwid=1,cdid,cid;
		public string clast;
		decimal ytd,dytd,c_balance,camount,c_ytd_payment;
        string cdata, c_credit,c_amount;
		int c_payment_cnt;
        Encoding enc = Encoding.ASCII;
		public Label status;
        bool FetchDistrict()
        {
            form.BeginTransaction();
     //       Console.WriteLine("Payment transaction start");
            Set(42, DateTime.Now.ToString());
            var rdr = form.conn.ExecuteReader("FetchWarehouse2",""+wid);
            try {
                if (!rdr.Read())
                {
                    form.Commit("No payment");
    //                Console.WriteLine("Payment transaction empty");
                    return true;
                }
                Set(1, (string)rdr[0]);
                Set(2, (string)rdr[1]);
                Set(3, (string)rdr[3]);
                Set(4, (string)rdr[4]);
                Set(5, (string)rdr[5]);
                ytd = (decimal)rdr[6];
            }
            catch (Exception ex)
            {
                PyrrhoConnect.reqs.WriteLine("Payment exception 1 " + ex.Message);
                form.Rollback();
            }
            finally
            {
                rdr.Close();
            }
            rdr = form.conn.ExecuteReader("FetchDistrict2",""+wid,""+did);
            try {
                if (!rdr.Read())
                {
     //               Console.WriteLine("Payment done empty");
                    form.Commit("Empty payment");
                    return true;
                }
                Set(8, (string)rdr[0]);
                Set(9, (string)rdr[1]);
                Set(10, (string)rdr[3]);
                Set(11, (string)rdr[4]);
                Set(12, (string)rdr[5]);
                dytd = (decimal)rdr[6];
            }
            catch (Exception ex)
            {
                PyrrhoConnect.reqs.WriteLine("Payment exception 2 " + ex.Message);
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
            ArrayList custs = new ArrayList();
            //				cmd.CommandText="select c_first,c_middle,c_last,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_since,c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment from customer where c_wid="+cwid+" and c_d_id="+cdid+" and c_last='"+c_last+"' order by c_first";
            var rdr = form.conn.ExecuteReader("FetchCustomer2",""+cwid,""+cdid,"'"+clast+"'");
            try { 
                while (rdr.Read())
                    custs.Add((long)rdr[0]);
            }
            catch (Exception ex)
            {
                PyrrhoConnect.reqs.WriteLine("Payment exception 3 " + ex.Message);
                form.Rollback();
            }
            finally
            {
                rdr.Close();
            }
            if (custs.Count == 0)
                return true;
            cid = (int)(long)custs[custs.Count / 2];
            Set(14, cid);
            Set(15, wid);
            Set(16, cdid);
            rdr = form.conn.ExecuteReader("FetchCustomer3",""+cwid,""+cdid,"'"+clast+"'");
            try { 
                rdr.Read();
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
                Set(37, ((decimal)rdr[11]).ToString("F2"));
                Set(30, ((decimal)rdr[12]).ToString("F4").Substring(1)); // c_discount
                Set(31, (string)rdr[8]); // c_phone
                c_balance = (decimal)rdr[13];
                c_ytd_payment = (decimal)rdr[14];
                c_payment_cnt = (int)(decimal)rdr[15];
            }
            finally
            {
                rdr.Close();
            }
            return false;
        }
        bool FetchCustFromId(ref string mess)
        {
            Set(14, cid);
            Set(15, cwid);
            Set(16, cdid);
            SetCurField(35);
            var rdr = form.conn.ExecuteReader("FetchCustomer4",""+cwid,""+cdid,""+cid);
            try { 
                if (!rdr.Read())
                    return true;
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
                c_balance = (decimal)rdr[13];
                c_ytd_payment = (decimal)rdr[14];
                c_payment_cnt = (int)(decimal)rdr[15];
            }
            catch (Exception ex)
            {
                PyrrhoConnect.reqs.WriteLine("Payment exception 4 " + ex.Message);
                form.Rollback();
            }
            finally
            {
                rdr.Close();
            }
            return false;
        }
        bool DoPayment(ref string mess)
        {
            var db = form.conn;
            Set(35, c_amount);
            camount = decimal.Parse(c_amount);
            db.ExecuteTrace("UpdateDistrict2",""+(dytd+camount),""+wid,""+did);
            Set(36, (c_balance + camount).ToString("F2"));
            db.ExecuteTrace("UpdateCustomer2",""+(camount + c_balance),""+(camount + c_ytd_payment),""+(c_payment_cnt + 1),""+cwid,""+cdid,""+cid);
            db.ExecuteTrace("UpdateWarehouse2",""+(ytd+camount),""+wid);
            if (c_credit == "BC")
            {
                var rdr = db.ExecuteReader("FetchCustomer5",""+cwid,""+cdid,""+cid);
                try { 
                    if (!rdr.Read())
                        return true;
                   cdata = (string)rdr[0]; 
                }
                finally
                {
                    rdr.Close();
                }
                cdata = "" + cid + "," + cdid + "," + wid + "," + did + "," + wid + "," + c_amount + ";" + cdata;
                if (cdata.Length > 500)
                    cdata = cdata.Substring(0, 500);
                db.ExecuteTrace("UpdateCustomer3",cdata,""+cwid,""+cdid,""+cid);
                Set(38, cdata.Substring(0, 50));
                if (cdata.Length > 50)
                    Set(39, cdata.Substring(50, 50));
                if (cdata.Length > 100)
                    Set(40, cdata.Substring(100, 50));
                if (cdata.Length > 150)
                    Set(41, cdata.Substring(150, 50));
            }
            //           Console.WriteLine("Payment done");
            form.Commit("Payment");
            return false;
        }
		public void Single()
		{
			PutBlanks();
			did = util.random(1,10);
            if (FetchDistrict())
                return;
			cdid = did;
			cid = -1;
			clast="";
/*			if (activewh>1)
			{
				int x = util.random(1,100);
				if (x>85)
				{
					cdid = util.random(1,10);
					cwid = util.random(1,activewh,wid);
				}
			} */
			int y = util.random(1,100);
			if (y<=60) // select by random last name
				clast = enc.GetString(util.NextLast(9999));
			else
				cid = util.NURandCID();
			c_amount = enc.GetString(util.NextNString(1, 500000, 2));
			string mess="";
			try
			{
				if (cid<0)
					FetchCustFromLast(ref mess);
				else
					FetchCustFromId(ref mess);
				DoPayment(ref mess);
				Invalidate(true);
				form?.Commit("Single Payment");
			} 
			catch(Exception ex)
			{
                var s = ex.Message;
                PyrrhoConnect.reqs.WriteLine("Payment exception 5 " + ex.Message);
                form.Rollback();
                if (s.Contains("with read"))
                    Form1.rconflicts++;
                else
                    Form1.wconflicts++;
            }
		}

		public Payment(Form1 f, int w)
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
            form.conn.Prepare("FetchWarehouse2", "select w_name,w_street_1,w_street_2,w_city,w_state,w_zip,w_ytd from warehouse where w_id=?");
            form.conn.Prepare("FetchDistrict2", "select d_name,d_street_1,d_street_2,d_city,d_state,d_zip,d_ytd from district where d_w_id=? and d_id=?");
            form.conn.Prepare("FetchCustomer2", "select c_id from customer where c_w_id = ? and c_d_id =? and c_last = ? order by c_first");
            form.conn.Prepare("FetchCustomer3", "select c_id,c_first,c_middle,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_since,c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,c_payment_cnt from customer where c_w_id=? and c_d_id=? and  c_last=? order by c_first");
            form.conn.Prepare("FetchCustomer4", "select c_first,c_middle,c_last,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_since,c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,c_payment_cnt from customer where c_w_id=? and c_d_id=? and c_id=?");
            form.conn.Prepare("UpdateDistrict2", "update district set d_ytd = ? where d_w_id = ? and d_id = ?");
            form.conn.Prepare("UpdateCustomer2", "update customer set c_balance=?,c_ytd_payment=?,c_payment_cnt=? where c_w_id=? and c_d_id=? and c_id=?");
            form.conn.Prepare("UpdateWarehouse2", "update warehouse set w_ytd = ? where w_id = ?");
            form.conn.Prepare("FetchCustomer5", "select c_data from customer where c_w_id=? and c_d_id=? and c_id=?");
            form.conn.Prepare("UpdateCustomer3", "update customer set c_data = ? where c_w_id = ? and c_d_id = ? and c_id = ?");
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
						if (cid>0 && cdid>0)
							FetchCustFromId(ref s); 
						break;
					case 15: wid=int.Parse(s); 
						if (cid>0 && cdid>0)
							FetchCustFromId(ref s); 
						break;
					case 16: cdid=int.Parse(s); 
						if (cid>0 && cdid>0)
							FetchCustFromId(ref s); 
						break;
					case 19: 
						clast = s;
						FetchCustFromLast(ref s); break;
					case 35:	
						c_amount = s;
						DoPayment(ref s); break;
				}
			}
			catch (Exception ex)
			{
				s = ex.Message;
                if (s.Contains("with read"))
                    Form1.rconflicts++;
                else
                    Form1.wconflicts++;
                PyrrhoConnect.reqs.WriteLine("Payment exception 5 " + ex.Message);
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
