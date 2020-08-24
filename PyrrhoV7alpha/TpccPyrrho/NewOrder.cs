using System;
using System.Windows.Forms;
using Pyrrho;
using System.Net.Sockets;

namespace Tpcc
{
    /// <summary>
    /// Summary description for NewOrder.
    /// </summary>
    public class NewOrder : VTerm
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.Container components = null;
        public int wid;
        public bool oneDistrict;
        public int did;
        public int cid;
        public int ol_cnt;
        public OrderLine[] ols = new OrderLine[15];
        public decimal w_tax;
        public decimal d_tax;
        public decimal c_discount;
        public Label status;
        int o_id;
        string i_name;
        decimal i_price;
        string i_data;
        string sdata;
        string bg;
        int s_quantity;
        decimal total;
        bool allhome = true;
        public int activewh = 1;
        public Button btn;
        public TextBox txtBox;
        public Form1 form;
        public class OrderLine
        {
            public int ol_supply_w_id;
            public bool allhome;
            public int oliid;
            public int s_quantity;
            public decimal ol_price;
            public int ol_quantity;
            public decimal ol_amount;
        }
        public void Multiple()
        {
            int Tcount = 0;
            while (Tcount++ < 2000)
            {
                try
                {
                    Single();
                    txtBox.Text = "" + Tcount;
                }
                catch(SocketException e)
                {
                    throw e;
                }
                catch (TransactionConflict ex)
                {
                    var s = ex.Message;
                    if (s.Contains("with read"))
                        Form1.rconflicts++;
                    else
                        Form1.wconflicts++;
                    PyrrhoConnect.reqs.WriteLine("Commit exception " + ex.Message
                        + " " + ex.info["WITH"]);
                    form.Rollback();
                }
                catch (Exception ex)
                {
                    var s = ex.Message;
                        Form1.wconflicts++;
                    PyrrhoConnect.reqs.WriteLine("Commit exception "+ex.Message);
                }
            }
        }
        bool ExecNQ(string name,params string[] args)
        {
            try
            {
                var t = DateTime.Now.Ticks;
      //          Console.WriteLine("ExecNQ started "+cmd.CommandText);
                form.conn.ExecuteTrace(name,args);
      //          Console.WriteLine("ExecNQ done " + (DateTime.Now.Ticks-t));
                return false;
            }
            catch (TransactionConflict ex)
            {
                var s = ex.Message;
                Form1.wconflicts++;
                PyrrhoConnect.reqs.WriteLine("New Order exception 5 " + ex.Message
                    + " " + ex.info["WITH"]);
                form.Rollback();
            }
            catch (Exception ex)
            {
                var s = ex.Message;
                    Form1.wconflicts++;
                PyrrhoConnect.reqs.WriteLine("New Order exception 5 " + ex.Message);
            }
            return true;
        }
        bool FetchCustomer(ref string mess)
        {
            var rdr = form.conn.ExecuteReader("FetchCustomer",""+wid,""+did,""+cid);
            try { 
                if (!rdr.Read())
                {
                    mess = "No customer " + cid;
                    return true;
                }
                Set(3, (string)rdr[1]);
                Set(4, (string)rdr[2]);
                c_discount = (decimal)rdr[0];
                Set(5, c_discount.ToString("F4").Substring(1));
            }
            catch(Exception)
            {
                form.Rollback();
            }
            finally
            {
                rdr.Close();
            }
            return false;
		}
        bool FetchDistrict(ref string mess)
        {
            form.BeginTransaction();
            var rdr = form.conn.ExecuteReader("FetchDistrict",""+wid,""+did);
            try { 
                if (!rdr.Read())
                {
                    mess = "No District " + did;
                    return true;
                }
                var o = rdr[0];
                d_tax = (decimal)o;
                o_id = (int)(long)rdr[1];
                Set(6, o_id);
                Set(132, DateTime.Now.ToString());
            }
            catch (TransactionConflict ex)
            {
                var s = ex.Message;
                PyrrhoConnect.reqs.WriteLine("New Order exception 1 " + ex.Message
                    + " " + ex.info["WITH"]);
                form.Rollback();
            }
            catch (Exception ex)
            {
                PyrrhoConnect.reqs.WriteLine("New Order Exception 1 " + ex.Message);
                form.Rollback();
            }
            finally
            {
                rdr.Close();
            }
            //		mess=cmd.CommandText;
            rdr = form.conn.ExecuteReader("FetchWarehouse",""+wid);
            try
            {
                if (!rdr.Read())
                {
                    mess = "No warehouse " + wid;
                    return true;
                }
                w_tax = (decimal)rdr[0];
                Set(8, w_tax.ToString("F4").Substring(1));
                Set(9, d_tax.ToString("F4").Substring(1));
            }
            catch (TransactionConflict ex)
            {
                var s = ex.Message;
                PyrrhoConnect.reqs.WriteLine("New Order exception 2 " + ex.Message
                    + " " + ex.info["WITH"]);
                form.Rollback();
            }
            catch (Exception ex)
            {
                PyrrhoConnect.reqs.WriteLine("New Order exception 2 " + ex.Message);
                form.Rollback();
            }
            finally
            {
                rdr.Close();
            }
            //		mess=cmd.CommandText;
            if (ExecNQ("UpdateOrderNo", "" + (o_id + 1), "" + wid, "" + did))
                return true;
            allhome = true;
            return false;
        }
		bool DoOLCount(ref string mess)
		{
            Set(7, ol_cnt);
            if (ExecNQ("CreateOrder",""+o_id,""+did,""+wid,""+cid, "date'" + DateTime.Now.ToString("yyyy-MM-dd")+"'",""+ol_cnt,""+ (allhome ? 1 : 0)))
                return true;
            //		mess=cmd.CommandText;
            if (ExecNQ("CreateNewOrder",""+o_id,""+did,""+wid))
                return true;
            return false;
		}

        bool FetchItemData(int j, ref string mess)
        {
            return FetchItemData1(j, ref mess) || FetchItemData2(j, ref mess);
        }

        bool FetchItemData1(int j, ref string mess)
        {
            OrderLine a = ols[j];
            int k = 10 + j * 8;
            var rdr = form.conn.ExecuteReader("FetchItemData",""+a.oliid);
            try { 
                if (!rdr.Read())
                {
                    mess = "No such item " + a.oliid;
                    return true;
                }
                i_price = (decimal)rdr[0];
                a.ol_price = i_price;
                i_name = (string)rdr[1];
                i_data = (string)rdr[2];
                Set(k, a.ol_supply_w_id);
                Set(k + 1, string.Format("{0,6}", a.oliid));
            }
            catch (TransactionConflict ex)
            {
                PyrrhoConnect.reqs.WriteLine("New Order exception 3 " + ex.Message
                    + " " + ex.info["WITH"]);
                form.Rollback();
            }
            catch (Exception ex)
            {
                PyrrhoConnect.reqs.WriteLine("New Order exception 3 " + ex.Message);
                form.Rollback();
            }
            finally
            {
                rdr.Close();
            }
            return false;
        }
        bool FetchItemData2(int j, ref string mess)
        {
            OrderLine a = ols[j];
            int k = 10 + j * 8;
            //		mess=cmd.CommandText;
            var rdr = form.conn.ExecuteReader("FetchItemData2"+(did-1),""+a.oliid,""+a.ol_supply_w_id);
            try { 
                if (!rdr.Read())
                {
                    mess = "no such stock item " + a.oliid + " in " + a.ol_supply_w_id;
                    return true;
                }
                Set(k + 2, i_name);
                object o = rdr[0];
                s_quantity = 0;
                if (o is long)
                    s_quantity = (int)(long)o;
                else if (o is decimal)
                    s_quantity = (int)(decimal)o;
                Set(k + 4, s_quantity);
                a.s_quantity = s_quantity;
                sdata = (string)rdr[2];
            }
            catch (TransactionConflict ex)
            {
                PyrrhoConnect.reqs.WriteLine("New Order exception 4 " + ex.Message
                    + " " + ex.info["WITH"]);
                form.Rollback();
            }
            catch (Exception ex)
            {
                PyrrhoConnect.reqs.WriteLine("New Order exception 4 " + ex.Message);
                form.Rollback();
            }
            finally
            {
                rdr.Close();
            }
            bg = "G";
            if (sdata.IndexOf("ORIGINAL") >= 0 && i_data.IndexOf("ORIGINAL") >= 0)
                bg = "B";
            Set(k + 5, bg);
            Set(k + 6, string.Format("${0,6:F2}", i_price)); return false;
        }
		bool DoOLQuantity(int j,ref string mess)
		{
			OrderLine a = ols[j];
			int k = 10+j*8;
			s_quantity = a.s_quantity-a.ol_quantity;
			if (s_quantity<10)
				s_quantity += 91;
			Set(k+4,s_quantity);
			Set(k+3,a.ol_quantity);
			a.ol_amount = a.ol_quantity*i_price;
			Set(k+7,string.Format("${0,7:F2}",a.ol_amount));
			return false;
		}
		bool DoTotal()
		{
			total = 0.0M;
			for (int j=0;j<ol_cnt;j++)
			{
				OrderLine a = ols[j];
				total += a.ol_amount;
			}
			total = total*(1-c_discount)*(1+w_tax+d_tax);
			Set(131,string.Format("${0,8:F2}",total));
			return false;
		}
        public bool DoCommit(ref string mess)
        {
            bool done = false;
            try
            {
                for (int j = 0; j < ol_cnt; j++)
                {
                    OrderLine a = ols[j];
                    s_quantity = a.s_quantity - a.ol_quantity;
                    if (s_quantity < 10)
                        s_quantity += 91;
                    if (ExecNQ("UpdateStock",""+s_quantity,""+a.oliid,""+a.ol_supply_w_id))
                        return false;
                    if (ExecNQ("AddOrderLine",""+o_id,""+did,""+wid,""+(j+1),""+a.oliid,""+a.ol_supply_w_id,""+a.ol_quantity,""+a.ol_amount))
                        return false;
                }
                mess = "OKAY";
                int rbk = util.random(1, 100);
                if (rbk == 1)
                {
     //               Console.WriteLine("New Order Rollback");
                    form.Rollback();
                    done = true;
                }
                else
                {
     //               Console.WriteLine("New Order Commit");
                   form.Commit("NewOrder");
                    Form1.commits++;
                }
                // Phase 3 display the results
                Set(130, "OKAY");
                done = true;
            }
            catch (TransactionConflict ex)
            {
                var s = ex.Message;
                Set(130, s);
                Form1.wconflicts++;
                PyrrhoConnect.reqs.WriteLine("Commit exception 1 "+ex.Message 
                    + " " + ex.info["WITH"]);
                form.Rollback();
            }
            catch (Exception ex)
            {
                var s = ex.Message;
                Set(130, s);
                    Form1.wconflicts++;
                PyrrhoConnect.reqs.WriteLine("Commit exception 1 "+ex.Message);
                form.Rollback();
            }
            return done;
        }
		void GetData()
		{
            if (!oneDistrict)
			    did = util.random(1,10); 
            Set(1,did);
			cid = util.NURandCID(); Set(2,cid);
			ol_cnt = util.random(5,15);
			for (int j=0;j<ol_cnt;j++)
			{
				OrderLine a = new OrderLine();
				ols[j] = a;
				int x = util.random(1,100);
				a.ol_supply_w_id = wid;
				if (x==1 && activewh>1) 
				{
					a.ol_supply_w_id = util.random(1,activewh,wid);
					allhome = false;
				}
				a.oliid = util.NURandOLID();
				a.ol_quantity = util.random(1,10);
			}		
		}
		int count = 0;
		string mess;
		public void Single()
		{
			status.Text = "";
			// Phase 1 generate the "terminal input"
			PutBlanks();
			Set(0,wid);
			GetData();
			count = 0;
			mess = "OKAY";
			//		Invalidate(true);
			//		Thread.Sleep(1000);
			// Phase 2 (re)start the transaction
			while(count++<1000)
			{
				if (FetchDistrict(ref mess))
					goto bad;
				if (FetchCustomer(ref mess))
					goto bad;
				if (DoOLCount(ref mess))
					goto bad;
				total=0.0M;	
				for (int j=0;j<ol_cnt;j++)
				{
					if (FetchItemData(j,ref mess))
						goto bad;
					if (DoOLQuantity(j,ref mess))
						goto bad;
				}
				DoTotal();
				if (DoCommit(ref mess))
					break;
                bad:
      //          Console.WriteLine("Rollback in New Order");
                form.Rollback();
                Set(130,mess);
				Invalidate(true);
			}
			Invalidate(true);
			if (btn!=null)
				btn.Enabled = true;
        }
        public void Single(ref int stage)
		{
			status.Text = "";
			// Phase 1 generate the "terminal input"
			if (stage<=0)
			{
				PutBlanks();
				Set(0,wid);
				GetData();
				count = 0;
				mess = "OKAY";
				if (stage==0)
					stage++;
			}
			//		Invalidate(true);
			//		Thread.Sleep(1000);
			// Phase 2 (re)start the transaction
			again: count++;
			if (count>=1000)
				goto bad;
			if (stage<0 || stage==1)
			{
				if (FetchDistrict(ref mess))
					goto bad;
			}
			if (stage==1)
			{
				Invalidate(true);
				stage++;
				return;
			}
			if (stage<0 || stage==2)
			{
				if (FetchCustomer(ref mess))
					goto bad;
			}
			if (stage==2)
			{
				Invalidate(true);
				stage++;
				return;
			}
			if (stage<0 || stage==3)
			{
				if (DoOLCount(ref mess))
					goto bad;
			}
			if (stage==3)
			{
				Invalidate(true);
				stage++;
				return;
			}
			total=0.0M;	
			if (stage<0 || (stage>=4 && stage<4+ol_cnt+ol_cnt))
			{
				int j;
				if (stage<0)
					j = 0;
				else
					j = (stage-4)/2;
			ol_loop:
				if (stage<0 || stage%2==0)
				{
					if (FetchItemData(j,ref mess))
						goto bad;
				}
				if (stage%2==0)
				{
					Invalidate(true);
					stage++;
					return;
				}
				if (stage<0 || stage%2==1)
				{
					if (DoOLQuantity(j,ref mess))
						goto bad;
				}
				if (j<ol_cnt-1)
				{
					if (stage>=0)
					{
						Invalidate(true);
						stage++;
						return;
					}
					j++;
					goto ol_loop;
				}		
				DoTotal();
				if (!DoCommit(ref mess))
				{
					if (stage>=0)
						stage = 0;
					goto again;
				} else if (stage>=0)
					stage = 99;	
			}
			Invalidate(true);
			if (btn!=null)
				btn.Enabled = true;
        //    Console.WriteLine("Committed Order");
            form.Commit("Single Order");
			return;
        bad:
       //     Console.WriteLine("Rollback new order");
            form.Rollback();
            Set(130,mess);
			Invalidate(true);
			if (btn!=null)
				btn.Enabled = true;
		}

		public NewOrder(Form1 f,int w)
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
			vt1.Put(36,1,"New Order");
			vt1.Put(1,2,"Warehouse: 9999   District: 99");
			vt1.Put(55,2,"Date: DD-MM-YYYY hh:mm:ss");
			vt1.Put(1,3,"Customer:  9999   Name: XXXXXXXXXXXXXXXX   Credit: XX   %Disc: 99.99");
			vt1.Put(1,4,"Order Number: 99999999  Number of Lines: 99");
			vt1.Put(52,4,"W_tax: 99.99   D_tax: 99.99");
			vt1.Put(2,6,"Supp_W  Item_Id  Item Name");
			vt1.Put(45,6,"Qty  Stock  B/G  Price    Amount");
			for (int j=7;j<=21;j++)
				vt1.Put(3,j,"9999   999999   XXXXXXXXXXXXXXXXXXXXXXXX  99    999    X   $999.99  $9999.99");
			vt1.Put(1,22,"Execution Status: XXXXXXXXXXXXXXXXXXXXXXXX");
			vt1.Put(62,22,"Total:  $99999.99");
			vt1.AddField(12,2,4);
			vt1.AddField(29,2,2,true);
			vt1.AddField(12,3,4,true);
			vt1.AddField(25,3,16);
			vt1.AddField(52,3,2);
			vt1.AddField(64,3,5);
			vt1.AddField(15,4,8);
			vt1.AddField(42,4,2,true);
			vt1.AddField(59,4,5);
			vt1.AddField(74,4,5);
			for (int k=7;k<=21;k++)
			{
				vt1.AddField(3,k,4,true);
				vt1.AddField(10,k,6,true);
				vt1.AddField(19,k,24);
				vt1.AddField(45,k,2,true);
				vt1.AddField(51,k,3);
				vt1.AddField(58,k,1);
				vt1.AddField(62,k,7);
				vt1.AddField(71,k,8);
			}
			vt1.AddField(19,22,24);
			vt1.AddField(70,22,9);
			vt1.AddField(61,2,19);
			vt1.PutBlanks();
        }

        internal void PrepareStatements()
        {
            form.conn.Prepare("FetchCustomer", "select c_discount,c_last,c_credit from customer where c_w_id=? and c_d_id=? and c_id=?");
            form.conn.Prepare("FetchDistrict", "select d_tax,d_next_o_id from district where d_w_id=? and d_id=?");
            form.conn.Prepare("FetchWarehouse", "select w_tax from warehouse where w_id=?");
            form.conn.Prepare("UpdateOrderNo", "update district set d_next_o_id=? where d_w_id=? and d_id=?");
            form.conn.Prepare("CreateOrder", "insert into \"ORDER\"(o_id,o_d_id,o_w_id,o_c_id,o_entry_d,o_ol_cnt,o_all_local) values(?,?,?,?,?,?,?)");
            form.conn.Prepare("CreateNewOrder", "insert into new_order(no_o_id,no_d_id,no_w_id)values(?,?,?)");
            form.conn.Prepare("FetchItemData", "select i_price,i_name,i_data from item where i_id=?");
            for (var i = 1; i <= 10; i++)
                form.conn.Prepare("FetchItemData2" + (i - 1), "select s_quantity,s_dist_" + i.ToString("D2") + ",s_data from stock where s_i_id=? and s_w_id=?");
            form.conn.Prepare("UpdateStock", "update stock set s_quantity=? where s_i_id=? and s_w_id=?");
            form.conn.Prepare("AddOrderLine", "insert into order_line(ol_o_id,ol_d_id,ol_w_id,ol_number,ol_i_id,ol_supply_w_id,ol_quantity,ol_amount)values(?,?,?,?,?,?,?,?)");

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

		#region Windows Form Designer generated code
		/// <summary>
		/// Required method for Designer support - do not modify
		/// the contents of this method with the code editor.
		/// </summary>
		private void InitializeComponent()
		{
			// 
			// NewOrder
			// 
			this.Name = "NewOrder";
			this.Size = new System.Drawing.Size(568, 374);

		}
		#endregion

		public override void Activate()
		{
			Field f = (Field)fields[0];
			f.Put(""+wid);
			SetCurField(1);
		}
		protected override void EnterField(int fn)
		{
			Field f = (Field)fields[fn];
			string s = f.ToString();
			int ol;
			try
			{
				if (fn<10)
					switch(fn)
					{
						case 1: did = int.Parse(s);
							FetchDistrict(ref s);
							break;
						case 2: cid = int.Parse(s);
							FetchCustomer(ref s);
							break;
						case 7: ol_cnt = int.Parse(s);
							DoOLCount(ref s);
							for (int j=0;j<ol_cnt;j++)
								ols[j] = new OrderLine();
							break;
					}
				else
				{
					ol = (fn-10)/8;
					fn = (fn-10)%8;
					OrderLine o = ols[ol];
					switch(fn)
					{
						case 0:	o.ol_supply_w_id = int.Parse(s);
							break;
						case 1: o.oliid = int.Parse(s);
							FetchItemData(ol,ref s);
							break;
						case 3: o.ol_quantity = int.Parse(s);
							DoOLQuantity(ol,ref s);
							if (ol==ol_cnt-1)
								DoTotal();
							break;
					}
				}
				status.Text = s;
			}
			catch(Exception ex) {
				status.Text = ex.Message;
                form.Rollback();
                Form1.wconflicts++;
            }
			Invalidate(true);
		}

        public int step = 0,line = 0;
        internal void Step()
        {
            switch (step)
            {
                case 0:
                    PutBlanks();
                    Set(0, wid);
                    GetData();
                    count = 0;
                    mess = "";
                    break;
                case 1:
                    FetchDistrict(ref mess);
                    break;
                case 2:
                    FetchCustomer(ref mess);
                    break;
                case 3:
                    DoOLCount(ref mess);
                    line = 0;
                    break;
                case 4:
                    FetchItemData1(line, ref mess);
                    break;
                case 5:
                    FetchItemData2(line, ref mess);
                    break;
                case 6:
                    DoOLQuantity(line, ref mess);
                    if (++line < ol_cnt)
                        step = 3;
                    break;
                case 7:
                    DoTotal();
                    DoCommit(ref mess);
                    step = -1;
                    break;
            }
            Set(130, mess);
            Invalidate(true);
            step++;
        }
    }
}
