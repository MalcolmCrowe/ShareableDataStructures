using System;
using System.Drawing;
using System.Collections;
using System.ComponentModel;
using System.Windows.Forms;
using Pyrrho;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Configuration;

namespace Tpcc
{
	/// <summary>
	/// Summary description for Form1.
	/// </summary>
	public class Form1 : System.Windows.Forms.Form
	{
		private System.Windows.Forms.TabControl tabControl1;
		private System.Windows.Forms.TabPage tabPage1;
		private System.Windows.Forms.TabPage tabPage2;
		private System.Windows.Forms.TabPage tabPage3;
		private System.Windows.Forms.TabPage tabPage4;
		private System.Windows.Forms.TabPage tabPage5;
		private System.Windows.Forms.Button button1;
		private System.Windows.Forms.Label label2;
        private System.Windows.Forms.TextBox textBox1;
		private Tpcc.OrderStatus orderStatus1;
		private Tpcc.NewOrder newOrder1;
		private Tpcc.Payment payment1;
//		private Tpcc.StockLevel stockLevel1;
 		private System.ComponentModel.IContainer components;
		public PyrrhoConnect conn;
        PyrrhoTransaction tr = null;
		private System.Windows.Forms.Button button2;
		private System.Windows.Forms.Label label1;
		public int wid;
        static int _fid = 0,maxfid=0,maxloaded=0;
        int fid = ++_fid;
        static object _lock = new object();
		private System.Windows.Forms.TextBox textBox3;
		public int activewh;
		private System.Windows.Forms.Button AutoRun;
		private System.Windows.Forms.Button CommitBtn;
		Thread thread = null;
		Thread deferred = null;
        private System.Windows.Forms.Timer timer1,timer2;
		Thread emulate = null;
        private Label label3;
        private TextBox textBox4;
        private CheckBox checkBox1;
        private Button button4;
        private Button button3;
        private TextBox textBox2;
        private Label label4;
        private Button Step;
        private Label label5;
        private TextBox Clerks;
        public static string host;
        public static int commits, rconflicts, wconflicts;
		public Form1(int w)
		{
            wid = w;
			conn = new PyrrhoConnect("Files=Tpcc");
            conn.Open();
            //
            // Required for Windows Form Designer support
            //
            InitializeComponent();
            Control.CheckForIllegalCrossThreadCalls = false;
		}

		/// <summary>
		/// Clean up any resources being used.
		/// </summary>
		protected override void Dispose( bool disposing )
		{
			if( disposing )
			{
				if (thread!=null)
				{
					thread.Abort();
					thread = null;
				}
				if (deferred!=null)
				{
					deferred.Abort();
					deferred = null;
				}
				if (emulate!=null)
				{
					emulate.Abort();
					emulate = null;
				}
				if (components != null) 
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
            this.components = new System.ComponentModel.Container();
            this.tabControl1 = new System.Windows.Forms.TabControl();
            this.tabPage1 = new System.Windows.Forms.TabPage();
            this.label5 = new System.Windows.Forms.Label();
            this.Clerks = new System.Windows.Forms.TextBox();
            this.textBox2 = new System.Windows.Forms.TextBox();
            this.label4 = new System.Windows.Forms.Label();
            this.textBox4 = new System.Windows.Forms.TextBox();
            this.checkBox1 = new System.Windows.Forms.CheckBox();
            this.label3 = new System.Windows.Forms.Label();
            this.AutoRun = new System.Windows.Forms.Button();
            this.textBox1 = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.button4 = new System.Windows.Forms.Button();
            this.button3 = new System.Windows.Forms.Button();
            this.button1 = new System.Windows.Forms.Button();
            this.tabPage2 = new System.Windows.Forms.TabPage();
            this.Step = new System.Windows.Forms.Button();
            this.CommitBtn = new System.Windows.Forms.Button();
            this.textBox3 = new System.Windows.Forms.TextBox();
            this.button2 = new System.Windows.Forms.Button();
            this.newOrder1 = new Tpcc.NewOrder(this,1);
            this.tabPage3 = new System.Windows.Forms.TabPage();
            this.orderStatus1 = new Tpcc.OrderStatus(this,1);
            this.tabPage4 = new System.Windows.Forms.TabPage();
            this.payment1 = new Tpcc.Payment(this,1);
            this.tabPage5 = new System.Windows.Forms.TabPage();
      //      this.stockLevel1 = new Tpcc.StockLevel(this,1);
            this.label1 = new System.Windows.Forms.Label();
            this.timer1 = new System.Windows.Forms.Timer(this.components);
            this.tabControl1.SuspendLayout();
            this.tabPage1.SuspendLayout();
            this.tabPage2.SuspendLayout();
            this.tabPage3.SuspendLayout();
            this.tabPage4.SuspendLayout();
            this.tabPage5.SuspendLayout();
                  this.SuspendLayout();
            // 
            // tabControl1
            // 
            this.tabControl1.Controls.Add(this.tabPage1);
            this.tabControl1.Controls.Add(this.tabPage2);
            this.tabControl1.Controls.Add(this.tabPage3);
            this.tabControl1.Controls.Add(this.tabPage4);
            this.tabControl1.Controls.Add(this.tabPage5);
            this.tabControl1.Location = new System.Drawing.Point(0, 0);
            this.tabControl1.Name = "tabControl1";
            this.tabControl1.SelectedIndex = 0;
            this.tabControl1.Size = new System.Drawing.Size(568, 392);
            this.tabControl1.TabIndex = 0;
            this.tabControl1.SelectedIndexChanged += new System.EventHandler(this.tabControl1_SelectedIndexChanged);
            // 
            // tabPage1
            // 
            this.tabPage1.Controls.Add(this.label5);
            this.tabPage1.Controls.Add(this.Clerks);
            this.tabPage1.Controls.Add(this.textBox2);
            this.tabPage1.Controls.Add(this.label4);
            this.tabPage1.Controls.Add(this.textBox4);
            this.tabPage1.Controls.Add(this.checkBox1);
            this.tabPage1.Controls.Add(this.label3);
            this.tabPage1.Controls.Add(this.AutoRun);
            this.tabPage1.Controls.Add(this.textBox1);
            this.tabPage1.Controls.Add(this.label2);
            this.tabPage1.Controls.Add(this.button4);
            this.tabPage1.Controls.Add(this.button3);
            this.tabPage1.Controls.Add(this.button1);
            this.tabPage1.Location = new System.Drawing.Point(4, 22);
            this.tabPage1.Name = "tabPage1";
            this.tabPage1.Size = new System.Drawing.Size(560, 366);
            this.tabPage1.TabIndex = 0;
            this.tabPage1.Text = "Setup";
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(267, 238);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(35, 13);
            this.label5.TabIndex = 16;
            this.label5.Text = "clerks";
            // 
            // Clerks
            // 
            this.Clerks.Location = new System.Drawing.Point(161, 235);
            this.Clerks.Name = "Clerks";
            this.Clerks.Size = new System.Drawing.Size(100, 20);
            this.Clerks.TabIndex = 15;
            this.Clerks.Text = "1";
            // 
            // textBox2
            // 
            this.textBox2.Location = new System.Drawing.Point(92, 39);
            this.textBox2.Name = "textBox2";
            this.textBox2.Size = new System.Drawing.Size(148, 20);
            this.textBox2.TabIndex = 14;
            this.textBox2.Text = "localhost";
            this.textBox2.TextChanged += new System.EventHandler(this.textBox2_TextChanged);
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(23, 42);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(63, 13);
            this.label4.TabIndex = 13;
            this.label4.Text = "Server Host";
            // 
            // textBox4
            // 
            this.textBox4.Location = new System.Drawing.Point(128, 124);
            this.textBox4.Name = "textBox4";
            this.textBox4.Size = new System.Drawing.Size(56, 20);
            this.textBox4.TabIndex = 12;
            this.textBox4.Text = "1";
            this.textBox4.Visible = false;
            this.textBox4.TextChanged += new System.EventHandler(this.textBox4_TextChanged);
            // 
            // checkBox1
            // 
            this.checkBox1.AutoSize = true;
            this.checkBox1.Location = new System.Drawing.Point(21, 126);
            this.checkBox1.Name = "checkBox1";
            this.checkBox1.Size = new System.Drawing.Size(76, 17);
            this.checkBox1.TabIndex = 11;
            this.checkBox1.Text = "For District";
            this.checkBox1.UseVisualStyleBackColor = true;
            this.checkBox1.CheckedChanged += new System.EventHandler(this.checkBox1_CheckedChanged);
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(18, 185);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(320, 13);
            this.label3.TabIndex = 10;
            this.label3.Text = "To run the NewOrder task, select the NewOrder tab and click Run";
            // 
            // AutoRun
            // 
            this.AutoRun.Location = new System.Drawing.Point(26, 232);
            this.AutoRun.Name = "AutoRun";
            this.AutoRun.Size = new System.Drawing.Size(112, 24);
            this.AutoRun.TabIndex = 9;
            this.AutoRun.Text = "Automatic Mode";
            this.AutoRun.Click += new System.EventHandler(this.AutoRun_Click);
            // 
            // textBox1
            // 
            this.textBox1.Location = new System.Drawing.Point(148, 80);
            this.textBox1.Name = "textBox1";
            this.textBox1.Size = new System.Drawing.Size(56, 20);
            this.textBox1.TabIndex = 3;
            this.textBox1.Text = "1";
            this.textBox1.TextChanged += new System.EventHandler(this.textBox1_TextChanged);
            // 
            // label2
            // 
            this.label2.Location = new System.Drawing.Point(16, 88);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(140, 16);
            this.label2.TabIndex = 2;
            this.label2.Text = "Number of Warehouses";
            // 
            // button4
            // 
            this.button4.Location = new System.Drawing.Point(265, 120);
            this.button4.Name = "button4";
            this.button4.Size = new System.Drawing.Size(88, 24);
            this.button4.TabIndex = 1;
            this.button4.Text = "Fill District(s)";
            this.button4.Click += new System.EventHandler(this.button4_Click);
            // 
            // button3
            // 
            this.button3.Location = new System.Drawing.Point(265, 77);
            this.button3.Name = "button3";
            this.button3.Size = new System.Drawing.Size(88, 24);
            this.button3.TabIndex = 1;
            this.button3.Text = "Add Stock";
            this.button3.Click += new System.EventHandler(this.button3_Click);
            // 
            // button1
            // 
            this.button1.Location = new System.Drawing.Point(265, 29);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(88, 24);
            this.button1.TabIndex = 1;
            this.button1.Text = "Recreate DB";
            this.button1.Click += new System.EventHandler(this.button1_Click);
            // 
            // tabPage2
            // 
            this.tabPage2.Controls.Add(this.Step);
            this.tabPage2.Controls.Add(this.CommitBtn);
            this.tabPage2.Controls.Add(this.textBox3);
            this.tabPage2.Controls.Add(this.button2);
            this.tabPage2.Controls.Add(this.newOrder1);
            this.tabPage2.Location = new System.Drawing.Point(4, 22);
            this.tabPage2.Name = "tabPage2";
            this.tabPage2.Size = new System.Drawing.Size(560, 366);
            this.tabPage2.TabIndex = 1;
            this.tabPage2.Text = "New Order";
            // 
            // Step
            // 
            this.Step.Location = new System.Drawing.Point(399, 336);
            this.Step.Name = "Step";
            this.Step.Size = new System.Drawing.Size(64, 24);
            this.Step.TabIndex = 4;
            this.Step.Text = "Step";
            this.Step.Click += new System.EventHandler(this.Step_Click);
            // 
            // Commit
            // 
            this.CommitBtn.Location = new System.Drawing.Point(304, 336);
            this.CommitBtn.Name = "Commit";
            this.CommitBtn.Size = new System.Drawing.Size(64, 24);
            this.CommitBtn.TabIndex = 3;
            this.CommitBtn.Text = "Commit";
            this.CommitBtn.Click += new System.EventHandler(this.Commit_Click);
            // 
            // textBox3
            // 
            this.textBox3.Location = new System.Drawing.Point(120, 336);
            this.textBox3.Name = "textBox3";
            this.textBox3.Size = new System.Drawing.Size(136, 20);
            this.textBox3.TabIndex = 2;
            this.textBox3.Text = "0";
            // 
            // button2
            // 
            this.button2.Location = new System.Drawing.Point(8, 336);
            this.button2.Name = "button2";
            this.button2.Size = new System.Drawing.Size(72, 24);
            this.button2.TabIndex = 1;
            this.button2.Text = "Run";
            this.button2.Click += new System.EventHandler(this.button2_Click);
            // 
            // newOrder1
            // 
            this.newOrder1.Location = new System.Drawing.Point(8, 0);
            this.newOrder1.Name = "newOrder1";
            this.newOrder1.Size = new System.Drawing.Size(568, 328);
            this.newOrder1.TabIndex = 0;
            this.newOrder1.Enter += new System.EventHandler(this.newOrder1_Enter);
            // 
            // tabPage3
            // 
            this.tabPage3.Controls.Add(this.orderStatus1);
            this.tabPage3.Location = new System.Drawing.Point(4, 22);
            this.tabPage3.Name = "tabPage3";
            this.tabPage3.Size = new System.Drawing.Size(560, 366);
            this.tabPage3.TabIndex = 2;
            this.tabPage3.Text = "Order Status";
            // 
            // orderStatus1
            // 
            this.orderStatus1.Location = new System.Drawing.Point(16, 24);
            this.orderStatus1.Name = "orderStatus1";
            this.orderStatus1.Size = new System.Drawing.Size(536, 360);
            this.orderStatus1.TabIndex = 0;
            // 
            // tabPage4
            // 
            this.tabPage4.Controls.Add(this.payment1);
            this.tabPage4.Location = new System.Drawing.Point(4, 22);
            this.tabPage4.Name = "tabPage4";
            this.tabPage4.Size = new System.Drawing.Size(560, 366);
            this.tabPage4.TabIndex = 3;
            this.tabPage4.Text = "Payment";
            // 
            // payment1
            // 
            this.payment1.Location = new System.Drawing.Point(8, 8);
            this.payment1.Name = "payment1";
            this.payment1.Size = new System.Drawing.Size(536, 384);
            this.payment1.TabIndex = 0;
      /*      // 
            // tabPage5
            // 
            this.tabPage5.Controls.Add(this.stockLevel1);
            this.tabPage5.Location = new System.Drawing.Point(4, 22);
            this.tabPage5.Name = "tabPage5";
            this.tabPage5.Size = new System.Drawing.Size(560, 366);
            this.tabPage5.TabIndex = 4;
            this.tabPage5.Text = "StockLevel";
            // 
            // stockLevel1
            // 
            this.stockLevel1.Location = new System.Drawing.Point(8, 8);
            this.stockLevel1.Name = "stockLevel1";
            this.stockLevel1.Size = new System.Drawing.Size(552, 392);
            this.stockLevel1.TabIndex = 0; */
            // 
            // label1
            // 
            this.label1.Location = new System.Drawing.Point(24, 400);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(536, 24);
            this.label1.TabIndex = 1;
            this.label1.Text = "label1";
            // 
            // timer1
            // 
            this.timer1.Tick += new System.EventHandler(this.timer1_Tick);
            // 
            // Form1
            // 
            this.AutoScaleBaseSize = new System.Drawing.Size(5, 13);
            this.ClientSize = new System.Drawing.Size(568, 430);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.tabControl1);
            this.Name = "Form1";
            this.Text = "TPC/C";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.FormClosing += new FormClosingEventHandler(this.Form1_Closing);
            this.tabControl1.ResumeLayout(false);
            this.tabPage1.ResumeLayout(false);
            this.tabPage1.PerformLayout();
            this.tabPage2.ResumeLayout(false);
            this.tabPage2.PerformLayout();
            this.tabPage3.ResumeLayout(false);
            this.tabPage4.ResumeLayout(false);
            this.tabPage5.ResumeLayout(false);
            this.ResumeLayout(false);

		}
		#endregion

		/// <summary>
		/// The main entry point for the application.
		/// </summary>
		static public void Main(string[] args) 
		{
            if (args.Length >= 1)
                host = args[0];
            try
            {
                Application.Run(new Form1(0));
            } catch (Exception e)
            {
                Console.WriteLine("Main loop caught exception: " + e.Message);
            }
		}
        public void BeginTransaction()
        {
            tr = conn.BeginTransaction();
        }
        public void Rollback()
        {
            if (tr!=null)
                tr.Rollback();
            tr = null;
        }
        public void Commit(string mess)
        {
            tr?.Commit(mess);
            tr = null;
        }
        private void Form1_Load(object sender, System.EventArgs e)
        {
            if (fid > maxfid)
                Console.WriteLine("fid " + fid + " loaded at " + DateTime.Now);
            if (fid > maxloaded)
                maxloaded = fid;
            if (fid != 1)
            {
                PrepareStatements();
                UserChoice();
            }
            else
                try
                {
                    var cmd = conn.CreateCommand();
                    cmd.CommandText = "select count(W_ID) from WAREHOUSE";
                    var rdr = cmd.ExecuteReader();
                    try
                    {
                        if (rdr.Read())
                            activewh = (int)rdr.GetInt64(0);
                        PrepareStatements();
                    }
                    finally
                    {
                        rdr.Close();
                    }
                    textBox1.Text = "" + activewh;
                    //			deferred = new Thread(new ThreadStart(new Deferred(db,wid).Run));
                    //          deferred.Name = "Deferred";
                    //			deferred.Start();
                    PyrrhoConnect.OpenRequests();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Form1_Load caught exception: " + ex.Message);
                }
        }
        private void Form1_Closing(object sender,System.EventArgs e)
        {
      //      PyrrhoConnect.CloseRequests();
        }

		private void button1_Click(object sender, System.EventArgs e)
		{
            var g = new GenBase(conn);
    	    g.BuildTpcc();
            PrepareStatements();
		}
        void PrepareStatements()
        {
            newOrder1.PrepareStatements();
            orderStatus1.PrepareStatements();
            payment1.PrepareStatements();
     //       stockLevel1.PrepareStatements();
          }
		private void tabControl1_SelectedIndexChanged(object sender, System.EventArgs e)
		{
			VTerm vt = null;
			switch (tabControl1.SelectedIndex)
			{
				case 0:
				case 1:
					NewOrder n = newOrder1;
                    n.step = 0;
					vt = n;
					n.activewh = activewh;
					n.status = label1;
					break;
				case 2:
					OrderStatus o = (OrderStatus)tabPage3.Controls[0];
					o.status = label1;
					vt = o;
					break;
				case 3:
					Payment p = (Payment)tabPage4.Controls[0];
					vt = p;
					p.status = label1;
					break;
		/*		case 4:
					StockLevel s = (StockLevel)tabPage5.Controls[0];
					s.did = 1;
					s.status = label1;
					vt = s;
					break; */
			}
			//		VTerm vt = (VTerm)tabControl1.SelectedTab.Controls[0];
			vt.Activate();
		}

		private void textBox1_TextChanged(object sender, System.EventArgs e)
		{
		}

		private void newOrder1_Enter(object sender, System.EventArgs e)
		{
		}

		private void button2_Click(object sender, System.EventArgs e) // Run button for New Order
		{
			NewOrder n = newOrder1;
            n.oneDistrict = checkBox1.Checked;
            if (checkBox1.Checked)
            {
                n.did = int.Parse(textBox4.Text);
                conn = new PyrrhoConnect("Files=Tpcc");
                conn.Open();
            }
			n.btn = button2;
			n.txtBox = textBox3;
			button2.Enabled = false;
			thread = new Thread(new ThreadStart(n.Multiple));
            thread.Name = "Run button";
			thread.Start();
		}

		private void Commit_Click(object sender, System.EventArgs e)
		{
			string mess = "";
			newOrder1.DoCommit(ref mess);
			label1.Text = mess;
		}

		private void AutoRun_Click(object sender, System.EventArgs e)
		{
            var nw = int.Parse(textBox1.Text);
            var nc = int.Parse(Clerks.Text);
            Form1.maxfid += nc;
            commits = 0; rconflicts = 0; wconflicts = 0;
            Console.WriteLine("Started at " + DateTime.Now.ToString()+" with "+nc+" clerks");
            var w = 1;
      //      for (var w =1;w<=nw;w++)
            for (var i = 0; i < nc; i++)
            Task.Run(()=>{
                var f = new Form1(w);
                f.ShowDialog();
            });
            timer2 = new System.Windows.Forms.Timer();
            timer2.Interval = 600000;
            timer2.Tick += new System.EventHandler(timer2_Tick);
            timer2.Enabled = true;
        }
        void timer2_Tick(object sender, EventArgs e)
        {
            lock (PyrrhoConnect.reqs)
            {
                PyrrhoConnect.reqs.WriteLine("At " + DateTime.Now.ToString() + " Commits " + commits + ", Conflicts " + rconflicts + " " + wconflicts);
                Console.WriteLine("At " + DateTime.Now.ToString() + " Commits " + commits + ", Conflicts " + rconflicts + " " + wconflicts);
                PyrrhoConnect.reqs.WriteLine("Last fid=" + maxloaded);
                PyrrhoConnect.reqs.Close();
            }
            Application.Exit();
        }
        int action = -1;
		int stage = -1;
		void UserChoice()
		{
            try
            {
                int i = util.random(0, 21);
                stage = 0;
                if (i < 10)
                {
                    newOrder1.status = label1;
                    newOrder1.wid = wid;
                    newOrder1.PutBlanks();
                    newOrder1.Activate();
                    tabControl1.SelectedIndex = 1;
                    action = 1;
                    timer1.Interval = 500;
                }
                else if (i < 20)
                {
                    payment1.wid = wid;
                    payment1.PutBlanks();
                    tabControl1.SelectedIndex = 3;
                    payment1.status = label1;
                    payment1.Activate();
                    action = 3;
                    timer1.Interval = 3000;
                }
                else// if (i < 21)
                {
                    orderStatus1.wid = wid;
                    orderStatus1.PutBlanks();
                    tabControl1.SelectedIndex = 2;
                    orderStatus1.status = label1;
                    orderStatus1.Activate();
                    action = 2;
                    timer1.Interval = 2000;
                }
         /*       else
                {
                    stockLevel1.wid = wid;
                    stockLevel1.PutBlanks();
                    tabControl1.SelectedIndex = 5;
                    stockLevel1.status = label1;
                    stockLevel1.did = 1;
                    stockLevel1.Activate();
                    action = 4;
                    timer1.Interval = 2000;
                } */
                timer1.Enabled = true;
            } catch (Exception e)
            {
                lock (PyrrhoConnect.reqs)
                    PyrrhoConnect.reqs.WriteLine("UserChoice caught exception: " + e.Message);
            }
		}

		private void timer1_Tick(object sender, System.EventArgs e)
		{
            timer1.Enabled = false;
            while (maxloaded < maxfid)
                Thread.Sleep(1000);
            label1.Text = "";
			try
			{
				switch (action)
				{
					case 0:
						UserChoice();
						stage = 0;
						break;
					case 1:
						newOrder1.Single(ref stage);
						if (stage>=90)
						{
							action=0;
							timer1.Interval = 15000;
						}
						break;
					case 2:
						orderStatus1.Single();
						timer1.Interval = 10000;
						action = 0;
						break;
					case 3:
						payment1.Single();
						timer1.Interval = 3000;
						action = 0;
						break;
			/*		case 4:
						stockLevel1.Single();
						timer1.Interval = 2000;
						action = 0;
						break; */
				}
			}
			catch(Exception ex)
			{
				label1.Text = ex.Message;
                lock(PyrrhoConnect.reqs)
                    PyrrhoConnect.reqs.WriteLine(ex.Message);
                action = 0;
			}
			timer1.Enabled = true;
		}

        private void checkBox1_CheckedChanged(object sender, EventArgs e)
        {
            textBox4.Visible = checkBox1.Checked;
        }

        private void textBox4_TextChanged(object sender, EventArgs e)
        {
        }

        private void button3_Click(object sender, EventArgs e)
        {
            var nw = int.Parse(textBox1.Text);
            var g = new GenBase(conn);
            for (var i=1;i<=nw;i++)
                g.FillWarehouse(i);
        }

        private void button4_Click(object sender, EventArgs e)
        {
            var nw = int.Parse(textBox1.Text);
            if (checkBox1.Checked)
            {
                int d = int.Parse(textBox4.Text);
                conn = new PyrrhoConnect("Files=Tpcc");
                conn.Open();
                for (var i = 1; i <= nw; i++)
                    new GenBase(conn).FillDistrict(i, d);
            }
            else
                for (var i = 1; i <= nw; i++)
                {
                    Console.WriteLine("For Warehouse " + i);
                    new GenBase(conn).FillDistricts(i);
                    Console.WriteLine("Done warehouse " + i);
                }
        }

        private void textBox2_TextChanged(object sender, EventArgs e)
        {
            host = textBox2.Text;
        }

        private void Step_Click(object sender, EventArgs e)
        {
            newOrder1.Step();
        }

    }
}
