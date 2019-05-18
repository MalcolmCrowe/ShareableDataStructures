namespace DictDemo
{
    partial class Form1
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.label1 = new System.Windows.Forms.Label();
            this.textBox1 = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.textBox2 = new System.Windows.Forms.TextBox();
            this.button1 = new System.Windows.Forms.Button();
            this.pictureBox1 = new System.Windows.Forms.PictureBox();
            this.memInsert = new System.Windows.Forms.RadioButton();
            this.memRem = new System.Windows.Forms.RadioButton();
            this.compSuccess = new System.Windows.Forms.RadioButton();
            this.compInsert = new System.Windows.Forms.RadioButton();
            this.compRem = new System.Windows.Forms.RadioButton();
            this.compFailed = new System.Windows.Forms.RadioButton();
            this.textBox3 = new System.Windows.Forms.TextBox();
            this.button4 = new System.Windows.Forms.Button();
            this.label4 = new System.Windows.Forms.Label();
            this.button2 = new System.Windows.Forms.Button();
            this.label3 = new System.Windows.Forms.Label();
            this.depthBox = new System.Windows.Forms.TextBox();
            this.pictureBox2 = new System.Windows.Forms.PictureBox();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox2)).BeginInit();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(25, 19);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(27, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "Max";
            // 
            // textBox1
            // 
            this.textBox1.Location = new System.Drawing.Point(86, 17);
            this.textBox1.Name = "textBox1";
            this.textBox1.ReadOnly = true;
            this.textBox1.Size = new System.Drawing.Size(117, 20);
            this.textBox1.TabIndex = 1;
            this.textBox1.Text = "100";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(232, 19);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(15, 13);
            this.label2.TabIndex = 2;
            this.label2.Text = "N";
            // 
            // textBox2
            // 
            this.textBox2.Location = new System.Drawing.Point(261, 16);
            this.textBox2.Name = "textBox2";
            this.textBox2.ReadOnly = true;
            this.textBox2.Size = new System.Drawing.Size(99, 20);
            this.textBox2.TabIndex = 3;
            this.textBox2.Text = "0";
            // 
            // button1
            // 
            this.button1.Location = new System.Drawing.Point(230, 56);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(101, 24);
            this.button1.TabIndex = 4;
            this.button1.Text = "More";
            this.button1.UseVisualStyleBackColor = true;
            this.button1.Click += new System.EventHandler(this.More_Click);
            // 
            // pictureBox1
            // 
            this.pictureBox1.Location = new System.Drawing.Point(28, 112);
            this.pictureBox1.Name = "pictureBox1";
            this.pictureBox1.Size = new System.Drawing.Size(747, 313);
            this.pictureBox1.TabIndex = 7;
            this.pictureBox1.TabStop = false;
            this.pictureBox1.Paint += new System.Windows.Forms.PaintEventHandler(this.PictureBox1_Paint);
            // 
            // memInsert
            // 
            this.memInsert.AutoSize = true;
            this.memInsert.Checked = true;
            this.memInsert.Location = new System.Drawing.Point(531, 17);
            this.memInsert.Name = "memInsert";
            this.memInsert.Size = new System.Drawing.Size(73, 17);
            this.memInsert.TabIndex = 8;
            this.memInsert.TabStop = true;
            this.memInsert.Tag = "0";
            this.memInsert.Text = "memInsert";
            this.memInsert.UseVisualStyleBackColor = true;
            this.memInsert.CheckedChanged += new System.EventHandler(this.Radio_CheckedChanged);
            // 
            // memRem
            // 
            this.memRem.AutoSize = true;
            this.memRem.Location = new System.Drawing.Point(531, 40);
            this.memRem.Name = "memRem";
            this.memRem.Size = new System.Drawing.Size(87, 17);
            this.memRem.TabIndex = 8;
            this.memRem.Tag = "2";
            this.memRem.Text = "memRemove";
            this.memRem.UseVisualStyleBackColor = true;
            this.memRem.CheckedChanged += new System.EventHandler(this.Radio_CheckedChanged);
            // 
            // compSuccess
            // 
            this.compSuccess.AutoSize = true;
            this.compSuccess.Location = new System.Drawing.Point(531, 63);
            this.compSuccess.Name = "compSuccess";
            this.compSuccess.Size = new System.Drawing.Size(98, 17);
            this.compSuccess.TabIndex = 8;
            this.compSuccess.Tag = "4";
            this.compSuccess.Text = "successSearch";
            this.compSuccess.UseVisualStyleBackColor = true;
            this.compSuccess.CheckedChanged += new System.EventHandler(this.Radio_CheckedChanged);
            // 
            // compInsert
            // 
            this.compInsert.AutoSize = true;
            this.compInsert.Location = new System.Drawing.Point(632, 17);
            this.compInsert.Name = "compInsert";
            this.compInsert.Size = new System.Drawing.Size(77, 17);
            this.compInsert.TabIndex = 8;
            this.compInsert.Tag = "1";
            this.compInsert.Text = "compInsert";
            this.compInsert.UseVisualStyleBackColor = true;
            this.compInsert.CheckedChanged += new System.EventHandler(this.Radio_CheckedChanged);
            // 
            // compRem
            // 
            this.compRem.AutoSize = true;
            this.compRem.Location = new System.Drawing.Point(632, 40);
            this.compRem.Name = "compRem";
            this.compRem.Size = new System.Drawing.Size(91, 17);
            this.compRem.TabIndex = 8;
            this.compRem.Tag = "3";
            this.compRem.Text = "compRemove";
            this.compRem.UseVisualStyleBackColor = true;
            this.compRem.CheckedChanged += new System.EventHandler(this.Radio_CheckedChanged);
            // 
            // compFailed
            // 
            this.compFailed.AutoSize = true;
            this.compFailed.Location = new System.Drawing.Point(632, 63);
            this.compFailed.Name = "compFailed";
            this.compFailed.Size = new System.Drawing.Size(84, 17);
            this.compFailed.TabIndex = 8;
            this.compFailed.Tag = "5";
            this.compFailed.Text = "failedSearch";
            this.compFailed.UseVisualStyleBackColor = true;
            this.compFailed.CheckedChanged += new System.EventHandler(this.Radio_CheckedChanged);
            // 
            // textBox3
            // 
            this.textBox3.Location = new System.Drawing.Point(385, 56);
            this.textBox3.Name = "textBox3";
            this.textBox3.Size = new System.Drawing.Size(100, 20);
            this.textBox3.TabIndex = 9;
            // 
            // button4
            // 
            this.button4.Location = new System.Drawing.Point(128, 57);
            this.button4.Name = "button4";
            this.button4.Size = new System.Drawing.Size(75, 23);
            this.button4.TabIndex = 10;
            this.button4.Text = "Max*10";
            this.button4.UseVisualStyleBackColor = true;
            this.button4.Click += new System.EventHandler(this.Max10_Click);
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(361, 59);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(14, 13);
            this.label4.TabIndex = 2;
            this.label4.Text = "Y";
            // 
            // button2
            // 
            this.button2.Location = new System.Drawing.Point(28, 57);
            this.button2.Name = "button2";
            this.button2.Size = new System.Drawing.Size(75, 23);
            this.button2.TabIndex = 11;
            this.button2.Text = "Clear";
            this.button2.UseVisualStyleBackColor = true;
            this.button2.Click += new System.EventHandler(this.Clear_Click);
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(377, 20);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(15, 13);
            this.label3.TabIndex = 2;
            this.label3.Text = "D";
            // 
            // depthBox
            // 
            this.depthBox.Location = new System.Drawing.Point(406, 17);
            this.depthBox.Name = "depthBox";
            this.depthBox.ReadOnly = true;
            this.depthBox.Size = new System.Drawing.Size(99, 20);
            this.depthBox.TabIndex = 3;
            this.depthBox.Text = "0";
            // 
            // pictureBox2
            // 
            this.pictureBox2.Location = new System.Drawing.Point(28, 431);
            this.pictureBox2.Name = "pictureBox2";
            this.pictureBox2.Size = new System.Drawing.Size(747, 313);
            this.pictureBox2.TabIndex = 7;
            this.pictureBox2.TabStop = false;
            this.pictureBox2.Paint += new System.Windows.Forms.PaintEventHandler(this.PictureBox2_Paint);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 773);
            this.Controls.Add(this.button2);
            this.Controls.Add(this.button4);
            this.Controls.Add(this.textBox3);
            this.Controls.Add(this.compSuccess);
            this.Controls.Add(this.compFailed);
            this.Controls.Add(this.compRem);
            this.Controls.Add(this.compInsert);
            this.Controls.Add(this.memRem);
            this.Controls.Add(this.memInsert);
            this.Controls.Add(this.pictureBox2);
            this.Controls.Add(this.pictureBox1);
            this.Controls.Add(this.button1);
            this.Controls.Add(this.depthBox);
            this.Controls.Add(this.textBox2);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.textBox1);
            this.Controls.Add(this.label1);
            this.Name = "Form1";
            this.Text = "Form1";
            this.Load += new System.EventHandler(this.Form1_Load);
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox2)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox textBox1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.TextBox textBox2;
        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.PictureBox pictureBox1;
        private System.Windows.Forms.RadioButton memInsert;
        private System.Windows.Forms.RadioButton memRem;
        private System.Windows.Forms.RadioButton compSuccess;
        private System.Windows.Forms.RadioButton compInsert;
        private System.Windows.Forms.RadioButton compRem;
        private System.Windows.Forms.RadioButton compFailed;
        private System.Windows.Forms.TextBox textBox3;
        private System.Windows.Forms.Button button4;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Button button2;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TextBox depthBox;
        private System.Windows.Forms.PictureBox pictureBox2;
    }
}

