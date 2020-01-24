using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace ProfileViewer
{
    public partial class Form1 : Form
    {
        internal Profile profile = null;
        internal TreeView tree
        {
            get { return treeView1; }
        }

        public Form1()
        {
            InitializeComponent();
        }

        void ShowProfile()
        {
            int tott = 0;
            if (profile==null)
                return;
            foreach (var c in profile.combined)
                tott += c.Show(this);
            label2.Text = tott.ToString() + " transactions";
            label3.Text = profile.date.ToString();
        }

        private void button1_Click(object sender, EventArgs e) // Load from XML file
        {
            treeView1.Nodes.Clear();
            profile = new Profile(textBox1.Text);
            profile.Load();
            if (profile.combined.Count == 0)
            {
                label2.Text = "File " + textBox1.Text + ".xml not found or is empty";
                return;
            }
            ShowProfile();
        }

        private void button2_Click(object sender, EventArgs e) // Fetch from PyyrrhoDBMS
        {
            treeView1.Nodes.Clear();
            profile = new Profile(textBox1.Text);
            profile.Fetch();
            profile.date = DateTime.Now;
            ShowProfile();
        }

        private void checkBox1_CheckedChanged(object sender, EventArgs e)
        {
            treeView1.Nodes.Clear();
            ShowProfile();
        }
    }
}
