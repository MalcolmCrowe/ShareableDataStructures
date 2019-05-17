using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Shareable;

namespace DictDemo
{
    public partial class Form1 : Form
    {
        // we will add instrumentation to dict
        public SDict<int, int> dict = SDict<int, int>.Empty;
        public int max; // keys will fall in range 0..max
        // we will build accumulators for a number of costs
        public SDict<int, decimal>
            // insert 
            memInsertion, compInsertion,
            // remove
            memRemove, compRemove,
            // ssearch
            compSuccessSearch, compFailedSearch;
        int rTag;
        public Random rnd = new Random(0);
        public int N => dict.Length ?? 0;
        public void Step()
        {
            var x = rnd.Next();
            x = x % max;
            if (Contains(dict, x))
                dict = Remove(dict, x);
            else
                dict = Add(dict, x);
            textBox2.Text = "" + N;
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            pictureBox1.Image = new Bitmap(700, 313);
            Clear();
        }

        private void Clear_Click(object sender, EventArgs e)
        {
            max = 1000;
            Clear();
        }
        private void Clear()
        {
            try
            {
                textBox1.Text = max.ToString();
                dict = SDict<int, int>.Empty;
                memInsertion = compInsertion = memRemove = compRemove = compSuccessSearch = compFailedSearch = SDict<int, decimal>.Empty;
                textBox2.Text = "0";
                Steps10000();
            }
            catch (Exception) { }
        }

        private void Steps10000()
        {
            for (var i = 0; i < 10000; i++)
                Step();
            pictureBox1.Invalidate();
        }
        private void Steps10000_Click(object sender, EventArgs e)
        {
            Steps10000();
        }
        private void PictureBox1_Paint(object sender, PaintEventArgs e)
        {
            var select = SDict<int, decimal>.Empty;
            var display = SDict<int, decimal>.Empty;
            e.Graphics.Clear(Color.White);
            var br = Brushes.Black;
            switch (rTag)
            {
                case 0: select = memInsertion; break;
                case 1: select = compInsertion; break;
                case 2: select = memRemove; break;
                case 3: select = compRemove; break;
                case 4: select = compSuccessSearch; break;
                case 5: select = compFailedSearch; break;
            }
            var count = select.Length??0;
            if (count == 0)
                return;
            var xscale = 700M / count;
            var yscale = 0M;
            for (var b=select.First();b!=null;b=b.Next())
            {
                if (b.Value.Item1 == 0)
                    continue;
                var x = (int)(b.Value.Item1*xscale);
                var y = display.Contains(x) ? display[x] : 0M;
                y += b.Value.Item2;
                display += (x, y);
            }
            for (var b=display.First();b!=null;b=b.Next())
            {
                if (b.Value.Item1 == 0)
                    continue;
                var y = b.Value.Item2;
                if (y > yscale)
                    yscale = y;
            } 
            textBox3.Text = yscale.ToString();
            yscale = yscale / 300;
            for (var b = display.First(); b != null; b = b.Next())
            {
                if (b.Value.Item1 == 0)
                    continue;
                var y = b.Value.Item2;
                var x = b.Value.Item1*1.0;
                e.Graphics.FillRectangle(br, (float)x, 313-(float)(y / yscale), 2, 2);
            }
        }

        private void Radio_CheckedChanged(object sender, EventArgs e)
        {
            var r = (RadioButton)sender;
            if (r.Checked)
                rTag = int.Parse((string)r.Tag);
            pictureBox1.Invalidate();
        }

        private void Button4_Click(object sender, EventArgs e)
        {
            max = max * 10;
            textBox1.Text = "" + max;
            Clear();
            pictureBox1.Invalidate();
        }

        SDict<int,int> Remove(SDict<int,int> d,int x)
        {
            SBucket<int, int>.C = 0;
            SBucket<int, int>.M = 0;
            d -= x;
            decimal c = SBucket<int, int>.C;
            decimal m = SBucket<int, int>.M;
            compRemove = AddIn(compRemove, c);
            memRemove = AddIn(memRemove, m);
            return d;
        }
        SDict<int,int> Add(SDict<int,int> d,int x)
        {
            SBucket<int, int>.C = 0;
            SBucket<int, int>.M = 0;
            d += (x, 1);
            decimal c = SBucket<int, int>.C;
            decimal m = SBucket<int, int>.M;
            compInsertion = AddIn(compInsertion, c);
            memInsertion = AddIn(memInsertion, m);
            return d;
        }
        bool Contains(SDict<int,int> d,int x)
        {
            SBucket<int, int>.C = 0;
            SBucket<int, int>.M = 0;
            var r = d.Contains(x);
            decimal c = SBucket<int, int>.C;
            decimal m = SBucket<int,int>.M;
            if (r)
                compSuccessSearch = AddIn(compSuccessSearch, c);
            else
                compFailedSearch = AddIn(compFailedSearch, c);
            return r;
        }
        SDict<int,decimal> AddIn(SDict<int,decimal> d,decimal c)
        {
            var a = d.Contains(N)?d[N]:0M;
            a = a*0.75M + c * 0.25M;
            return d += (N, a);
        }
        public Form1()
        {
            InitializeComponent();
            memInsertion = compInsertion = memRemove = compRemove = compSuccessSearch = compFailedSearch = SDict<int, decimal>.Empty;
            max = int.Parse(textBox1.Text);
        }
    }
}
