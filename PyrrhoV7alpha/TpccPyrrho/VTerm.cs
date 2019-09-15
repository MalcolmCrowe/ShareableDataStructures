using System;
using System.Collections;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Windows.Forms;
using System.Text;

namespace Tpcc
{
	/// <summary>
	/// Summary description for VTerm.
	/// </summary>
	public class VTerm : System.Windows.Forms.UserControl
	{
		public int hpos,vpos;
		private System.Windows.Forms.PictureBox pictureBox1;
		public static Font ft;
		public float lht,em;
		public Line[] lines;
		public int curField;
		public int nchars;
		string[] strings;
		public void Put(int x,int y,string str)
		{
			hpos = x;
			vpos = y;
			byte[] b = Line.enc.GetBytes(str);
			for (int j=0;j<b.Length && hpos<80;j++)
				lines[y].bytes[hpos++] = b[j];
			lines[y].dirty = true;
		}
		public void Put(byte[] bytes,int off,int len) // first two bytes set vpos and hpos if bytes[off]<32
		{
			int j = 0;
			byte b = bytes[off];
			if (b<32)
			{
				vpos = (int)bytes[off];
				hpos = (int)bytes[off+1];
				j = 2;
			}
			Line x =lines[vpos];
			for (;j<len;j++)
				x.bytes[hpos++] = bytes[j+off];
			x.dirty = true;
		}
		public void Put(int x)
		{
			lines[vpos].bytes[hpos++] = (byte)x;
		}
		public void PutBlanks(int x,int y,int len)
		{
			Line m = lines[y];
			for (int j=0;j<len;j++)
				m.bytes[j+x]=32;
		}
		public void PutBlanks()
		{
			foreach(Field f in fields)
				f.Clear();
		}
		public class Field
		{
			public VTerm vt;
			public int hp,vp,len;
			public bool input=false;
			public Field(VTerm v,int x,int y, int n)
			{
				vt =v;
				hp=x; vp=y; len = n; 
			}
			public void Clear()
			{
				vt.PutBlanks(hp,vp,len);
			}
			public void Put(string s)
			{
                if (s.Length > len)
                    s = s.Substring(0, len);
		//			Console.WriteLine("Field size error: ("+hp+","+vp+","+len+")<"+s+">");
				vt.Put(hp,vp,s);
			}
			public void Put(int k)
			{
				Put(""+k);
			}
			public override string ToString()
			{
				return Line.enc.GetString(vt.lines[vp].bytes,hp,len);
			}
		}
		public void AddField(int x,int y,int n)
		{
			fields.Add(new Field(this,x,y,n));
		}
		public void AddField(int x,int y,int n,bool inp)
		{
			Field f = new Field(this,x,y,n);
			f.input = inp;
			fields.Add(f);
		}
		void SetCurField()
		{
			curField =-1;
			for (int j=0;j<fields.Count;j++)
			{
				Field f = (Field)fields[j];
				if (vpos==f.vp && hpos>=f.hp && hpos<=f.hp+f.len)
				{
					curField = j;
					break;
				}
			}
			nchars = 0;
		}
		public void SetCurField(int c)
		{
			if (c==fields.Count)
				c = 0;
			if (c>=0 && c<=fields.Count-1)
			{
				Field f = (Field)fields[c];
				hpos = f.hp;
				vpos = f.vp;
				curField = c;
				PutBlanks(f.hp,f.vp,f.len);
			}
		}
		public void MoveToNextInput()
		{
			int oc = curField;
			curField = -1;
			for (int c =((oc>=fields.Count-1)?0:oc+1);c!=oc;c=((c>=fields.Count-1)?0:c+1))
			{
				Field f = (Field)fields[c];
				if (f.input)
				{
					SetCurField(c);
					break;
				}
			}
			if (oc>=0)
			{
				Field f = (Field)fields[oc];
				if (f.ToString().Trim()!="")
					EnterField(oc);
			}
		}
		public ArrayList fields = new ArrayList();
		/// <summary> 
		/// Required designer variable.
		/// </summary>
		private System.ComponentModel.Container components = null;

		public VTerm()
		{
			// This call is required by the Windows.Forms Form Designer.
			InitializeComponent();

			// TODO: Add any initialization after the InitializeComponent call
			if (ft==null) 
			{
				ft = new Font("Courier New",8.0F);
				lht = ft.Height;
			}
			lines = new Line[24];
			strings = new string[24];
			for (int j=0;j<24;j++)
				lines[j] = new Line(lht*j);
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

		#region Component Designer generated code
		/// <summary> 
		/// Required method for Designer support - do not modify 
		/// the contents of this method with the code editor.
		/// </summary>
		private void InitializeComponent()
		{
			this.pictureBox1 = new System.Windows.Forms.PictureBox();
			this.SuspendLayout();
			// 
			// pictureBox1
			// 
			this.pictureBox1.Location = new System.Drawing.Point(8, 0);
			this.pictureBox1.Name = "pictureBox1";
			this.pictureBox1.Size = new System.Drawing.Size(544, 328);
			this.pictureBox1.TabIndex = 0;
			this.pictureBox1.TabStop = false;
			this.pictureBox1.Paint += new System.Windows.Forms.PaintEventHandler(this.pictureBox1_Paint);
			this.pictureBox1.MouseDown += new System.Windows.Forms.MouseEventHandler(this.pictureBox1_MouseDown);
			// 
			// VTerm
			// 
			this.Controls.Add(this.pictureBox1);
			this.Name = "VTerm";
			this.Size = new System.Drawing.Size(552, 328);
			this.KeyDown += new System.Windows.Forms.KeyEventHandler(this.VTerm_KeyDown);
			this.ResumeLayout(false);

		}
		#endregion

		private void pictureBox1_Paint(object sender, System.Windows.Forms.PaintEventArgs e)
		{
			Graphics g = e.Graphics;
			if (em==0.0F)
			{
				SizeF s = g.MeasureString(new string('X',80),ft);
				em = s.Width/80-0.05F;
				lht = s.Height;
				for (int j=0;j<24;j++)
					lines[j].fpt = new PointF(0.0F,lht*j);
//				SetCurField(0);
			}
			for (int k=0;k<fields.Count;k++)
			{
				Field f = (Field)fields[k];
				g.FillRectangle(f.input?Brushes.White:Brushes.Yellow,em*f.hp,f.vp*lht,f.len*em,lht);
			}
			for (int j=0;j<24;j++)
			{
				Line x = lines[j];
				if (x.dirty)
				{
					strings[j] = Line.enc.GetString(x.bytes);
					x.dirty = false;
				}
				g.DrawString(strings[j],ft,Brushes.Black,x.fpt);
			}
			if (curField>=0 && curField<=fields.Count-1)
			{
				Field fi = (Field)fields[curField];
				vpos = fi.vp;
				hpos = fi.hp + nchars;
			}
			g.DrawLine(Pens.Black,(int)(hpos*em)+3,(int)(vpos*lht)+1,(int)(hpos*em)+3,(int)(vpos*lht)+14);
		}

		private void pictureBox1_MouseDown(object sender, System.Windows.Forms.MouseEventArgs e)
		{
			hpos = (int)(e.X/em);
			vpos = (int)(e.Y/lht);
			SetCurField();
//			Invalidate(true);
//			Console.WriteLine(""+hpos+","+vpos);
		}
		protected override bool IsInputKey(Keys code)
		{
			return true;
		}
		private void VTerm_KeyDown(object sender, System.Windows.Forms.KeyEventArgs e)
		{
			switch (e.KeyData)
			{
				case Keys.Up: vpos--; break;
				case Keys.Down: vpos++; break;
				case Keys.Left: hpos--; break;
				case Keys.Right: hpos++; break;
				case Keys.Tab: MoveToNextInput(); nchars=0; break;
				case Keys.Enter: EnterField(curField); nchars=0; break;
				case Keys.Back: lines[vpos].bytes[--hpos]=32; nchars--; break;
				case Keys.OemPeriod: lines[vpos].bytes[hpos++]=46; nchars++; break;
				default:
					Put(e.KeyValue); nchars++; lines[vpos].dirty=true; break;
			}
//			Invalidate(true);
		}
		protected virtual void EnterField(int f)
		{
		}
		public virtual void Activate()
		{
		}
		public Field this[int i]
		{
			get
			{
				return (Field)fields[i];
			}
		}
		public void Set(int f,int k)
		{
			lock(this)
			{
				this[f].Put(k);
  //              Invalidate(true);
			}
		}
		public void Set(int f,string s)
		{
			lock(this)
			{
				this[f].Put(s);
   //             Invalidate(true);
			}
		}
		public object Get(IDbConnection db,string s)
		{
			var cmd = db.CreateCommand();
			cmd.CommandText = s;
			object r = cmd.ExecuteScalar();
			cmd.Dispose();
			return r;
		}
        public class Buffer
        {
            public byte[] buffer;  // emulate terminal input: circular buffer of chars typed
            public int bufget, bufput, bufSize; // full if bufput+1==bufget or if bufput==bufSize-1 && bufget==bufSize
            public Buffer()
            {
                bufSize = 1024;
                buffer = new byte[bufSize];
                bufget = 0; bufput = 0;
            }
            public int CharsAvailable()
            {
                int r = bufput - bufget;
                if (r > bufSize)
                    return r - bufSize;
                else if (r < 0)
                    return r + bufSize;
                else
                    return r;
            }
            public byte GetChar()
            {
                if (bufget >= bufSize)
                    bufget = 0;
                return buffer[bufget++];
            }
            bool PutChar(byte b) // private: this control catches KeyPress etc and places chars here as appropriate
            {
                if (bufput + 1 == bufget)
                    return false;
                if (bufput == bufSize - 1 && bufget == bufSize)
                    return false;
                if (bufput >= bufSize)
                    bufput = 0;
                buffer[bufput++] = b;
                return true;
            }
        }
        public struct Line
        {
            public static Encoding enc = new ASCIIEncoding();
            public PointF fpt;
            public byte[] bytes;
            public bool dirty;
            public Line(float y)
            {
                bytes = new byte[81]; fpt = new PointF(0.0F, y); dirty = true;
                for (int j = 0; j < 81; j++)
                    bytes[j] = 32;
            }
            public override string ToString()
            {
                dirty = false;
                return enc.GetString(bytes) + new string(' ', 80 - bytes.Length);
            }
        }
    }
}
