using System;
using System.Text;
using System.IO;
using System.Net;
using System.Threading;
using System.Collections.Generic;
using System.Xml;
using System.Xml.Xsl;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho
{
	/// <summary>
	/// Provides a simple HTTP1.1-like interface to Pyrrho DBMS
	/// compatible with web browsers:
	/// </summary>
	internal class HttpService
	{
        /// <summary>
        /// the listener (usually port 8133 and 8180)
        /// </summary>
		HttpListener listener;
        /// <summary>
        /// the host name
        /// </summary>
        string host;
        /// <summary>
        /// the port for HTTP
        /// </summary>
		int port;
        /// <summary>
        /// The port for HTTPS
        /// </summary>
        int sport;
        /// <summary>
        /// constructor: set up the host and port for the HttpService
        /// </summary>
        /// <param name="sv">the host name (default "127.0.0.1")</param>
        /// <param name="p">the port (default 8080)</param>
		public HttpService(string h,int p,int s)
		{
            host = h;
			port = p;
            sport = s;
		}
        /// <summary>
        /// the main service loop for the HttpService
        /// </summary>
		public void Run()
		{
			try 
			{
				listener = new HttpListener();
                if (port>0)
                    listener.Prefixes.Add("http://" + host + ":" + port + "/");
                if (sport > 0)
                    listener.Prefixes.Add("https://" + host + ":" + sport + "/");
                listener.Realm = "PyrrhoDB granted password";
                listener.AuthenticationSchemes = AuthenticationSchemes.Basic;
                listener.Start();
                if (port>0)
                    Console.WriteLine("HTTP service started on port " + port);
                if (sport > 0)
                    Console.WriteLine("HTTPS service started on port " + sport); 
                for (; ; )
				{
                    HttpListenerContext hcx = listener.GetContext();
					new Thread(new ThreadStart(new HttpServer(hcx).Server)).Start();
				}
			} 
			catch (Exception e)
			{
				Console.WriteLine(e.Message);
			}
		}
	}
    /// <summary>
    /// A base class for the responses from HttpService
    /// </summary>
    internal abstract class PyrrhoWebOutput
    {
        public Transaction db;
        public StringBuilder sbuild;
        /// <summary>
        /// A Filter property
        /// </summary>
        public virtual string Filter
        {
            get { return ""; }
            set { }
        }
        /// <summary>
        /// Constructor for the web oputput class
        /// </summary>
        /// <param name="s">the output stream</param>
        protected PyrrhoWebOutput(Transaction d,StringBuilder s,string agent=null)
        {
            sbuild = s;
            db = d;
        }
        /// <summary>
        /// Send results to the client using the PyrrhoWebOutput mechanisms
        /// </summary>
        public virtual void SendResults(HttpListenerResponse rs,Transaction tr,Context cx,
            string rdc)
        {
            Header(rs, tr, cx, rdc);
            if (cx.val is RowSet r)
            {
                BeforeResults();
                for (var e = r.First(cx);e!=null;e=e.Next(cx))
                    PutRow(cx,e);
                AfterResults();
            }
            Footer();
        }
        /// <summary>
        /// Header for the page
        /// </summary>
        public virtual void Header(HttpListenerResponse rs, Transaction tr,
            Context cx, string rdc)
        {
            rs.StatusCode = 200;
            string s = (cx.etag?.ToString() ?? "") + rdc;
            if (s != null)
            {
                rs.AddHeader("ETag", s);
                if (PyrrhoStart.DebugMode)
                    Console.WriteLine("Returning ETag: " + s);
            }
        }
        /// <summary>
        /// Footer for the page
        /// </summary>
        public virtual void Footer() { }
        /// <summary>
        /// Something to introduce the results
        /// </summary>
        public virtual void BeforeResults() { }
        /// <summary>
        /// something to do per row of the results
        /// </summary>
        public virtual void PutRow(Context _cx, Cursor e) { }
        /// <summary>
        /// Finish off the table after the results
        /// </summary>
        public virtual void AfterResults(){ }
    }
    /// <summary>
    /// Plain text page output class
    /// </summary>
    internal class SqlWebOutput : PyrrhoWebOutput
    {
        /// <summary>
        /// simple constructor
        /// </summary>
        /// <param name="s"></param>
        public SqlWebOutput(Transaction d,StringBuilder s)
            : base(d,s)
        { }
        public override void PutRow(Context _cx, Cursor e)
        {
            var dt = e.dataType;
            var oi = (ObInfo)_cx.db.role.obinfos[dt.defpos];
            var cm = "(";
            for (int i = 0; i < dt.Length; i++, cm = ", ")
            {
                var ci = oi.columns[i];
                sbuild.Append(cm);
                sbuild.Append(e[ci.defpos]);
            }
            sbuild.Append(")");
        }
        public override void SendResults(HttpListenerResponse rs, Transaction tr,Context cx, 
            string rdc)
        {
            var cm = "";
            Header(rs, tr,cx, rdc);
            if (cx.val is RowSet r)
            {
                BeforeResults();
                for (var e=r.First(cx); e != null; e = e.Next(cx))
                {
                    sbuild.Append(cm); cm = ",\r\n";
                    PutRow(cx,e);
                }
                AfterResults();
            }
            Footer();
        }
    }
    /// <summary>
    /// HTML page output class
    /// </summary>
    internal class HtmlWebOutput : PyrrhoWebOutput
    {
        Metadata chartType = new Metadata();
        long xcol= 0;
        long ycol = 0;
        long ccol = 0;
        string xdesc = "";
        string ydesc = "";
        string comma = "";
        /// <summary>
        /// simple constructor
        /// </summary>
        /// <param name="s"></param>
        public HtmlWebOutput(Transaction d, StringBuilder s)
            : base(d, s)
        { }
        public override void Header(HttpListenerResponse hrs, Transaction tr,
            Context cx,string rdc)
        {
            base.Header(hrs, tr, cx, rdc);
            sbuild.Append("<!DOCTYPE HTML>\r\n");
            sbuild.Append("<html>\r\n");
            sbuild.Append("<body>\r\n");
            var rs = cx.val as RowSet;
            var fm = (From)cx.obs[rs.defpos];
            var om = tr.objects[fm.target] as DBObject;
            if (om!=null && om.defpos > 0)
            {
                chartType.flags = om.Meta().flags;
                if (om.description != "" && om.description[0] == '<')
                    sbuild.Append(om.description);
            }
            if (chartType.flags != 0)
            {
                for (var co = rs?.info.columns.First(); co != null; co = co.Next())
                {
                    var sl = co.value();
                    var m = sl.Meta();
                    if (m.Has(Sqlx.X))
                    {
                        xcol = sl.defpos;
                        xdesc = m.description;
                    }
                    if (m.Has(Sqlx.Y))
                    {
                        ycol = sl.defpos;
                        ydesc = m.description;
                    }
                    if (m.Has(Sqlx.CAPTION))
                        ccol = sl.defpos;
                }
                if ((xcol ==0) && (ycol ==0))
                    chartType.flags = 0UL;
            }
            if (chartType.flags!=0)
            {
                var wd = 210;
                if (chartType.Has(Sqlx.LEGEND))
                    wd = 310;
                sbuild.Append("<canvas id=\"myCanvas\" width=\""+wd+"\" height=\"210\" style=\"border:1px solid #c3c3c3;\">\r\n");
                sbuild.Append("Your browser does not support the canvas element.</canvas>\r\n"); 
                sbuild.Append("<script type=\"text/javascript\">\r\n");
                sbuild.Append("var canvas = document.getElementById(\"myCanvas\");\r\n");
                sbuild.Append("var ctx = canvas.getContext('2d');\r\n"); 
                sbuild.Append("var chartType = \"" + chartType + "\";\r\n");
                sbuild.Append("var xdesc = \"" + xdesc + "\";\r\n");
                sbuild.Append("var ydesc = \"" + ydesc + "\";\r\n");
                sbuild.Append("var data = [ \r\n");
            }
            else
            {
                sbuild.Append("<table border><tr>");
                for (int i = 0; i < rs.info.Length; i++)
                    sbuild.Append("<th>" + rs.info[i].name + "</th>");
                sbuild.Append("</tr>");
            }
        }
        public string GetVal(TypedValue v)
        {
            return (v!=null && !v.IsNull) ? v.ToString() : "";
        }
        public override void PutRow(Context _cx, Cursor e)
        {
            var dt = e.dataType;
            if (chartType.flags!=0UL)
            {
                sbuild.Append(comma+"[");
                var rc = e[xcol];
                var s = "";
                if (rc != null)
                {
                    s+= GetVal(rc);
                    if (rc.dataType.kind == Sqlx.CHAR)
                        s = "\"" + s + "\"";
                }
                sbuild.Append(s + "," + GetVal(e[ycol]));
                if (ccol >0)
                    sbuild.Append(",\"" + GetVal(e[ccol]) + "\"");
                else
                    sbuild.Append(",\"" + GetVal(e[xcol]) + "\"");
                sbuild.Append("]");
                comma = ",";
            }
            else
            {
                sbuild.Append("<tr>");
                for (int i = 0; i < dt.Length; i++)
                {
                    var s = GetVal(e[dt.representation[i].Item1]);
                    sbuild.Append("<td>" + s + "</td>");
                }
                sbuild.Append("</tr>");
            }
        }
        public override void Footer()
        {
            if (chartType.flags!=0UL)
            {
                sbuild.Append("     ];           var pt = data[0];\r\n");
                sbuild.Append("    // first find data window\r\n");
                sbuild.Append("    var minX = pt[0], maxX = pt[0];\r\n");
                sbuild.Append("    var minY = pt[1], maxY = pt[1];\r\n");
                sbuild.Append("    var sumY = pt[1];\r\n");
                sbuild.Append("    for (var i = 1; i < data.length; i++) {\r\n");
                sbuild.Append("        pt = data[i];\r\n");
                sbuild.Append("        if (pt[0] < minX) minX = pt[0];\r\n");
                sbuild.Append("        if (pt[0] > maxX) maxX = pt[0];\r\n");
                sbuild.Append("        if (pt[1] < minY) minY = pt[1];\r\n");
                sbuild.Append("        if (pt[1] > maxY) maxY = pt[1];\r\n");
                sbuild.Append("        sumY = sumY + pt[1];");
                sbuild.Append("    }\r\n");
                sbuild.Append("    // next sort out axes\r\n");
                sbuild.Append("    var xd = axisd(minX, maxX);\r\n");
                if (chartType.Has(Sqlx.HISTOGRAM))
                {
                    sbuild.Append("    minY=0; \r\n");
                }
                sbuild.Append("    var yd = axisd(minY, maxY);\r\n");
                sbuild.Append("    minY = yd*Math.round(minY*1.0/yd-0.5);\r\n");
                sbuild.Append("    maxY = yd*Math.round(maxY*1.0/yd+1.5);\r\n");
                sbuild.Append("    var wid = canvas.width - 40;\r\n");
                sbuild.Append("    var hig = canvas.height - 30;\r\n"); 
                if (chartType.flags!=0UL)
                {
                    sbuild.Append("    var scx = (maxX == minX) ? 1 : wid / (maxX - minX);\r\n");
                    sbuild.Append("    var scy = (maxY == minY) ? 1 : hig / (maxY - minY);\r\n");
                    sbuild.Append("    var colours = new Array();\r\n");
                }
                if (chartType.Has(Sqlx.HISTOGRAM))
                {
                        sbuild.Append("    pickColours(data.length); drawAxes();\r\n");
                        sbuild.Append("    drawYmarks(); drawHistogram(); \r\n");
                        sbuild.Append("    function drawHistogram() {\r\n");
                        sbuild.Append("       w = 80.0/data.length;\r\n");
                        sbuild.Append("       for(i=0;i<data.length;i++) { \r\n");
                        sbuild.Append("         pt=data[i];\r\n");
                        sbuild.Append("         ctx.fillStyle = colours[i];\r\n");
                        sbuild.Append("         ctx.fillRect(30+w/2+i*w+i*w,trY(pt[1]),w,170-trY(pt[1]));\r\n");
                        sbuild.Append("        }\r\n"); 
                        sbuild.Append("     }\r\n");
              //          if (chartType.Has(Sqlx.LEGEND))
              //          {
                            sbuild.Append("    drawXCaptions();\r\n");
                            sbuild.Append("    function drawXCaptions() {\r\n");
                            sbuild.Append("       for(i=0;i<data.length;i++) {\r\n");
                            sbuild.Append("         pt = data[i]; \r\n");
                            sbuild.Append("         ctx.fillStyle = \"black\";\r\n");
                            sbuild.Append("         ctx.fillText(pt[2],40+i*160/data.length,180);\r\n");
                            sbuild.Append("       }\r\n");
                            sbuild.Append("    }\r\n");
               //         }
                } else if (chartType.Has(Sqlx.LINE))
                {
                        sbuild.Append("    drawAxes();drawYmarks(); drawXmarks(); drawLineGraph();\r\n");
                        sbuild.Append("    function drawLineGraph() {\r\n");
                        sbuild.Append("      // now plot the data points\r\n");
                        sbuild.Append("      pt = data[0];\r\n");
                        sbuild.Append("      ctx.beginPath();\r\n");
                        sbuild.Append("      ctx.moveTo(trX(pt[0]), trY(pt[1]));\r\n");
                        if (chartType.Has(Sqlx.LEGEND))
                        {
                            sbuild.Append("      if (pt.length > 2)\r\n");
                            sbuild.Append("        ctx.fillText(pt[2], trX(pt[0]), trY(pt[1]));\r\n");
                        }
                        sbuild.Append("      for (i = 1; i < data.length; i++) {\r\n");
                        sbuild.Append("        pt = data[i];\r\n");
                        sbuild.Append("        ctx.lineTo(trX(pt[0]), trY(pt[1]));\r\n");
                        if (chartType.Has(Sqlx.LEGEND))
                        {
                            sbuild.Append("        if (pt.length > 2)\r\n");
                            sbuild.Append("            ctx.fillText(pt[2], trX(pt[0]), trY(pt[1]));\r\n");
                        }
                        sbuild.Append("       }\r\n");
                        sbuild.Append("     ctx.lineWidth = 1.5;\r\n");
                        sbuild.Append("     ctx.stroke();\r\n");
                        sbuild.Append("    }\r\n");
                } else if (chartType.Has(Sqlx.POINTS))
                {
                        sbuild.Append("    drawAxes(); drawYmarks(); drawXmarks(); drawPoints();\r\n");
                        sbuild.Append("    function drawPoints() {\r\n");
                        sbuild.Append("       for (i=0;i<data.length;i++) {\r\n");
                        sbuild.Append("         pt = data[i];\r\n");
                        sbuild.Append("         ctx.fillStyle=\"red\";\r\n");
                        sbuild.Append("         ctx.fillRect(trX(pt[0])-1,trY(pt[1])-1,3,3);\r\n");
                        if (chartType.Has(Sqlx.LEGEND))
                        {
                            sbuild.Append("         ctx.fillStyle=\"black\";\r\n");
                            sbuild.Append("         ctx.fillText(pt[2],trX(pt[0])-1,trY(pt[1])-8);\r\n");
                        }
                        sbuild.Append("       }\r\n");
                        sbuild.Append("    }\r\n");
                } else if(chartType.Has(Sqlx.PIE))
                {
                        sbuild.Append("    pickColours(data.length); drawPie(); \r\n");
                        sbuild.Append("    function drawPie() {\r\n");
                        sbuild.Append("      var ang = 0;\r\n");
                        sbuild.Append("      for(i=0;i<data.length;i++) {\r\n");
                        sbuild.Append("         pt = data[i];\r\n");
                        sbuild.Append("         var nang = ang+2*Math.PI*pt[1]/sumY;\r\n");
                        sbuild.Append("         ctx.beginPath();\r\n");
                        sbuild.Append("         ctx.moveTo(100,100);\r\n");
                        sbuild.Append("         ctx.arc(100,100,80,ang,nang,false);\r\n");
                        sbuild.Append("         ctx.closePath();\r\n");
                        sbuild.Append("         ctx.fillStyle=colours[i];\r\n");
                        sbuild.Append("         ctx.fill();\r\n");
                        sbuild.Append("         var m = (ang+nang)/2;\r\n");
                        if (chartType.Has(Sqlx.LEGEND))
                        {
                            sbuild.Append("         ctx.fillStyle = \"black\";\r\n");
                            sbuild.Append("         ctx.fillText(pt[2],80+50*Math.cos(m),100+50*Math.sin(m));\r\n");
                        }
                        sbuild.Append("         ang = nang;\r\n");
                        sbuild.Append("        }\r\n");
                        sbuild.Append("      }\r\n");
                }
                if(chartType.Has(Sqlx.LEGEND))
                {
                    sbuild.Append("    drawLegend();\r\n");
                    sbuild.Append("    function drawLegend() {\r\n");
                    sbuild.Append("      for(i=0;i<data.length;i++) {\r\n");
                    sbuild.Append("         pt = data[i];\r\n");    
                    sbuild.Append("         ctx.fillStyle=colours[i];\r\n");
                    sbuild.Append("         ctx.fillRect(200,15+13*i,5,5);\r\n");
                    sbuild.Append("         ctx.fillStyle=\"black\";\r\n");
                    sbuild.Append("         ctx.fillText(pt[2],210,20+13*i);\r\n");
                    sbuild.Append("        }\r\n");
                    sbuild.Append("      }\r\n");               
                }
                if (chartType.flags!=0UL && !chartType.Has(Sqlx.PIE))
                {
                    sbuild.Append("    function drawAxes() {\r\n");
                    sbuild.Append("      ctx.beginPath();\r\n");
                    sbuild.Append("      ctx.moveTo(35, 0);\r\n");
                    sbuild.Append("      ctx.lineTo(35, hig);\r\n");
                    sbuild.Append("      ctx.lineTo(canvas.width, hig);\r\n");
                    sbuild.Append("      ctx.lineWidth = 0.5;\r\n");
                    sbuild.Append("      ctx.stroke();\r\n"); 
                    sbuild.Append("    }\r\n");
                    sbuild.Append("    function drawYmarks() {\r\n");
                    sbuild.Append("    for (var y = minY; y < maxY; y += yd)\r\n");
                    sbuild.Append("        drawYMark(y);\r\n");
                    sbuild.Append("      ctx.save(); ctx.rotate(-Math.PI/2);");
                    sbuild.Append("      ctx.fillText(ydesc,-2*hig/3,10);\r\n");
                    sbuild.Append("      ctx.restore();");
                    sbuild.Append("    }\r\n");
                    sbuild.Append("    function drawYMark(v) {\r\n");
                    sbuild.Append("        var y = hig-hig * (v-minY) / (maxY-minY);\r\n");
                    sbuild.Append("        ctx.beginPath();\r\n");
                    sbuild.Append("        ctx.moveTo(35,y);\r\n");
                    sbuild.Append("        ctx.lineTo(30,y);\r\n");
                    sbuild.Append("        ctx.stroke();\r\n");
                    sbuild.Append("        ctx.fillText(Math.round(v), 15, y);\r\n");
                    sbuild.Append("    }\r\n");
                }
                if(chartType.Has(Sqlx.POINTS)||chartType.Has(Sqlx.LINE))
                {
                    sbuild.Append("    function drawXmarks() {\r\n");
                    sbuild.Append("    // draw axis marks\r\n");
                    sbuild.Append("        var x0 = xd*Math.round(minX*1.0/xd-0.5);\r\n");
                    sbuild.Append("        for (var x = x0; x < maxX; x += xd)\r\n");
                    sbuild.Append("        drawXMark(x);\r\n");
                    sbuild.Append("      ctx.fillText(xdesc,wid/3+40,hig+25);\r\n");
                    sbuild.Append("    }\r\n");
                    sbuild.Append("    function drawXMark(v) {\r\n");
                    sbuild.Append("        var x = 35 + wid * (v - minX) / (maxX - minX);\r\n");
                    sbuild.Append("        ctx.beginPath();\r\n");
                    sbuild.Append("        ctx.moveTo(x, hig);\r\n");
                    sbuild.Append("        ctx.lineTo(x, hig + 5);\r\n");
                    sbuild.Append("        ctx.stroke();\r\n");
                    sbuild.Append("        ctx.fillText(v.toPrecision(3), x, canvas.height-20);\r\n");
                    sbuild.Append("    }\r\n");
                }
                sbuild.Append("    function abs(x) { return (x>0)?x:-x; }\r\n");
                sbuild.Append("    function pos(x) { return (x>0)?x:0; }\r\n");
                sbuild.Append("    function blend(x,a) { return Math.round(255*(pos(1-abs(x+3-a))+\r\n");
                sbuild.Append("           pos(1-abs(x-a))+pos(1-abs(x-3-a)))); } \r\n");
                sbuild.Append("    function colour(x) { return \"rgb(\"+blend(x,0.5)+\",\"+\r\n");
                sbuild.Append("        blend(x,1.5)+\",\"+blend(x,2.5)+\")\"; }\r\n");
                sbuild.Append("    function pickColours(n) {\r\n");
                sbuild.Append("        for(i=0;i<n;i++) colours[i]=colour(i*3.0/n);\r\n");
                sbuild.Append("     }\r\n");
                sbuild.Append("    function trX(x) { return (x - minX) * scx + 35; }\r\n");
                sbuild.Append("    function trY(y) { return hig - (y - minY) * scy; }\r\n");
                sbuild.Append("    // decide on axis marks: between 4 and 10\r\n");
                sbuild.Append("    // e.g. id minX is 102 and maxX is 233, decide on d=20, range [100,240]\r\n");
                sbuild.Append("    // examples: r=0.42, want d=0.1; r=7523, want d=2000\r\n");
                sbuild.Append("    // so: divide r by 5, take log10 and look at mantissa 0,.3,.6,1\r\n");
                sbuild.Append("    function axisd(low, high) {\r\n");
                sbuild.Append("        var m = (high + low)/2.0;\r\n");
                sbuild.Append("        var lm = Math.log(m) / Math.LN10;\r\n");
                sbuild.Append("        var c = Math.floor(lm);\r\n");
                sbuild.Append("        var d = Math.pow(10,c);\r\n");
                sbuild.Append("        while (d*10<high-low) d=2*d;");
                sbuild.Append("        return d;\r\n");
                sbuild.Append("    }\r\n");
                sbuild.Append(" </script>\r\n");
            }
            else
                sbuild.Append("</table>");
            sbuild.Append("</body></html>\r\n");
        }
    }
    internal class JsonWebOutput : PyrrhoWebOutput
    {
        public JsonWebOutput(Transaction db, StringBuilder s) : base(db, s)
        { }
        public override void PutRow(Context _cx, Cursor e)
        {
            var rt = e._info;
            var doc = new TDocument();
     //       var rv = e._Rvv();
      //      if (rv == null && db.affected.Count > 0)
     //           rv = Rvvs.New(db.affected[0]);
     //       doc.Add(TDocument._id, new TChar(rv?.ToString()??""));
            for(int i=0;i<rt.Length;i++)
                if (e[rt[i].defpos].NotNull() is TypedValue tv)
                    doc.Add(rt[i].name, tv);
            sbuild.Append(doc.ToString());
        }
    }
    /// <summary>
    /// XML output class
    /// </summary>
    internal class XmlWebOutput : PyrrhoWebOutput
    {
        public string rootName;
        /// <summary>
        /// a simple constructor
        /// </summary>
        /// <param name="s"></param>
        public XmlWebOutput(Transaction db,StringBuilder s,string rn)
            : base(db,s)
        {
            rootName = rn;
        }
/*        /// <summary>
        /// Output a row for XML
        /// </summary>
        /// <param name="rdr">the results</param>
        public override void PutRow(Context _cx, Cursor e)
        {
            RowSet tp = e._rs;
            var dt = tp.rowType;
            var rc = new TypedValue[dt.Length];
            for (int i = 0; i < dt.Length; i++)
                rc[i] = e.row[dt[i].defpos];
            var fm = tp.qry as From;
            var tb = e._rs._tr.objects[fm.target] as Table;
            sbuild.Append(dt.Xml(tp._tr as Transaction, _cx,tb?.defpos??-1L, new TRow(dt, rc)));
        } */
    }
    /// <summary>
    /// The HttpServer class
    /// </summary>
	internal class HttpServer 
	{
        /// <summary>
        /// The HttpContext
        /// </summary>
        protected HttpListenerContext client;
        string agent = null;
        protected PyrrhoWebOutput woutput;
        StringBuilder sbuild;
        /// <summary>
        /// the database path
        /// </summary>
        string path;

        /// <summary>
        /// constructor
        /// </summary>
		public HttpServer(HttpListenerContext h)
		{
            client = h;
            path = h.Request.RawUrl;
            agent = h.Request.UserAgent;
            sbuild = new StringBuilder();
		}
         internal void Server()
        {
            woutput = null;
            try
            {
                if (PyrrhoStart.TutorialMode || PyrrhoStart.DebugMode)
                    Console.WriteLine("HTTP " + client.Request.HttpMethod + " " + path);
                string[] pathbits = path.Split('/');
                if (path == "/")
                {
                    TransactionOperation();
                    return;
                }
                if (path.Length <= 2)
                    return;
                var dbn = new Ident(pathbits[1],0);
                if (dbn.ident.EndsWith(".htm"))
                {
                    var rdr = new StreamReader(PyrrhoStart.path + path);
                    var rds = rdr.ReadToEnd();
                    rdr.Close();
                    client.Response.StatusCode = 200;
                    sbuild.Append(rds);
                    return;
                }
                string role = dbn.ident;
                var db = Database.Get(dbn.ident).Transact(Transaction.Analysing,"");
                var h = client.Request.Headers["Authorization"];
                var s = Encoding.UTF8.GetString(Convert.FromBase64String(h.Substring(6))).Split(':');
                var details = BTree<string, string>.Empty;
                details+=("User", s[0]);
                details+=("Password", s[1]);
        //        ConnectionFor(dbn, details);
                var acc = client.Request.Headers["Accept"];
                if (acc != null && acc.Contains("text/plain"))
                    woutput = new SqlWebOutput(db, sbuild);
                else if (acc != null && acc.Contains("text/html"))
                    woutput = new HtmlWebOutput(db, sbuild);
                else if (acc != null && acc.Contains("xml"))
                    woutput = new XmlWebOutput(db, sbuild, "root");
                else
                    woutput = new JsonWebOutput(db, sbuild);
                byte[] bytes = new byte[client.Request.ContentLength64];
                if (bytes.Length > 0)
                    client.Request.InputStream.Read(bytes, 0, bytes.Length);
                var sb = Encoding.UTF8.GetString(bytes);
                var et = client.Request.Headers["If-Match"];
                if (PyrrhoStart.DebugMode)
                {
                    if (sb != "" && sb!=null)
                        Console.WriteLine(sb);
                    if (et != null)
                        Console.WriteLine("If-Match: " + et);
                }
                var cx = new Context(db);
                db.Execute(cx,client.Request.HttpMethod,"H",pathbits, client.Request.Headers["Content-Type"],
                    sb, et);
                woutput.SendResults(client.Response,db as Transaction,cx,"");
                return;
            }
            catch (DBException e)
            {
                if (woutput != null)
                    try
                    {
                        client.Response.StatusCode = 400;
                        woutput.sbuild.Append("SQL Error: ");
                        woutput.sbuild.Append(Resx.Format(e.signal, e.objects));
                        for (var ii = e.info.First(); ii != null; ii = ii.Next())
                            woutput.sbuild.Append("\n\r" + ii.key() + ": " + ii.value());
                        woutput.sbuild.Append("\n\r");
                    }
                    catch (Exception) { }
            }
            catch (IOException)
            {
            }
            catch (Exception e)
            {
                if (woutput != null && client.Response.StatusCode == 200)
                    try
                    {
                        client.Response.StatusCode = 500;
                        woutput.sbuild.Append("<p>Pyrrho DBMS Internal Error: " + e.Message + "</p>");
                    }
                    catch (Exception) { }
            }
            finally
            {
                Close();
            }
        }
        /// <summary>
        ///  for posted data we use this routine
        /// </summary>
        /// <returns>a string</returns>
		string ReadLine()
		{
			var a = new List<byte>();
			byte b;
			int i;
			for (;;)
			{
				i = client.Request.InputStream.ReadByte();
				if (i<0)
					break;
				b = (byte)i;
				if (b>=32)
					a.Add(b);
				if (b=='\n' || b=='\0')
					break;
			}
			byte[] bytes = new byte[a.Count];
			for (int j=0;j<bytes.Length;j++)
				bytes[j] = (byte)a[j];
			return Encoding.UTF8.GetString(bytes,0,bytes.Length);
		}
        internal virtual void TransactionOperation()
        {
            // nothing - though it is a surprise if we get here
        }
        internal virtual void Close()
        {
            if (sbuild!= null)
                try
                {
                    var bs = Encoding.UTF8.GetBytes(sbuild.ToString());
                    client.Response.ContentLength64 = bs.Length;
                    client.Response.OutputStream.Write(bs, 0, bs.Length);
                    client.Response.Close();
                }
                catch (Exception) { }
        }
	}
}

