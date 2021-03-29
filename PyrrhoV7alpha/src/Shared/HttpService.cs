using System;
using System.Text;
using System.IO;
using System.Net;
using System.Threading;
using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

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
            var r = cx.data[cx.result];
            Cursor e = r?.First(cx);
            Header(rs, tr, cx, rdc);
            if (r!=null)
            {
                BeforeResults();
                for (;e!=null;e=e.Next(cx))
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
            RowSet r = cx.data[cx.result];
            if (r == null && cx.exec is QuerySearch us)
                r = cx.data[us.nuid];
            if (r == null && cx.exec is SqlInsert si)
                r = cx.data[si.nuid];
            if (r!=null && cx.db.role.infos[r.rsTargets.First().key()] is ObInfo oi)
            {
                if (oi.description is string ds && ds != "")
                    rs.AddHeader("Description", ds);
                if (oi.classification != Level.D)
                    rs.AddHeader("Classification", oi.classification.ToString());
                if (cx.obs[r.target] is Table tb)
                    rs.AddHeader("LastData", tb.lastData.ToString());
            }
            string s = (r?._Rvv(cx)?.ToString() ?? "") + rdc;
            if (s != "")
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
        public override void Header(HttpListenerResponse rs, Transaction tr, Context cx, string rdc)
        {
            rs.AddHeader("Content-Type", "text/plain");
            base.Header(rs, tr, cx, rdc);
        }
        public override void PutRow(Context cx, Cursor e)
        {
            var dt = e.dataType;
            var cm = "(";
            for (var b = cx.Cols(e._rowsetpos).First();b!=null;b=b.Next())
            {
                var p = b.value();
                sbuild.Append(cm);
                sbuild.Append(e[p]);
            }
            sbuild.Append(")");
        }
        public override void SendResults(HttpListenerResponse rs, Transaction tr,Context cx, 
            string rdc)
        {
            var cm = "";
            Header(rs, tr,cx, rdc);
            if (cx.data[cx.result] is RowSet r)
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
        BTree<Sqlx,object> chartType = BTree<Sqlx,object>.Empty;
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
            hrs.AddHeader("Content-Type", "text/html");
            base.Header(hrs, tr, cx, rdc);
            sbuild.Append("<!DOCTYPE HTML>\r\n");
            sbuild.Append("<html>\r\n");
            sbuild.Append("<body>\r\n");
            var rs = cx.data[cx.result];
            var fm = (From)cx.obs[rs.defpos];
            var om = tr.objects[fm.target] as DBObject;
            var mi = (ObInfo)tr.role.infos[om.defpos];
            if (om!=null && om.defpos > 0)
            {
                chartType = mi.metadata;
                if (mi.description != "" && mi.description[0] == '<')
                    sbuild.Append(mi.description);
            }
            var oi = rs.rt;
            if (chartType != BTree<Sqlx,object>.Empty)
            {
                for (var co = oi.First(); co != null; co = co.Next())
                {
                    var p = co.value();
                    var ci = (ObInfo)cx.db.role.infos[p];
                    if (mi.metadata.Contains(Sqlx.X))
                    {
                        xcol = ci.defpos;
                        xdesc = mi.description;
                    }
                    if (mi.metadata.Contains(Sqlx.Y))
                    {
                        ycol = ci.defpos;
                        ydesc = mi.description;
                    }
                    if (mi.metadata.Contains(Sqlx.CAPTION))
                        ccol = ci.defpos;
                }
                if ((xcol ==0) && (ycol ==0))
                    chartType = BTree<Sqlx,object>.Empty;
            }
            if (chartType!=BTree<Sqlx,object>.Empty)
            {
                var wd = 210;
                if (chartType.Contains(Sqlx.LEGEND))
                    wd = 310;
                sbuild.Append("<canvas id=\"myCanvas\" width=\""+wd+"\" height=\"210\" style=\"border:1px solid #c3c3c3;\">\r\n");
                sbuild.Append("Your browser does not support the canvas element.</canvas>\r\n"); 
                sbuild.Append("<script type=\"text/javascript\">\r\n");
                sbuild.Append("var canvas = document.getElementById(\"myCanvas\");\r\n");
                sbuild.Append("var ctx = canvas.getContext('2d');\r\n"); 
                sbuild.Append("var chartType = \"" + Level2.PMetadata.Flags(chartType) + "\";\r\n");
                sbuild.Append("var xdesc = \"" + xdesc + "\";\r\n");
                sbuild.Append("var ydesc = \"" + ydesc + "\";\r\n");
                sbuild.Append("var data = [ \r\n");
            }
            else
            {
                sbuild.Append("<table border><tr>");
                for (var b=rs.rt.First();b!=null;b=b.Next())
                {
                    var ci = (ObInfo)cx.db.role.infos[b.value()];
                    sbuild.Append("<th>" + ci.name + "</th>");
                }
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
            var oi = _cx.Inf(e._rowsetpos);
            if (chartType!=BTree<Sqlx,object>.Empty)
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
                for (var b=oi.domain.rowType.First();b!=null;b=b.Next())
                {
                    var s = GetVal(e[b.value()]);
                    sbuild.Append("<td>" + s + "</td>");
                }
                sbuild.Append("</tr>");
            }
        }
        public override void Footer()
        {
            if (chartType!=BTree<Sqlx,object>.Empty)
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
                if (chartType.Contains(Sqlx.HISTOGRAM))
                {
                    sbuild.Append("    minY=0; \r\n");
                }
                sbuild.Append("    var yd = axisd(minY, maxY);\r\n");
                sbuild.Append("    minY = yd*Math.round(minY*1.0/yd-0.5);\r\n");
                sbuild.Append("    maxY = yd*Math.round(maxY*1.0/yd+1.5);\r\n");
                sbuild.Append("    var wid = canvas.width - 40;\r\n");
                sbuild.Append("    var hig = canvas.height - 30;\r\n"); 
                if (chartType!=BTree<Sqlx,object>.Empty)
                {
                    sbuild.Append("    var scx = (maxX == minX) ? 1 : wid / (maxX - minX);\r\n");
                    sbuild.Append("    var scy = (maxY == minY) ? 1 : hig / (maxY - minY);\r\n");
                    sbuild.Append("    var colours = new Array();\r\n");
                }
                if (chartType.Contains(Sqlx.HISTOGRAM))
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
                } else if (chartType.Contains(Sqlx.LINE))
                {
                        sbuild.Append("    drawAxes();drawYmarks(); drawXmarks(); drawLineGraph();\r\n");
                        sbuild.Append("    function drawLineGraph() {\r\n");
                        sbuild.Append("      // now plot the data points\r\n");
                        sbuild.Append("      pt = data[0];\r\n");
                        sbuild.Append("      ctx.beginPath();\r\n");
                        sbuild.Append("      ctx.moveTo(trX(pt[0]), trY(pt[1]));\r\n");
                        if (chartType.Contains(Sqlx.LEGEND))
                        {
                            sbuild.Append("      if (pt.length > 2)\r\n");
                            sbuild.Append("        ctx.fillText(pt[2], trX(pt[0]), trY(pt[1]));\r\n");
                        }
                        sbuild.Append("      for (i = 1; i < data.length; i++) {\r\n");
                        sbuild.Append("        pt = data[i];\r\n");
                        sbuild.Append("        ctx.lineTo(trX(pt[0]), trY(pt[1]));\r\n");
                        if (chartType.Contains(Sqlx.LEGEND))
                        {
                            sbuild.Append("        if (pt.length > 2)\r\n");
                            sbuild.Append("            ctx.fillText(pt[2], trX(pt[0]), trY(pt[1]));\r\n");
                        }
                        sbuild.Append("       }\r\n");
                        sbuild.Append("     ctx.lineWidth = 1.5;\r\n");
                        sbuild.Append("     ctx.stroke();\r\n");
                        sbuild.Append("    }\r\n");
                } else if (chartType.Contains(Sqlx.POINTS))
                {
                        sbuild.Append("    drawAxes(); drawYmarks(); drawXmarks(); drawPoints();\r\n");
                        sbuild.Append("    function drawPoints() {\r\n");
                        sbuild.Append("       for (i=0;i<data.length;i++) {\r\n");
                        sbuild.Append("         pt = data[i];\r\n");
                        sbuild.Append("         ctx.fillStyle=\"red\";\r\n");
                        sbuild.Append("         ctx.fillRect(trX(pt[0])-1,trY(pt[1])-1,3,3);\r\n");
                        if (chartType.Contains(Sqlx.LEGEND))
                        {
                            sbuild.Append("         ctx.fillStyle=\"black\";\r\n");
                            sbuild.Append("         ctx.fillText(pt[2],trX(pt[0])-1,trY(pt[1])-8);\r\n");
                        }
                        sbuild.Append("       }\r\n");
                        sbuild.Append("    }\r\n");
                } else if(chartType.Contains(Sqlx.PIE))
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
                        if (chartType.Contains(Sqlx.LEGEND))
                        {
                            sbuild.Append("         ctx.fillStyle = \"black\";\r\n");
                            sbuild.Append("         ctx.fillText(pt[2],80+50*Math.cos(m),100+50*Math.sin(m));\r\n");
                        }
                        sbuild.Append("         ang = nang;\r\n");
                        sbuild.Append("        }\r\n");
                        sbuild.Append("      }\r\n");
                }
                if(chartType.Contains(Sqlx.LEGEND))
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
                if (chartType!=BTree<Sqlx,object>.Empty && !chartType.Contains(Sqlx.PIE))
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
                if(chartType.Contains(Sqlx.POINTS)||chartType.Contains(Sqlx.LINE))
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
        string cm = "";
        public JsonWebOutput(Transaction db, StringBuilder s) : base(db, s)
        { }
        public override void Header(HttpListenerResponse rs, Transaction tr, Context cx, string rdc)
        {
            rs.AddHeader("Content-Type", "application/json");
            base.Header(rs, tr, cx, rdc);
        }
        public override void BeforeResults()
        {
            sbuild.Append("[");
            cm = "";
        }
        public override void AfterResults()
        {
            sbuild.Append("]");
        }
        public override void PutRow(Context _cx, Cursor e)
        {
            sbuild.Append(cm); cm = ",";
            var rt = e.columns;
            var doc = new TDocument();
            for (var b = rt.First(); b != null; b = b.Next())
            {
                var ci = (SqlValue)_cx.obs[b.value()];
                if (e[ci.defpos] is TypedValue tv)
                    doc=doc.Add(ci.name, tv);
            }
            if (e[DBObject.Classification] is TLevel lv)
                doc=doc.Add("$classification", lv.ToString());
            if (e[Domain.Provenance] is TChar pv)
                doc=doc.Add("$provenance", pv.ToString());
            doc=doc.Add("$pos", new TInt(e._defpos));
            doc=doc.Add("$check", new TInt(e._ppos));
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
        public override void Header(HttpListenerResponse rs, Transaction tr, Context cx, string rdc)
        {
            rs.AddHeader("Content-Type", "application/xml");
            base.Header(rs, tr, cx, rdc);
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
                if (path.Length > 2)
                    role = pathbits[2];
                var h = client.Request.Headers["Authorization"];
                var s = Encoding.UTF8.GetString(Convert.FromBase64String(h.Substring(6))).Split(':');
                var details = BTree<string, string>.Empty;
                details+=("User", s[0]);
                details+=("Password", s[1]);
                details += ("Files", dbn.ident);
                details += ("Role", role);
                var acc = client.Request.Headers["Accept"];
                var d = Database.Get(details);
                var db = d.Transact(Transaction.Analysing,"");
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
                cx.result = cx.db.lexeroffset;
                db.Execute(cx,client.Request.HttpMethod,"H",pathbits, client.Request.Headers["Content-Type"],
                    sb, et);
                woutput.SendResults(client.Response,db,cx,"");
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

