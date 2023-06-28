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
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
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
		HttpListener? listener;
        /// <summary>
        /// the host name
        /// </summary>
        readonly string host;
        /// <summary>
        /// the port for HTTP
        /// </summary>
		readonly int port;
        /// <summary>
        /// The port for HTTPS
        /// </summary>
        readonly int sport;
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
        protected PyrrhoWebOutput(Transaction d,StringBuilder s)
        {
            sbuild = s;
            db = d;
        }
        /// <summary>
        /// Send results to the client using the PyrrhoWebOutput mechanisms
        /// </summary>
        public virtual ETag SendResults(HttpListenerResponse rs,Transaction tr,Context cx,
            string url,bool etags)
        {
            var r = (RowSet?)cx.obs[cx.result];
            Cursor? e = r?.First(cx);
            ETag et = ETag.Empty;
            if (cx.db is not null && etags)
            {
                if (cx.affected!=Rvv.Empty)
                    et = new ETag(cx.db, cx.affected);
                else if (e != null)
                {
                    for (; e != null; e = e.Next(cx))
                        et = e._Rvv(cx);
                    cx.funcs = BTree<long, BTree<TRow, BTree<long, Register>>>.Empty;
                    e = r?.First(cx);
                } 
            }
            Header(rs, tr, cx, url,et.assertMatch.ToString());
            if (r is not null)
            {
                BeforeResults();
                for (; e != null; e = e.Next(cx))
                    PutRow(cx, e);
                AfterResults();
            }
            Footer();
            return et;
        }
        /// <summary>
        /// Header for the page
        /// </summary>
        public virtual void Header(HttpListenerResponse rs, Transaction tr,
            Context cx, string url, string etags)
        {
            rs.StatusCode = 200;
            cx.versioned = true;
            var r = (RowSet?)cx.obs[cx.result];
            if (r == null && cx.exec is QuerySearch us)
                r = (RowSet?)cx.obs[us.source];
            if (r == null && cx.exec is SqlInsert si)
                r = (RowSet?)cx.obs[si.source];
            if (r != null && cx.db is not null && cx.db.role is not null
                && cx._Ob(r.target) is DBObject ob && ob.infos[cx.db.role.defpos] is ObInfo oi)
            {
                if (oi.description is string ds && ds != "")
                    rs.AddHeader("Description", ds);
                if (ob.classification != Level.D)
                    rs.AddHeader("Classification", ob.classification.ToString());
                if (cx.obs[r.target] is Table tb)
                    rs.AddHeader("LastData", tb.lastData.ToString());
            }
            rs.AddHeader("ETag", etags);
            if (PyrrhoStart.DebugMode || PyrrhoStart.HTTPFeedbackMode)
                Console.WriteLine("Returning ETag: " + etags);
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
        public override void Header(HttpListenerResponse rs, Transaction tr, Context cx, 
            string dn, string etags)
        {
            base.Header(rs, tr, cx, dn, etags);
        }
        public override void PutRow(Context cx, Cursor e)
        {
            var dt = e.dataType;
            var cm = "(";
            for (var b = dt.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sbuild.Append(cm);
                    sbuild.Append(e[p]);
                }
            sbuild.Append(')');
        }
        public override ETag SendResults(HttpListenerResponse rs, Transaction tr,Context cx, 
            string dn,bool etags)
        {
            var cm = "";
            Header(rs, tr,cx, dn,"");
            if (cx.obs[cx.result] is RowSet r)
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
            return ETag.Empty;
        }
    }
    /// <summary>
    /// HTML page output class
    /// </summary>
    internal class HtmlWebOutput : PyrrhoWebOutput
    {
        CTree<Sqlx,TypedValue> chartType = CTree<Sqlx,TypedValue>.Empty;
        long xcol= 0;
        long ycol = 0;
        long ccol = 0;
        string xdesc = "";
        string ydesc = "";
        string comma = "";
        readonly string query = "";
        /// <summary>
        /// simple constructor
        /// </summary>
        /// <param name="s"></param>
        public HtmlWebOutput(Transaction d, StringBuilder s, string qry)
            : base(d, s)
        { query = qry; }
        public override void Header(HttpListenerResponse hrs, Transaction tr,
            Context cx,string dn,string etags)
        {
            base.Header(hrs, tr, cx, dn,"");
            sbuild.Append("<!DOCTYPE HTML>\r\n");
            sbuild.Append("<html>\r\n");
            sbuild.Append("<body>\r\n");
            cx.versioned = true;
            var rs = (RowSet?)cx.obs[cx.result];
            if (rs == null)
                return;
            var fm = rs as TableRowSet ?? cx.obs[rs.source] as TableRowSet;
            var om = tr.objects[fm?.target??-1L] as DBObject;
            var psr = new Parser(cx, query);
            chartType = psr.ParseMetadata(Sqlx.TABLE);
            var mi = om?.infos[tr.role.defpos];
            if (mi is not null && om is not null && om.defpos > 0)
            {
                chartType += mi.metadata;
                if (mi.description != "" && mi.description[0] == '<')
                    sbuild.Append(mi.description);
            }
            var oi = fm?.rowType;
            if (chartType != CTree<Sqlx, TypedValue>.Empty)
            {
                for (var co = oi?.First(); co != null; co = co.Next())
                    if (co.value() is long p)
                    {
                        var cp = (cx.obs[p] is SqlCopy sc) ? sc.copyFrom : p;
                        var ci = cx._Ob(cp)?.infos[cx.role.defpos];
                        if ((chartType[Sqlx.X] is TChar xc && xc.value == ci?.name)
                            || ci?.metadata.Contains(Sqlx.X) == true)
                        {
                            xcol = p;
                            xdesc = ci.description;
                        }
                        if ((chartType[Sqlx.Y] is TChar yc && yc.value == ci?.name)
                            || ci?.metadata.Contains(Sqlx.Y) == true)
                        {
                            ycol = p;
                            ydesc = ci.description;
                        }
                        if (chartType.Contains(Sqlx.CAPTION))
                            ccol = cp;
                    }
                if ((xcol == 0) && (ycol == 0))
                    chartType = CTree<Sqlx, TypedValue>.Empty;
            }
            if (chartType!=CTree<Sqlx,TypedValue>.Empty)
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
                sbuild.Append("var obs = [ \r\n");
            }
            else
            {
                sbuild.Append("<table border><tr>");
                for (var b = rs.rowType.First(); b != null; b = b.Next())
                    if (fm is not null && b.value() is long p &&  cx._Ob(fm.sIMap[p]??-1L) is DBObject c &&
                        c.infos[cx.role.defpos] is ObInfo ci && ci.name != null)
                        sbuild.Append("<th>" + ci?.name ?? "" + "</th>");
                sbuild.Append("</tr>");
            }
        }
        public static string GetVal(TypedValue v)
        {
            return (v != TNull.Value) ? v.ToString() : "";
        }
        public override void PutRow(Context _cx, Cursor e)
        {
            var dt = e.dataType;
            if (chartType!=CTree<Sqlx,TypedValue>.Empty)
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
                sbuild.Append(']');
                comma = ",";
            }
            else
            {
                sbuild.Append("<tr>");
                for (var b = dt.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        var s = GetVal(e[p]);
                        sbuild.Append("<td>" + s + "</td>");
                    }
                sbuild.Append("</tr>");
            }
        }
        public override void Footer()
        {
            if (chartType!=CTree<Sqlx,TypedValue>.Empty)
            {
                sbuild.Append("     ];           var pt = obs[0];\r\n");
                sbuild.Append("    // first find obs window\r\n");
                sbuild.Append("    var minX = pt[0], maxX = pt[0];\r\n");
                sbuild.Append("    var minY = pt[1], maxY = pt[1];\r\n");
                sbuild.Append("    var sumY = pt[1];\r\n");
                sbuild.Append("    for (var i = 1; i < obs.length; i++) {\r\n");
                sbuild.Append("        pt = obs[i];\r\n");
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
                if (chartType!=CTree<Sqlx,TypedValue>.Empty)
                {
                    sbuild.Append("    var scx = (maxX == minX) ? 1 : wid / (maxX - minX);\r\n");
                    sbuild.Append("    var scy = (maxY == minY) ? 1 : hig / (maxY - minY);\r\n");
                    sbuild.Append("    var colours = new Array();\r\n");
                }
                if (chartType.Contains(Sqlx.HISTOGRAM))
                {
                        sbuild.Append("    pickColours(obs.length); drawAxes();\r\n");
                        sbuild.Append("    drawYmarks(); drawHistogram(); \r\n");
                        sbuild.Append("    function drawHistogram() {\r\n");
                        sbuild.Append("       w = 80.0/obs.length;\r\n");
                        sbuild.Append("       for(i=0;i<obs.length;i++) { \r\n");
                        sbuild.Append("         pt=obs[i];\r\n");
                        sbuild.Append("         ctx.fillStyle = colours[i];\r\n");
                        sbuild.Append("         ctx.fillRect(30+w/2+i*w+i*w,trY(pt[1]),w,170-trY(pt[1]));\r\n");
                        sbuild.Append("        }\r\n"); 
                        sbuild.Append("     }\r\n");
              //          if (chartType.Has(Sqlx.LEGEND))
              //          {
                            sbuild.Append("    drawXCaptions();\r\n");
                            sbuild.Append("    function drawXCaptions() {\r\n");
                            sbuild.Append("       for(i=0;i<obs.length;i++) {\r\n");
                            sbuild.Append("         pt = obs[i]; \r\n");
                            sbuild.Append("         ctx.fillStyle = \"black\";\r\n");
                            sbuild.Append("         ctx.fillText(pt[2],40+i*160/obs.length,180);\r\n");
                            sbuild.Append("       }\r\n");
                            sbuild.Append("    }\r\n");
               //         }
                } else if (chartType.Contains(Sqlx.LINE))
                {
                        sbuild.Append("    drawAxes();drawYmarks(); drawXmarks(); drawLineGraph();\r\n");
                        sbuild.Append("    function drawLineGraph() {\r\n");
                        sbuild.Append("      // now plot the obs points\r\n");
                        sbuild.Append("      pt = obs[0];\r\n");
                        sbuild.Append("      ctx.beginPath();\r\n");
                        sbuild.Append("      ctx.moveTo(trX(pt[0]), trY(pt[1]));\r\n");
                        if (chartType.Contains(Sqlx.LEGEND))
                        {
                            sbuild.Append("      if (pt.length > 2)\r\n");
                            sbuild.Append("        ctx.fillText(pt[2], trX(pt[0]), trY(pt[1]));\r\n");
                        }
                        sbuild.Append("      for (i = 1; i < obs.length; i++) {\r\n");
                        sbuild.Append("        pt = obs[i];\r\n");
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
                        sbuild.Append("       for (i=0;i<obs.length;i++) {\r\n");
                        sbuild.Append("         pt = obs[i];\r\n");
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
                        sbuild.Append("    pickColours(obs.length); drawPie(); \r\n");
                        sbuild.Append("    function drawPie() {\r\n");
                        sbuild.Append("      var ang = 0;\r\n");
                        sbuild.Append("      for(i=0;i<obs.length;i++) {\r\n");
                        sbuild.Append("         pt = obs[i];\r\n");
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
                    sbuild.Append("      for(i=0;i<obs.length;i++) {\r\n");
                    sbuild.Append("         pt = obs[i];\r\n");    
                    sbuild.Append("         ctx.fillStyle=colours[i];\r\n");
                    sbuild.Append("         ctx.fillRect(200,15+13*i,5,5);\r\n");
                    sbuild.Append("         ctx.fillStyle=\"black\";\r\n");
                    sbuild.Append("         ctx.fillText(pt[2],210,20+13*i);\r\n");
                    sbuild.Append("        }\r\n");
                    sbuild.Append("      }\r\n");               
                }
                if (chartType!=CTree<Sqlx,TypedValue>.Empty && !chartType.Contains(Sqlx.PIE))
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
                sbuild.Append("        for(i=0;i<n;i++) colours[i]=colour(i*3.0/(n+1));\r\n");
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
        public override void Header(HttpListenerResponse rs, Transaction tr, Context cx, 
            string dn,string etags)
        {
            cx.versioned = true;
            rs.ContentType= "application/json";
            base.Header(rs, tr, cx, dn,etags);
        }
        public override void BeforeResults()
        {
            sbuild.Append('[');
            cm = "";
        }
        public override void AfterResults()
        {
            sbuild.Append(']');
        }
        public override void PutRow(Context cx, Cursor e)
        {
            var rs = (RowSet?)cx.obs[e._rowsetpos];
            if (rs == null)
                return;
            var key = (cx.groupCols[rs.defpos] is Domain gc) ? new TRow(gc, e.values) : TRow.Empty;
            sbuild.Append(cm); cm = ",";
            var rt = e.columns;
            var doc = TDocument.Null;
            for (var b = rt.First(); b != null && b.key()<rs.display; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue ci && e[ci.defpos] is TypedValue tv)
                {
                    var n = ci.alias ?? ci.NameFor(cx);
                    if (n == "")
                        n = "Col" + b.key();
                    doc = doc.Add(n, tv);
                }
            if (e[DBObject.Classification] is TLevel lv)
                doc = doc.Add("$classification", lv.ToString());
            if (e._ds.First() is ABookmark<long, (long, long)> ab)
            {
                var (p, c) = ab.value();
                doc = doc.Add("$pos", new TInt(p));
                doc = doc.Add("$check", new TInt(c));
            }
            for (var b = cx.funcs[e._rowsetpos]?[key]?.First(); b != null; b = b.Next())
                doc = doc.Add("$" + DBObject.Uid(b.key()), new TDocument(b.value()));
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
        public override void Header(HttpListenerResponse rs, Transaction tr, Context cx, 
            string dn, string etags)
        {
            cx.versioned = true;
            base.Header(rs, tr, cx, dn, etags);
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
                    var ta = e._rs._tr.objects[fm.target];
                    var tb = (ta is EdgeType et) ? (Table)et : (ta is NodeType nt) ? (Table)nt: ta as Table;
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
//        readonly string agent;
        protected PyrrhoWebOutput? woutput;
        readonly StringBuilder sbuild;
        /// <summary>
        /// the database path
        /// </summary>
        readonly string path;

        /// <summary>
        /// constructor
        /// </summary>
		public HttpServer(HttpListenerContext h)
		{
            client = h;
            path = h.Request.RawUrl ?? "";
//            agent = h.Request.UserAgent ?? "";
            sbuild = new StringBuilder();
		}
         internal void Server()
        {
            woutput = null;
            try
            {
                if (PyrrhoStart.TutorialMode || PyrrhoStart.DebugMode || PyrrhoStart.HTTPFeedbackMode)
                    Console.WriteLine("HTTP " + client.Request.HttpMethod + " " + path);
                string[] pathbits = path.Split('?');
                string query = (pathbits.Length > 1) ? pathbits[1] : "";
                pathbits = pathbits[0].Split('/');
                if (path == "/")
                {
                    TransactionOperation();
                    return;
                }
                if (path.Length <= 2)
                    return;
                var dbn = new Ident(pathbits[1], Iix.None);
                if (dbn.ident.EndsWith("favicon.ico"))
                    return;
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
                if (pathbits.Length > 2)
                    role = pathbits[2];
                var details = BTree<string, string>.Empty;
                if (client.Request.Headers["Authorization"] is string h)
                {
                    var s = Encoding.UTF8.GetString(Convert.FromBase64String(h[6..])).Split(':');
                    details += ("User", s[0]);
                    details += ("Password", s[1]);
                }
                details += ("Files", dbn.ident);
                details += ("Role", role);
                var acc = client.Request.Headers["Accept"];
                var d = Database.Get(details)??throw new DBException("3D000", dbn.ident);
                var db = d.Transact(Transaction.Analysing, new Connection(details));
                if (d.lastModified != null)
                    db += (Database.LastModified, d.lastModified); // use the file time, not UTCNow
                if (acc != null && acc.Contains("text/plain"))
                    woutput = new SqlWebOutput(db, sbuild);
                else if (acc != null && acc.Contains("text/html"))
                    woutput = new HtmlWebOutput(db, sbuild, query);
                else if (acc != null && acc.Contains("xml"))
                    woutput = new XmlWebOutput(db, sbuild, "root");
                else
                    woutput = new JsonWebOutput(db, sbuild);
                byte[] bytes = new byte[client.Request.ContentLength64];
                if (bytes.Length > 0)
                    client.Request.InputStream.Read(bytes, 0, bytes.Length);
                var sb = Encoding.UTF8.GetString(bytes);
                var et = client.Request.Headers["If-Match"];
                var eu = client.Request.Headers["If-Unmodified-Since"];
                var rv = Rvv.Empty;
                if (et != null)
                {
                    if (et.StartsWith("W/"))
                        et = et[2..];
                    var ets = et.Split(';');
                    for (var i = 0; i < ets.Length; i++)
                        rv += Rvv.Parse(ets[i].Trim().Trim('"'));
                }
                if (PyrrhoStart.DebugMode || PyrrhoStart.HTTPFeedbackMode)
                {
                    if (sb != "")
                        Console.WriteLine(sb);
                    if (et != null)
                        Console.WriteLine("Received If-Match: " + et);
                    if (eu != null)
                        Console.WriteLine("Received If-Unmodified-Since: " + eu);
                }
                if (Rvv.Validate(d, et, eu))
                    rv = Rvv.Empty;
                else
                    throw new DBException("40084");
                var cx = new Context(db);
                if (cx.db != null)
                    db.Execute(cx, 0L, client.Request.HttpMethod, cx.db.name, pathbits, query,
                        client.Request.Headers["Content-Type"], sb);
                var ocx = cx;
                if (cx.db != null)
                    cx = new Context(cx.db.Commit(cx));
                woutput.SendResults(client.Response, db, ocx, db.name, true);
                return;
            }
            catch (DBException e)
            {
                switch (e.signal[0..2])
                {
                    case "22":
                    case "20": client.Response.StatusCode = 400; break;
                    case "23":
                        client.Response.StatusCode = 403;
                        if (woutput != null)
                        {
                            var bs = Encoding.UTF8.GetBytes(e.Message);
                            client.Response.ContentLength64 = bs.Length;
                            var wr = client.Response.OutputStream;
                            wr.Write(bs, 0, bs.Length);
                            wr.Close();
                        }
                        break;
                    case "42": client.Response.StatusCode = 401; break;
                    case "40": client.Response.StatusCode = 412; break;
                    case "3D": client.Response.StatusCode = 404; break;
                    default: client.Response.StatusCode = 400; break;
                }
                return;
            }
            catch (PEException)
            {
                client.Response.StatusCode = 500;
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
                        var bs = Encoding.UTF8.GetBytes(woutput.sbuild.ToString());
                        client.Response.ContentLength64 = bs.Length;
                        var wr = client.Response.OutputStream;
                        wr.Write(bs, 0, bs.Length);
                        wr.Close();
                    }
                    catch (Exception) { }
            }
            finally
            {
                Close();
            }
        }
        internal static void TransactionOperation()
        {
            // nothing - though it is a surprise if we get here
        }
        internal void Close()
        {
            if (sbuild != null && client.Response.StatusCode == 200)
                try
                {
                    var bs = Encoding.Default.GetBytes(sbuild.ToString());
                    client.Response.ContentLength64 = bs.Length;
                    client.Response.OutputStream.Write(bs, 0, bs.Length);
                }
                catch (Exception) { }
            client.Response.Close();
        }
	}
}

