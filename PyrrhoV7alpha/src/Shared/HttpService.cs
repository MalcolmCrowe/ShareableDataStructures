using System.Text;
using System.Net;
using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Level5;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
// NB: this file contains some code derived from W3Schools (c) 1999-2023 Refsnes Data under fair use
// (As an alternative, use windows.alert line commented out below and change
// TNode.Summary() in Graph.cs to use \\n instead of <br/>)
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
 
namespace Pyrrho
{
    /// <summary>
    /// Provides a simple HTTP1.1-like interface to Pyrrho DBMS
    /// compatible with web browsers:
    /// </summary>
    /// <remarks>
    /// constructor: set up the host and port for the HttpService
    /// </remarks>
    /// <param name="sv">the host name (default "127.0.0.1")</param>
    /// <param name="p">the port (default 8080)</param>
    internal class HttpService(string h, int p, int s)
    {
        /// <summary>
        /// the listener (usually port 8133 and 8180)
        /// </summary>
		HttpListener? listener;
        /// <summary>
        /// the host name
        /// </summary>
        readonly string host = h;
        /// <summary>
        /// the port for HTTP
        /// </summary>
		readonly int port = p;
        /// <summary>
        /// The port for HTTPS
        /// </summary>
        readonly int sport = s;

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
                listener.Realm = "PyrrhoDB";
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
    /// <remarks>
    /// Constructor for the web oputput class
    /// </remarks>
    /// <param name="s">the output stream</param>
    internal abstract class PyrrhoWebOutput(Transaction d, StringBuilder s)
    {
        public Transaction db = d;
        public StringBuilder sbuild = s;
        /// <summary>
        /// A Filter property
        /// </summary>
        public virtual string Filter
        {
            get { return ""; }
            set { }
        }

        /// <summary>
        /// Send results to the client using the PyrrhoWebOutput mechanisms
        /// </summary>
        public virtual ETag SendResults(HttpListenerResponse rs,Transaction tr,Context cx,
            string url,bool etags)
        {
            var r = (RowSet?)cx.result;
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
            var r = (RowSet?)cx.result;
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
    /// <remarks>
    /// simple constructor
    /// </remarks>
    /// <param name="s"></param>
    internal class SqlWebOutput(Transaction d, StringBuilder s) : PyrrhoWebOutput(d,s)
    {
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
            if (cx.result is RowSet r)
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
    /// <remarks>
    /// simple constructor
    /// </remarks>
    /// <param name="s"></param>
    internal class HtmlWebOutput(Transaction d, StringBuilder s, string qry) : PyrrhoWebOutput(d, s)
    {
        TMetadata chartType = TMetadata.Empty;
        long xcol= 0;
        long ycol = 0;
        long ccol = 0;
        string xdesc = "";
        string ydesc = "";
        string comma = "";
        readonly string query = qry;

        public override void Header(HttpListenerResponse hrs, Transaction tr,
            Context cx,string dn,string etags)
        {
            base.Header(hrs, tr, cx, dn,"");
            sbuild.Append("<!DOCTYPE HTML>\r\n");
            sbuild.Append("<html>\r\n");
            sbuild.Append("<body>\r\n");
            cx.versioned = true;
            var rs = (RowSet?)cx.result;
            if (rs == null)
                return;
            var fm = rs as TableRowSet ?? cx.obs[rs.source] as TableRowSet;
            var om = tr.objects[fm?.target??-1L] as DBObject;
            var psr = new Parser(cx, query);
            chartType = psr.ParseMetadata(Qlx.TABLE).Item2;
            var mi = om?.infos[tr.role.defpos];
            if (mi is not null && om is not null && om.defpos > 0)
            {
                chartType += mi.metadata;
                if (mi.description != "" && mi.description[0] == '<')
                    sbuild.Append(mi.description);
            }
            var oi = fm?.rowType;
            if (chartType != TMetadata.Empty && !chartType.Contains(Qlx.NODE))
            {
                for (var co = oi?.First(); co != null; co = co.Next())
                    if (co.value() is long p)
                    {
                        var cp = (cx.obs[p] is QlInstance sc) ? sc.sPos : p;
                        var ci = cx._Ob(cp)?.infos[cx.role.defpos];
                        if ((chartType[Qlx.X] is TChar xc && xc.value == ci?.name)
                            || ci?.metadata.Contains(Qlx.X) == true)
                        {
                            xcol = p;
                            xdesc = ci.description;
                        }
                        if ((chartType[Qlx.Y] is TChar yc && yc.value == ci?.name)
                            || ci?.metadata.Contains(Qlx.Y) == true)
                        {
                            ycol = p;
                            ydesc = ci.description;
                        }
                        if (chartType.Contains(Qlx.CAPTION))
                            ccol = cp;
                    }
                if ((xcol == 0) && (ycol == 0))
                    chartType = TMetadata.Empty;
            }
            if (chartType != TMetadata.Empty)
            {
                var wd = 210;
                if (chartType.Contains(Qlx.LEGEND))
                    wd = 310;
                var hd = 210;
                if (chartType.Contains(Qlx.NODE))
                {
                    wd = 2010; hd = 1810;
                    sbuild.Append("<canvas id=\"myCanvas\" width=\"" + wd + "\" height=\"" + hd +"\""
                        + " onclick=show(event)"
                        + " style=\"border:1px solid #c3c3c3;\">\r\n");
                } else
                    sbuild.Append("<canvas id=\"myCanvas\" width=\"" + wd + "\" height=\"" + hd
    + " \"style=\"border:1px solid #c3c3c3;\">\r\n");
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
                    if (fm is not null && b.value() is long p && cx._Ob(fm.sIMap[p] ?? -1L) is DBObject c &&
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
            if (chartType.Contains(Qlx.NODE) 
                && e is TableRowSet.TableCursor tc && tc._rec is TableRow tr
                && tc._table is NodeType nt)
            {
                var (li,ts) = NodeType.NodeTable(_cx, nt.Node(_cx, tr));
                var bl = CTree<int,NodeType>.Empty;
                for (var b = ts.First(); b != null; b = b.Next())
                    bl += (b.value(),b.key());
                for (var b=bl.First();b!=null;b=b.Next())
                { 
                    sbuild.Append(comma);
                    sbuild.Append('"');
                    sbuild.Append(b.value().name);
                    sbuild.Append('"');
                    comma = ",";
                }
                sbuild.Append("];\r\n var nodes=["); comma = "";
                for (var b=li.First();b!=null;b=b.Next())
                    if (b.value() is NodeType.NodeInfo ni)
                    {
                        sbuild.Append(comma + "[");
                        sbuild.Append(ni.ToString());
                        sbuild.Append(']');
                        comma = ",\r\n";
                    }
            } else
            if (chartType!=TMetadata.Empty)
            {
                sbuild.Append(comma+"[");
                var rc = e[xcol];
                var s = "";
                if (rc != null)
                {
                    s+= GetVal(rc);
                    if (rc.dataType.kind == Qlx.CHAR)
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
            if (chartType.Contains(Qlx.NODE))
            {
                GraphModelSupport();
                return;
            }
            if (chartType!=TMetadata.Empty)
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
                if (chartType.Contains(Qlx.HISTOGRAM))
                {
                    sbuild.Append("    minY=0; \r\n");
                }
                sbuild.Append("    var yd = axisd(minY, maxY);\r\n");
                sbuild.Append("    minY = yd*Math.round(minY*1.0/yd-0.5);\r\n");
                sbuild.Append("    maxY = yd*Math.round(maxY*1.0/yd+1.5);\r\n");
                sbuild.Append("    var wid = canvas.width - 40;\r\n");
                sbuild.Append("    var hig = canvas.height - 30;\r\n"); 
                if (chartType!=TMetadata.Empty)
                {
                    sbuild.Append("    var scx = (maxX == minX) ? 1 : wid / (maxX - minX);\r\n");
                    sbuild.Append("    var scy = (maxY == minY) ? 1 : hig / (maxY - minY);\r\n");
                    sbuild.Append("    var colours = new Array();\r\n");
                }
                if (chartType.Contains(Qlx.HISTOGRAM))
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
              //          if (chartType.Has(Qlx.LEGEND))
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
                } else if (chartType.Contains(Qlx.LINE))
                {
                        sbuild.Append("    drawAxes();drawYmarks(); drawXmarks(); drawLineGraph();\r\n");
                        sbuild.Append("    function drawLineGraph() {\r\n");
                        sbuild.Append("      // now plot the obs points\r\n");
                        sbuild.Append("      pt = obs[0];\r\n");
                        sbuild.Append("      ctx.beginPath();\r\n");
                        sbuild.Append("      ctx.moveTo(trX(pt[0]), trY(pt[1]));\r\n");
                        if (chartType.Contains(Qlx.LEGEND))
                        {
                            sbuild.Append("      if (pt.length > 2)\r\n");
                            sbuild.Append("        ctx.fillText(pt[2], trX(pt[0]), trY(pt[1]));\r\n");
                        }
                        sbuild.Append("      for (i = 1; i < obs.length; i++) {\r\n");
                        sbuild.Append("        pt = obs[i];\r\n");
                        sbuild.Append("        ctx.lineTo(trX(pt[0]), trY(pt[1]));\r\n");
                        if (chartType.Contains(Qlx.LEGEND))
                        {
                            sbuild.Append("        if (pt.length > 2)\r\n");
                            sbuild.Append("            ctx.fillText(pt[2], trX(pt[0]), trY(pt[1]));\r\n");
                        }
                        sbuild.Append("       }\r\n");
                        sbuild.Append("     ctx.lineWidth = 1.5;\r\n");
                        sbuild.Append("     ctx.stroke();\r\n");
                        sbuild.Append("    }\r\n");
                } else if (chartType.Contains(Qlx.POINTS))
                {
                        sbuild.Append("    drawAxes(); drawYmarks(); drawXmarks(); drawPoints();\r\n");
                        sbuild.Append("    function drawPoints() {\r\n");
                        sbuild.Append("       for (i=0;i<obs.length;i++) {\r\n");
                        sbuild.Append("         pt = obs[i];\r\n");
                        sbuild.Append("         ctx.fillStyle=\"red\";\r\n");
                        sbuild.Append("         ctx.fillRect(trX(pt[0])-1,trY(pt[1])-1,3,3);\r\n");
                        if (chartType.Contains(Qlx.LEGEND))
                        {
                            sbuild.Append("         ctx.fillStyle=\"black\";\r\n");
                            sbuild.Append("         ctx.fillText(pt[2],trX(pt[0])-1,trY(pt[1])-8);\r\n");
                        }
                        sbuild.Append("       }\r\n");
                        sbuild.Append("    }\r\n");
                } else if(chartType.Contains(Qlx.PIE))
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
                        if (chartType.Contains(Qlx.LEGEND))
                        {
                            sbuild.Append("         ctx.fillStyle = \"black\";\r\n");
                            sbuild.Append("         ctx.fillText(pt[2],80+50*Math.cos(m),100+50*Math.sin(m));\r\n");
                        }
                        sbuild.Append("         ang = nang;\r\n");
                        sbuild.Append("        }\r\n");
                        sbuild.Append("      }\r\n");
                }
                if(chartType.Contains(Qlx.LEGEND))
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
                if (chartType!=TMetadata.Empty && !chartType.Contains(Qlx.PIE))
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
                if(chartType.Contains(Qlx.POINTS)||chartType.Contains(Qlx.LINE))
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
        void GraphModelSupport()
        {
            sbuild.Append("];\r\n");
            sbuild.Append("    function show(event){\r\n");
            sbuild.Append("        ex = event.clientX+window.pageXOffset+minX;\r\n");
            sbuild.Append("        ey = event.clientY+window.pageYOffset+minY;\r\n");
            sbuild.Append("      for(i=0;i<nodes.length;i++){\r\n");
            sbuild.Append("        nd = nodes[i]; dx=ex-nd[2]; dy=ey-nd[3];\r\n");
            //       sbuild.Append("        if (dx*dx+dy*dy<400) { window.alert(nd[6]);break;}}};\r\n");
            sbuild.Append("        if (dx*dx+dy*dy<400) {\r\n");
            sbuild.Append("             var mbox = document.getElementById(\"modalBox\");\r\n");
            sbuild.Append("             var mtext = document.getElementById(\"modalText\");\r\n");
            sbuild.Append("             mtext.innerHTML = nd[6];\r\n");
            sbuild.Append("             mbox.style.display=\"block\";\r\n");
            sbuild.Append("             break;}}};\r\n");
            sbuild.Append("    maxX=0.0;maxY=0.0;minX=0.0;minY=0.0;\r\n");
            sbuild.Append("    for(i=0;i<nodes.length;i++){\r\n");
            sbuild.Append("      nd = nodes[i];\r\n");
            sbuild.Append("      if (nd[2]<minX) minX=nd[2];\r\n");
            sbuild.Append("      if (nd[2]>maxX) maxX=nd[2];\r\n");
            sbuild.Append("      if (nd[3]<minY) minY=nd[3];\r\n");
            sbuild.Append("      if (nd[3]>maxY) maxY=nd[3];\r\n"); 
            sbuild.Append("    }\r\n");
            sbuild.Append("    minX-=50;minY-=50;\r\n");
            sbuild.Append("    var colours = new Array();\r\n");
            sbuild.Append("    function abs(x) { return (x>0)?x:-x; }\r\n");
            sbuild.Append("    function pos(x) { return (x>0)?x:0; }\r\n");
            sbuild.Append("    function blend(x,a) { return Math.round(255*(pos(1-abs(x+3-a))+\r\n");
            sbuild.Append("           pos(1-abs(x-a))+pos(1-abs(x-3-a)))); } \r\n");
            sbuild.Append("    function colour(x) { return \"rgb(\"+blend(x,0.5)+\",\"+\r\n");
            sbuild.Append("        blend(x,1.5)+\",\"+blend(x,2.5)+\")\"; }\r\n");
            sbuild.Append("    function pickColours(n) {\r\n");
            sbuild.Append("        for(i=0;i<n;i++) colours[i]=colour(i*3.0/(n+1));\r\n");
            sbuild.Append("     }\r\n");
            sbuild.Append("    pickColours(obs.length);ctx.fillStyle=\"black\";\r\n");
            sbuild.Append("    const edges = [];");
            sbuild.Append("    for(i=0;i<nodes.length;i++){\r\n");
            sbuild.Append("      nd = nodes[i];\r\n");
            sbuild.Append("      if (nd[4]>=0) {\r\n");
            sbuild.Append("        lv = nodes[nd[4]]; ar = nodes[nd[5]];\r\n");
            sbuild.Append("        ctx.beginPath();\r\n");
            sbuild.Append("        ctx.moveTo(lv[2]-minX,lv[3]-minY); ctx.lineTo(nd[2]-minX,nd[3]-minY);\r\n");
            sbuild.Append("        ctx.lineTo(ar[2]-minX,ar[3]-minY); ctx.stroke();\r\n");
            sbuild.Append("        dx = ar[2]-nd[2]; dy=ar[3]-nd[3];\r\n");
            sbuild.Append("        d = Math.sqrt(dx*dx+dy*dy); // draw arrow head\r\n");
            sbuild.Append("        ctx.beginPath(); ctx.moveTo(ar[2]-20*dx/d-minX,ar[3]-20*dy/d-minY);\r\n");
            sbuild.Append("        ctx.lineTo(ar[2]-(25*dx-5*dy)/d-minX,ar[3]-(25*dy+5*dx)/d-minY);\r\n");
            sbuild.Append("        ctx.lineTo(ar[2]-(25*dx+5*dy)/d-minX,ar[3]-(25*dy-5*dx)/d-minY);\r\n");
            sbuild.Append("        ctx.closePath();ctx.fill();\r\n");
            sbuild.Append("        edges[i]=[lv[2]-minX,lv[3]-minY,ar[2]-minX,ar[3]-minY];\r\n");
            sbuild.Append("      } else edges[i]=0;\r\n");
            sbuild.Append("    }\r\n");
            sbuild.Append("    nd = nodes[0]; nX=nd[2]-minX; nY=nd[3]-minY;\r\n");
            sbuild.Append("    ctx.beginPath(); \r\n");
            sbuild.Append("    ctx.moveTo(nX-22,nY-22); ctx.lineTo(nX+22,nY-22); ctx.stroke();\r\n");
            sbuild.Append("    ctx.lineTo(nX+22,nY+22); ctx.stroke(); ctx.lineTo(nX-22,nY+22); ctx.stroke();\r\n");
            sbuild.Append("    ctx.lineTo(nX-22,nY-22); ctx.stroke();\r\n"); 
            sbuild.Append("    for(i=0;i<nodes.length;i++){\r\n");
            sbuild.Append("      nd = nodes[i];\r\n");
            sbuild.Append("      ctx.beginPath(); \r\n");
            sbuild.Append("      ctx.arc(nd[2]-minX,nd[3]-minY,20,0,2*Math.PI,false);\r\n");
            sbuild.Append("      ctx.closePath();\r\n");
            sbuild.Append("      ctx.fillStyle=colours[nd[0]]; ctx.fill();\r\n");
            sbuild.Append("      ctx.font=\"14px sans-serif\";\r\n");
            sbuild.Append("      m = ctx.measureText(nd[1].toString());\r\n");
            sbuild.Append("      ctx.fillStyle=\"black\";\r\n");
            sbuild.Append("      ctx.fillText(nd[1].toString(),nd[2]-minX-m.width/2,nd[3]-minY);\r\n");
            sbuild.Append("    }\r\n");
            sbuild.Append("    drawLegend();\r\n");
            sbuild.Append("    function drawLegend() {\r\n");
            sbuild.Append("      for(i=0;i<obs.length;i++) {\r\n");
            sbuild.Append("         pt = obs[i];\r\n");
            sbuild.Append("         ctx.fillStyle=colours[i];\r\n");
            sbuild.Append("         ctx.fillRect(20,15+13*i,5,5);\r\n");
            sbuild.Append("         ctx.fillStyle=\"black\";\r\n");
            sbuild.Append("         ctx.fillText(pt,30,20+13*i);\r\n");
            sbuild.Append("        }\r\n");
            sbuild.Append("    }\r\n");
            sbuild.Append("    function hideModal() {document.getElementById(\"modalBox\").style.display=\"none\"; }\r\n");
            sbuild.Append(" </script>");
            sbuild.Append("<!--  The following code is almost entirely pasted from w3schools.com \r\n");
            sbuild.Append("    and is (c)1999-2023 Refsnes Data. Its use in Pyrrho is covered by Fair Use\r\n");
            sbuild.Append("    since Pyrrho is a free-to-use-and-copy research project\r\n");
            sbuild.Append("    If you copy code from here you should ensure that your use is also fair -->\r\n");
            sbuild.Append("<style>\r\nbody {font-family: Arial, Helvetica, sans-serif;}\r\n\r\n");
            sbuild.Append("/* The Modal (background) */\r\n");
            sbuild.Append(".modal {\r\n  display: none; /* Hidden by default */\r\n");
            sbuild.Append("  position: fixed; /* Stay in place */\r\n"); 
            sbuild.Append("  z-index: 1; /* Sit on top */\r\n");
            sbuild.Append("  padding-top: 100px; /* Location of the box */\r\n");
            sbuild.Append("  left: 25%;\r\n  top: 25%;\r\n");
            sbuild.Append("  width: 50%;\r\n");
            sbuild.Append("  height: 50%; \r\n");
            sbuild.Append("  overflow: auto; /* Enable scroll if needed */\r\n");
            sbuild.Append("  background-color: rgb(0,0,0); /* Fallback color */\r\n");
            sbuild.Append("  background-color: rgb(255,255,255); /* White */\r\n");
            sbuild.Append("}\r\n\r\n");
            sbuild.Append("/* Modal Content */\r\n");
            sbuild.Append("modal-content {\r\n");
            sbuild.Append("  background-color: #fefefe;\r\n");
            sbuild.Append("  margin: auto;\r\n");
            sbuild.Append("  padding: 20px;\r\n");
            sbuild.Append("  border: 1px solid #888;\r\n");
            sbuild.Append("  width: 80%;\r\n}\r\n\r\n");
            sbuild.Append("/* The Close Button */\r\n");
            sbuild.Append(".close {\r\n  color: #aaaaaa;\r\n");
            sbuild.Append("  float: right;\r\n");
            sbuild.Append("  font-size: 28px;\r\n");
            sbuild.Append("  font-weight: bold;\r\n");
            sbuild.Append("}\r\n");
            sbuild.Append(".close:hover,\r\n");
            sbuild.Append(".close:focus {\r\n");
            sbuild.Append("  color: #000;\r\n");
            sbuild.Append("  text-decoration: none;\r\n");
            sbuild.Append("  cursor: pointer;\r\n");
            sbuild.Append("}\r\n");
            sbuild.Append("</style>\r\n");
            sbuild.Append("<!-- The Modal -->\r\n");
            sbuild.Append("<div id=\"modalBox\" class=\"modal\">\r\n\r\n");
            sbuild.Append("  <!-- Modal content -->\r\n");
            sbuild.Append("  <div class=\"modal-content\">\r\n");
            sbuild.Append("    <span class=\"close\" onclick=\"hideModal()\">[hide]</span>\r\n");
            sbuild.Append("    <div id=\"modalText\">\r\n");
            sbuild.Append("<p>Some text in the Modal..</p>\r\n");
            sbuild.Append("    </div>\r\n");
            sbuild.Append("  </div>\r\n\r\n");
            sbuild.Append("</div>");
            sbuild.Append("</body></html>\r\n"); 
        }
    }
    internal class JsonWebOutput(Transaction db, StringBuilder s) : PyrrhoWebOutput(db, s)
    {
        string cm = "";

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
                if (b.value() is long p && cx.obs[p] is QlValue ci && e[ci.defpos] is TypedValue tv)
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
    /// <remarks>
    /// a simple constructor
    /// </remarks>
    /// <param name="s"></param>
    internal class XmlWebOutput(Transaction db, StringBuilder s, string rn) : PyrrhoWebOutput(db,s)
    {
        public string rootName = rn;

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
    /// <remarks>
    /// constructor
    /// </remarks>
    internal class HttpServer(HttpListenerContext h)
    {
        /// <summary>
        /// The HttpContext
        /// </summary>
        protected HttpListenerContext client = h;
//        readonly string agent;
        protected PyrrhoWebOutput? woutput;
        readonly StringBuilder sbuild = new();
        /// <summary>
        /// the database path
        /// </summary>
        readonly string path = h.Request.RawUrl ?? "";

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
                var dbn = new Ident(pathbits[1], -1L);
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
                        rv += Rvv.Parse(ets[i].Trim());
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

