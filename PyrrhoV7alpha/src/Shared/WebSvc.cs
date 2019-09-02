using System;
using System.IO;
using System.Text;
using System.Net;
using System.Web;
using System.Collections.Generic;
using System.Reflection;

namespace Pyrrho
{
    /// <summary>
    /// supported URLs: {http/s}://{host:port}/{AppName}/{ControllerName}[/{data}]
    /// {data} can be a document starting with {
    /// otherwise is a set of strings separated with /
    /// </summary>
    public class WebSvc
    {
        public static string loginPage = "";
        public static bool logging = false;
        public HttpListenerContext context;
        protected static Dictionary<string, WebCtlr> controllers = new Dictionary<string, WebCtlr>();
        WebCtlr controller = null;
        string controllerName = "Home";
        Document param = null;
        object[] data = null;
        protected static void Add(WebCtlr wc)
        {
            var n = wc.GetType().Name;
            if (n.EndsWith("Controller"))
                n = n.Substring(0,n.Length - 10);
            controllers.Add(n, wc);
        }
        protected WebSvc() { }
        public void _Open(HttpListener hc)
        {
            context = hc.GetContext();
            data = null;
            var u = context.Request.Url.Segments;
            if (u.Length >= 3)
                controllerName = u[2].Trim('/');      
            if (controllers.ContainsKey(controllerName))
                controller = controllers[controllerName];
            if (u.Length >= 4)
            {
                var n = u[3];
                n = HttpUtility.UrlDecode(n);
                if (n[0] == '{')
                    param = new Document(n.Trim('/'));
                else
                {
                    data = new object[u.Length - 2];
                    data[0] = this;
                    data[1] = n.Trim('/');
                    for (var i = 2; i < data.Length; i++)
                        data[i] = HttpUtility.UrlDecode(u[i + 2].Trim('/'));
                }
            }
            Open(context);
        }
        public virtual void Open(HttpListenerContext cx)
        {
        }
        public void Serve()
        {
            try
            {
                var s = Serve(context.Request.HttpMethod, context.Request.Url);
                Send(s);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            Close();
        }
        string Serve(string m, Uri u)
        {
            var mc = "";
            var ret = "OK";
            var postData = "";
            switch (m)
            {
                case "GET":
                    var n = u.Segments.Length;
                    if (n >= 3)
                    {
                        var v = u.Segments[2].Trim('/');
                        if (v.EndsWith(".js"))
                            return new StreamReader("Scripts/" + v).ReadToEnd();
                        if (v.EndsWith(".htm") || v.EndsWith(".html"))
                        {
                            if (loginPage != "" && !Authenticated())
                                v = loginPage;
                            return new StreamReader("Pages/" + v).ReadToEnd();
                        }
                        if (v.EndsWith(".css"))
                            return new StreamReader("Styles/" + v).ReadToEnd();
                    }
                    mc = "Get";
                    break;
                case "PUT": mc = "Put";
                    postData = GetData();
                    if (postData!=null)
                        param = new Document(postData);
                    break;
                case "POST": mc = "Post";
                    postData = GetData();
                    if (postData!=null)
                        param = new Document(postData);
                    break;
                case "DELETE": mc = "Delete"; break;
                default:
                    ret = "Unsupported method " + m; break;
            }
            if (controller == null)
            {
                context.Response.StatusCode = 404;
                context.Response.StatusDescription = "NOT FOUND";
                return "No controller for " + controllerName;
            }
            if (!Authenticated())
            {
                context.Response.StatusCode = 401;
                context.Response.StatusDescription = "UNAUTHORISED";
                return "No authentication for " + controllerName;
            } 
            var mth = controller.GetType().GetMethod(mc + controllerName);
            if (mth == null)
            {
                context.Response.StatusCode = 400;
                context.Response.StatusDescription = "BAD REQUEST";
                return "No " + mc + " method for " + controllerName;
            }
            else
                try
                {
                    if (data == null)
                        ret = mth.Invoke(null, new object[] { this, param }).ToString();
                    else
                    {
                        FixParams(mth, data);
                        ret = mth.Invoke(null, data).ToString();
                    }
                }
                catch (Exception e)
                {
                    context.Response.StatusCode = 403;
                    context.Response.StatusDescription = "ERROR";
                    ret = e.InnerException.Message;
                }
            Log(m, u, postData);
            return ret;
        }
        /// <summary>
        /// data[1..] are strings but should match parameter types of mth
        /// </summary>
        /// <param name="mth"></param>
        /// <param name="data"></param>
        private void FixParams(MethodInfo mth, object[] data)
        {
            var ps = mth.GetParameters();
            for(var i=1;i<ps.Length;i++)
            {
                var pt = ps[i].ParameterType.GetMethod("Parse");
                if (pt != null)
                    data[i] = pt.Invoke(null, new object[]{data[i].ToString()});
            }
        }
        public virtual void Close() { }
        internal void Send(string s)
        {
            var b = Encoding.UTF8.GetBytes(s);
            var c = context.Response;
            c.AddHeader("Cache-control", "no-store");
            c.AddHeader("Expires", "-1");
            c.AddHeader("Pragma", "no-cache");
            c.ContentLength64 = b.Length;
            var st = c.OutputStream;
            st.Write(b, 0, b.Length);
            st.Close();
        }
        public string GetData()
        {
            var r = context.Request;
            if (!r.HasEntityBody)
                return null;
            var rdr = new StreamReader(r.InputStream, r.ContentEncoding);
            var rs = rdr.ReadToEnd();
            r.InputStream.Close();
            rdr.Close();
            return rs;
        }
        public virtual bool Authenticated()
        {
            if (loginPage == "")
                return true; 
            return (controller!=null)?controller.AllowAnonymous():false;
        }
        protected virtual void Log(string m,Uri u,string p)
        {
            if (!logging)
                return;
            var logFile = new StreamWriter("Log.txt", true);
            logFile.Write(DateTime.Now.ToString());
            if (context.User != null)
            {
                logFile.Write(" (");
                logFile.Write(context.User.Identity.Name);
                logFile.Write(")");
            }
            logFile.Write(": ");
            logFile.Write(m);
            logFile.Write(" ");
            logFile.Write(u.ToString());
            if (p!="")
            {
                logFile.Write("|<");
                logFile.Write(p);
                logFile.Write(">|");
            }
            logFile.WriteLine();
            logFile.Close();
        }
    }
}
