using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Web;
using System.IO;
using System.Threading;

namespace Pyrrho
{
    /// <summary>
    /// By default this is a single-threaded web server. 
    /// The Server method runs the service for the given set of 
    /// url prefixes. Override Get, Put, Post, Delete to customise.
    /// 
    /// To use in multithreaded mode, override the Factory method
    /// to create an instance of your own subclass of WebSvc,
    /// and create your method overrides in that instance instead.
    /// </summary>
    public class WebSvr : WebSvc
    {
        protected WebSvr()
        {
            CheckForLoginPage("Login.htm");
            CheckForLoginPage("Login.html");
            CheckForLogFile();
        }
        public virtual WebSvc Factory()
        {
            return this;
        }
        public void Server(params string[] prefixes)
        {
            Server(AuthenticationSchemes.Anonymous, prefixes);
        }
        public void Server(AuthenticationSchemes au, params string[] prefixes)
        {
            HttpListener listener = new HttpListener();
            foreach (var p in prefixes)
                listener.Prefixes.Add(p);
            listener.AuthenticationSchemes = au;
            listener.Start();
            for (; ; )
                try
                {
                    var ws = Factory();
                    ws._Open(listener);
                    if (ws == this)
                        Serve();
                    else
                        new Thread(new ThreadStart(ws.Serve)).Start();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    break;
                }
            listener.Stop();
        }
        void CheckForLoginPage(string v)
        {
            if (File.Exists("Pages/" + v))
                loginPage = v;
        }
        void CheckForLogFile()
        {
            if (File.Exists("Log.txt"))
                logging=true;
        }
    }
}
