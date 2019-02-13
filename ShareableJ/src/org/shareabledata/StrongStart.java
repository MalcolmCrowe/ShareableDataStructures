/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.net.*;
import java.io.FilePermission;
import java.security.AccessController;
/**
 *  The Client Listener for the StrongDBMS.
 *   The Main entry point is here
 * 
 * @author Malcolm
 */
public class StrongStart {
        static String host = "127.0.0.1";
        static int port = 50433;
        static ServerSocket tcp;
        /// <summary>
        /// The main service loop of the StrongDBMS is here
        /// </summary>
        static void Run() throws Exception
        {
            var ad = InetAddress.getByName(host);
            var i = 0;
            while (tcp == null && i++ < 100)
            {
                try
                {
                    tcp = new ServerSocket(port,50,ad);
                }
                catch (Exception e)
                {
                    port++;
                    tcp = null;
                }
            }
            if (tcp == null)
                throw new Exception("Cannot open a port on " + host);
            System.out.println("StrongDBMS protocol on " + host + ":" + port);
            if (StrongServer.path != "")
                System.out.println("Database folder " + StrongServer.path);
            int cid = 0;
            for (; ; )
                try
                {
                    Socket client = tcp.accept();
                    var t = new Thread(new StrongServer(client));
                    t.start();
                }
                catch (Exception e)
                { }
        }
        /// The main entry point for the application. Process arguments and create the main service loop
        public static void main(String[] args) throws Exception
        {
            SysTable.Init();
            for (String arg : args) {
                System.out.print(arg + " ");
            }
            System.out.print(" Enter to start up");
            System.in.read();
            for (int j = 0; j < Version.length; j++)
                if (j == 1 || j == 2)
                    System.out.print(Version[j]);
                else
                    System.out.println(Version[j]);
            int k = 0;
            while (args.length > k && args[k].charAt(0) == '-')
            {
                switch (args[k].charAt(1))
                {
                    case 'p': port = Integer.parseInt(args[k].substring(3)); break;
                    case 'h': host = args[k].substring(3); break;
                    case 'd':
                        StrongServer.path = args[k].substring(3);
                        FixPath();
                        break;
                    default: Usage(); return;
                }
                k++;
            }
            Run();
        }
        static void FixPath()
        {
            if (StrongServer.path == "")
                return;
            if (StrongServer.path.contains("/") && !StrongServer.path.endsWith("/"))
                StrongServer.path += "/";
            else if (!StrongServer.path.endsWith("\\"))
                StrongServer.path += "\\";
    //        AccessController.checkPermission(new FilePermission(StrongServer.path,"read,write"));
        }
        /// <summary>
        /// Provide help about the command line options
        /// </summary>
        static void Usage()
        {
            String serverName = "StrongDBMS";
            System.out.println("Usage: " + serverName + " [-d:path] [-h:host] [-p:port] [-s:http] [-t:nn] [-S:https] {-flag}");
            System.out.println("Parameters:");
            System.out.println("   -d  Use the given folder for database storage");
            System.out.println("   -h  Use the given host address. Default is 127.0.0.1.");
            System.out.println("   -p  Listen on the given port. Default is 5433");
        }
        /// <summary>
        /// Version information
        /// </summary>
 	static String[] Version = new String[]
{
    "Strong DBMS (c) 2019 Malcolm Crowe and University of the West of Scotland",
    "0.0"," (13 February 2019)", " github.com/MalcolmCrowe/ShareableDataStructures"
};

}
