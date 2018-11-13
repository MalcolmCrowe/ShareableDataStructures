/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.net.*;
/**
 *
 * @author Malcolm
 */
public class StrongConnect {
        ClientStream asy;
        public StrongConnect(String host,int port,String fn) throws Exception
        {
            Socket socket = null;
            try
            {
                var ad = InetAddress.getByName(host);
                socket = new Socket(ad,port);
            }
            catch (Exception e)
            {
            }
            if (socket == null || !socket.isConnected())
                throw new Exception("No connection to " + host + ":" + port);
            asy = new ClientStream(this, socket);
            asy.PutString(fn);
            asy.Receive();
        }
        public void CreateTable(String n,SColumn... cols) throws Exception
        {
            asy.Write(Protocol.Table);
            asy.PutString(n);
            asy.PutInt(cols.length);
            for(var i=0;i<cols.length;i++)
            {
                asy.PutString(cols[i].name);
                asy.WriteByte((byte)cols[i].dataType);
            }
            var b = asy.Receive();
        }
        public void CreateIndex(String tn,byte t,String rt,String... key)
                throws Exception
        {
            asy.Write(Protocol.Index);
            asy.PutString(tn);
            asy.WriteByte(t);
            if (rt == null)
                asy.PutInt(0);
            else
                asy.PutString(rt);
            asy.PutInt(key.length);
            for (var i=0;i<key.length;i++)
                asy.PutString(key[i]);
            var b = asy.Receive();
        }
        public void Insert(String tn,String[] cols,Serialisable[]... rows)
                throws Exception
        {
            asy.Write(Protocol.Insert);
            asy.PutString(tn);
            if (cols == null)
                asy.PutInt(0);
            else
            {
                asy.PutInt(cols.length);
                for (var i=0;i<cols.length;i++)
                    asy.PutString(cols[i]);
            }
            asy.PutInt(rows[0].length); // cols
            asy.PutInt(rows.length);  // rows
            for (var i = 0; i < rows.length; i++)
                for (var j = 0; j < rows[i].length; j++)
                    rows[i][j].Put(asy);
            var b = asy.Receive();
        }
        public String Get(String tn,Serialisable... key) throws Exception
        {
            asy.Write(Protocol.Get);
            asy.PutString(tn);
            asy.PutInt(key.length);
            for (var i=0;i<key.length;i++)
                key[i].Put(asy);
            asy.Flush();
            var r = asy.GetString();
            return r; //Json
        }
/*        public bool RoundTrip(Random rnd)
        {
      //      asy.Flush();
            var n = rnd.Next(10, 30);
            Console.WriteLine("Send group will be " + n);
            for (int i = 0; i < n; i++)
            {
                var x = rnd.Next(1, 255);
                Console.Write(" " + x);
                asy.WriteByte((byte)x);
            }
            asy.Flush();
            Console.WriteLine(" Sent "+n);
            var m = rnd.Next(10,30);
            Console.WriteLine("Receive group should be " + m);
            for(var i=0;i<m;i++)
            {
                var x = rnd.Next(1, 255);
                var y = asy.ReadByte();
                if (y < 0)
                {
                    Console.WriteLine("EOF seen");
                    return false;
                }
                if (x != y)
                    Console.WriteLine("Mismatch " + x + " vs " + y+ " rbuf is "+asy.rbuf.bid);
            }
            Console.WriteLine(" Matched " + m);
            return true;
        } */
        public void Close() throws Exception
        {
            asy.client.close();
        }
    }
    
