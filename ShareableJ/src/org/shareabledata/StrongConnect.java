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
            for (SColumn col : cols) {
                asy.PutString(col.name);
                asy.WriteByte((byte) col.dataType);
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
            for (String key1 : key) {
                asy.PutString(key1);
            }
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
                for (String col : cols) {
                    asy.PutString(col);
                }
            }
            asy.PutInt(rows[0].length); // cols
            asy.PutInt(rows.length);  // rows
            for (Serialisable[] row : rows) {
                for (Serialisable row1 : row) {
                    row1.Put(asy);
                }
            }
            var b = asy.Receive();
        }
        public String Get(SQuery qy) throws Exception
        {
            asy.Write(Protocol.Get);
            qy.Put(asy);
            asy.Flush();
            var r = asy.rbuf.GetString();
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
    
