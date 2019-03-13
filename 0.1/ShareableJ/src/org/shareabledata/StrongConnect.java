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
 * This class is not shareable
 */
public class StrongConnect {
        ClientStream asy;
        public boolean inTransaction = false;
        public SDict<Integer,String> description = null;
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
            asy.Write((byte)Types.SCreateTable);
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
            asy.Write((byte)Types.SCreateIndex);
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
            asy.Write((byte)Types.Insert);
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
        public DocArray ExecuteQuery(String sql) throws Exception
        {
            var qry = (SQuery)Parser.Parse(sql);
            return Get(qry);
        }
        public int ExecuteNonQuery(String sql) throws Exception
        {
            var s = Parser.Parse(sql);
            if (s == null)
                return Types.Exception;
            s.Put(asy);
            var b = asy.Receive();
            if (b == Types.Exception)
                inTransaction = false;
            else
            {
                var su = sql.trim().substring(0, 5).toUpperCase();
                switch (su)
                {
                    case "BEGIN": inTransaction = true; break;
                    case "ROLLB":
                    case "COMMI": inTransaction = false; break;
                }
            }
            return b;
        }
        public DocArray Get(Serialisable tn) throws Exception
        {
            asy.Write((byte)Types.DescribedGet);
            tn.Put(asy);
            var b = asy.Receive();
            if (b == (byte)Types.Exception)
            {
                inTransaction = false;
                asy.GetException();
            }
            if (b == (byte)Types.Done)
            {
                description = null;
                var n = asy.rbuf.GetInt();
                for (var i = 0; i < n; i++)
                    description = (description==null)?
                            new SDict(i, asy.rbuf.GetString()):
                            description.Add(i, asy.rbuf.GetString());
                return new DocArray(asy.rbuf.GetString());
            }
            throw new Exception("??");
        }
        public void BeginTransaction() throws Exception
        {
            asy.Write((byte)Types.SBegin);
            var b = asy.Receive();
            if (b == Types.Exception)
            {
                inTransaction = false;
                asy.GetException();
            }
            if (b == Types.Done)
                inTransaction = true;
        }
        public void Rollback() throws Exception
        {
            asy.Write((byte)Types.SRollback);
            var b = asy.Receive();
            inTransaction = false;
        }
        public void Commit() throws Exception
        {
            asy.Write((byte)Types.SCommit);
            var b = asy.Receive();
            inTransaction = false;
        }
        public void Close() throws Exception
        {
            asy.client.close();
        }
    }
    
