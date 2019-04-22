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
        SDict<Long,String> preps = null;
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
            var wtr = asy.wtr;
            wtr.PutString(fn);
            asy.Flush();
            asy.rdr.ReadByte();
            preps = null;
        }
        public long Prepare(String n)
        {
            var u = -2L-((preps!=null)?(preps.Length):0);
            preps = (preps==null)?new SDict(u,n):preps.Add(u, n);
            return u;
        }
        public void CreateTable(String n) throws Exception
        {
            var un = Prepare(n);
            var wtr = asy.wtr;
            wtr.SendUids(preps);
            wtr.Write((byte)Types.SCreateTable);
            wtr.PutLong(un);
            wtr.PutInt(0);
            wtr.PutInt(0);
            var b = asy.Receive();
            preps = null;
        }
        public void CreateColumn(String c,int t,String tn,SSlot<String,SFunction>...cs)
                throws Exception
        {
            var uc = Prepare(c);
            var ut = Prepare(tn);
            asy.wtr.SendUids(preps);
            asy.wtr.WriteByte((byte)Types.SCreateColumn);
            new SColumn(uc, t, ut, new SDict(cs)).PutColDef(asy.wtr);
            var b = asy.Receive();
            preps = null;
        }
        public void CreateIndex(String tn,byte t,String rt,String... key)
                throws Exception
        {
            var ut = Prepare(tn);
            long u = -1;
            if (rt!=null)
                u = Prepare(rt);
            SList<Long> keys = null;
            int i = 0;
            for (String key1 : key) 
            {
                keys =(keys==null)?new SList(Prepare(key1)):
                        keys.InsertAt(Prepare(key1),i);
                i++;
            }
            asy.wtr.SendUids(preps);
            new SIndex(ut,t==IndexType.Primary,u,keys).Put(asy.wtr);
            var b = asy.Receive();
            preps = null;
        }
        public void Insert(String tn,String[] cols,Serialisable[]... rows)
                throws Exception
        {
            var ut = Prepare(tn);
            var u = new long[cols.length];
            for (var i = 0; i < cols.length; i++)
                u[i] = Prepare(cols[i]);
            var wtr = asy.wtr;
            wtr.SendUids(preps);
            wtr.WriteByte((byte)Types.Insert);
            wtr.PutLong(ut);
            if (cols == null)
                wtr.PutInt(0);
            else
            {
                wtr.PutInt(cols.length);
                for (long ui : u) {
                    wtr.PutLong(ui);
                }
            }
            wtr.PutInt(rows[0].length); // cols
            wtr.PutInt(rows.length);  // rows
            for (Serialisable[] row : rows) {
                for (Serialisable row1 : row) {
                    row1.Put(asy.wtr);
                }
            }
            var b = asy.Receive();
        }
        public DocArray ExecuteQuery(String sql) throws Exception
        {
            var pair = Parser.Parse(sql);
            if (pair.ob.type!=Types.SSelect)
                throw new Exception("Bad query " + sql);
            var qry = (SQuery)pair.ob;
            return Get(pair.ns,qry);
        }
        public int ExecuteNonQuery(String sql) throws Exception
        {
            var s = Parser.Parse(sql);
            if (s == null)
                return Types.Exception;
            asy.wtr.SendUids(s.ns);
            s.ob.Put(asy.wtr);
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
        public DocArray Get(SDict<Long,String> d,Serialisable tn) throws Exception
        {
            var wtr = asy.wtr;
            var rdr = asy.rdr;
            wtr.SendUids(d);
            wtr.Write((byte)Types.DescribedGet);
            tn.Put(wtr);
            var b = asy.Receive();
            if (b == (byte)Types.Exception)
            {
                inTransaction = false;
                rdr.GetException();
            }
            if (b == (byte)Types.Done)
            {
                description = null;
                var n = rdr.GetInt();
                for (var i = 0; i < n; i++)
                    description = (description==null)?
                            new SDict(i, rdr.GetString()):
                            description.Add(i, rdr.GetString());
                return new DocArray(rdr.GetString());
            }
            throw new Exception("PE28");
        }
        public void BeginTransaction() throws Exception
        {
            asy.wtr.Write((byte)Types.SBegin);
            var b = asy.Receive();
            if (b == Types.Exception)
            {
                inTransaction = false;
                asy.rdr.GetException();
            }
            if (b == Types.Done)
                inTransaction = true;
        }
        public void Rollback() throws Exception
        {
            asy.wtr.Write((byte)Types.SRollback);
            var b = asy.Receive();
            inTransaction = false;
        }
        public void Commit() throws Exception
        {
            asy.wtr.Write((byte)Types.SCommit);
            var b = asy.Receive();
            inTransaction = false;
        }
        public void Close() throws Exception
        {
            asy.client.close();
        }
    }
    
