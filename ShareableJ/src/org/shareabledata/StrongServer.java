/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.net.*;
import java.util.*;
import java.time.*;
/**
 *
 * @author Malcolm
 */
public class StrongServer implements Runnable {
    /// <summary>
    /// The client socket
    /// </summary>
    Socket client;
    /// <summary>
    /// the Strong protocol stream for this client
    /// </summary>
    ServerStream asy;
    SDatabase db;
    static int _cid = 0;
    int cid = _cid++;
    static Random testlock = new Random();
    public LocalDateTime lastop = LocalDateTime.now();
    public Thread myThread = null;
    public static String path= System.getProperty("user.dir");
    /// <summary>
    /// Constructor: called on Accept
    /// </summary>
    /// <param name="c">the newly connected Client socket</param>
    public StrongServer(Socket c)
    {
        client = c;
    }
    public void run() 
    {
        int p = -1;
        try
        {
            asy = new ServerStream(client);
            var fn = asy.GetString();
    //       Console.WriteLine("Received " + fn);
            db = SDatabase.Open(path,fn);
            asy.WriteByte(Responses.Done);
            asy.Flush();
        } 
        catch (Exception e)
        {
            try
            {
                asy.StartException();
                asy.WriteByte(Responses.Exception);
                asy.PutString(e.getMessage());
                asy.Flush();
            }
            catch (Exception ee) 
            { 
                System.out.println(ee.getMessage());
            }
            return;
        }
        // start a Strong protocol service
        for (; ;)
        {
            p = -1;
            try
            {
                p = asy.ReadByte();
            } catch(Exception e)
            {
                p = -1;
            }
            if (p < 0)
                return;
            try
            {
                switch ((byte)p)
                {
                    case Protocol.Get:
                    {
                        var tb = (STable)STable.Get(asy);
                        var n = asy.GetInt();
                        SCList<Variant> k = null;
                        for (var i = 0; i < n; i++)
                        {
                            var v = new Variant(Variants.Single,asy._Get(db));
                            k = (k==null)?new SCList<>(v):(SCList<Variant>)k.InsertAt(v, k.Length);
                        }
                        SIndex px = null;
                        for (var b = db.objects.First(); px == null && b != null; b = b.Next())
                            if (b.getValue().val.type==Types.SIndex)
                            {
                                var x = (SIndex)b.getValue().val;
                                if (x.table == tb.uid)
                                    px = x;
                            }
                        var sb = new StringBuilder("[");
                        var cm = "";
                        for (var b=px.rows.PositionAt(k);b!=null; b=(MTreeBookmark<Long>)b.Next())
                        {
                            sb.append(cm); cm = ",";
                            db.Get(b.value()).Append(sb);
                        }
                        sb.append(']');
                        asy.PutString(sb.toString());
                        asy.Flush();
                        break;
                    }
                    case Protocol.Table:
                    {
                        var tr = db.Transact(true);
                        var tn = asy.GetString();// table name
                        if (tr.names!=null && tr.names.Contains(tn))
                            throw new Exception("Duplicate table name " + tn);
                        var tb = new STable(tr, tn);
                        tr = tr.Add(tb); 
                        var n = asy.GetInt(); // #cols
                        for (var i = 0; i < n; i++)
                        {
                            var cn = asy.GetString(); // column name
                            var dt = asy.ReadByte(); // dataType
                            tr = tr.Add(new SColumn(tr,cn,dt,tb.uid));
                        }
                        db = db.MaybeAutoCommit(tr);
                        asy.WriteByte(Responses.Done);
                        asy.Flush();
                        break;
                    }
                    case Protocol.Insert:
                    {
                        var tr = db.Transact(true);
                        var tn = asy.GetString();
                        SDbObject t = null;
                        if (tr.names!=null)
                            t = tr.names.Lookup(tn); 
                        if (t==null || t.type!=Types.STable)
                            throw new Exception("No table "+tn);
                        var tb = (STable)t;
                        var n = asy.GetInt(); // # named cols
                        SList<Long> cs = null;
                        Exception ex = null;
                        for (var i = 0; i < n; i++)
                        {
                            var cn = asy.GetString();
                            SColumn sc = null;
                            if (tb.names!=null)
                                sc = tb.names.Lookup(cn);
                            if (sc!=null)
                                cs = cs.InsertAt(sc.uid, cs.Length);
                            else
                                ex = new Exception("Column " + cn + " not found");
                        }
                        var nc = asy.GetInt(); // #cols
                        if ((n==0 && nc!=tb.cpos.Length) || (n!=0 && n!=nc))
                            throw new Exception("Wrong number of columns");
                        var nr = asy.GetInt(); // #records
                        for (var i = 0; i < nr; i++)
                        {
                            SDict<Long, Serialisable> f = null;
                            if (n == 0)
                                for (var b = tb.cpos; b.Length != 0; b = b.next)
                                    f = f.Add(b.element.uid, asy._Get(tr)); // serialsable values
                            else
                                for (var b = cs; b.Length != 0; b = b.next)
                                    f = f.Add(b.element, asy._Get(tr)); // serialisable values
                                tr = tr.Add(new SRecord(tr, tb.uid, f));
                        }
                        if (ex != null)
                            throw ex;
                        db = db.MaybeAutoCommit(tr);
                        asy.WriteByte(Responses.Done);
                        asy.Flush();
                        break;
                    }
                    case Protocol.Alter:
                    {
                        var tr = db.Transact(true);
                        var tn = asy.GetString(); // table name
                        SDbObject t = null;
                        if (tr.names!=null)
                            t = tr.names.Lookup(tn);
                        if (t==null || t.type!=Types.STable)
                            throw new Exception("Table "+tn+" not found"); 
                        var tb = (STable)t;
                        var cn = asy.GetString(); // column name or ""
                        var nm = asy.GetString(); // new name
                        if (cn.length()==0)
                            tr = tr.Add(new SAlter(tr, nm,Types.STable, 
                                    tb.uid, 0));
                        else
                        {
                            SColumn sc = null;
                            if (tb.names!=null)
                                sc = tb.names.Lookup(cn);
                            if (sc==null)
                                throw new Exception("Column "+cn+" not found");
                            tr = tr.Add(new SAlter(tr, nm, 
                                    Types.SColumn, tb.uid, sc.uid));
                        }
                        db = db.MaybeAutoCommit(tr);
                        asy.WriteByte(Responses.Done);
                        asy.Flush();
                        break;
                    }
                    case Protocol.Drop:
                    {
                        var tr = db.Transact(true);
                        var nm = asy.GetString(); // object name
                        var pt = (tr.names==null)?null:tr.names.Lookup(nm);
                        if (pt==null)
                            throw new Exception("Object " + nm + " not found");
                        var cn = asy.GetString();
                        SDrop nd = null;
                        if (cn.length()==0)
                            nd = new SDrop(tr,pt.uid,-1);
                        else if (pt.type==Types.STable)
                        {
                            SColumn sc = null;
                            var tb = (STable)pt;
                            if (tb.names!=null)
                                sc = tb.names.Lookup(cn);
                            if (sc==null)
                                throw new Exception("Column " + cn + " not found");
                            nd = new SDrop(tr,sc.uid,pt.uid);
                        } else
                            throw new Exception("Table expected");
                        tr = tr.Add(nd);
                        db = db.MaybeAutoCommit(tr);
                        asy.WriteByte(Responses.Done);
                        asy.Flush();
                        break;
                    }
                    case Protocol.Index:
                    {
                        var tr = db.Transact(true);
                        var tn = asy.GetString(); // table name
                        var t = (tr.names==null)?null:tr.names.Lookup(tn);
                        if (t==null || t.type!=Types.STable)
                            throw new Exception("Table " + tn + " not found");
                        var tb = (STable)t;
                        var xt = asy.ReadByte();
                        var rn = asy.GetString();
                        var nc = asy.GetInt();
                        SList<Long>cs = null;
                        for (var i=0;i<nc;i++)
                        {
                            var cn = asy.GetString();
                            SColumn sc = null;
                            if (tb.names!=null)
                                sc = tb.names.Lookup(cn);
                            if (sc==null)
                                throw new Exception("Column " + cn + " not found");
                            cs = cs.InsertAt(sc.uid,cs.Length);
                        }
                        tr = tr.Add(new SIndex(tr, tb.uid, xt < 2, cs));
                        db = db.MaybeAutoCommit(tr);
                        asy.WriteByte(Responses.Done);
                        asy.Flush();
                        break;
                   }
                   case Protocol.Read:
                   {
                        var id = asy.GetLong();
                        var sb = new StringBuilder();
                        db.Get(id).Append(sb);
                        asy.PutString(sb.toString());
                        asy.Flush();
                        break;
                   }
                   case Protocol.Update:
                   {
                        var tr = db.Transact(true);
                        var id = asy.GetLong();
                        var rc = db.Get(id);
                        var tb = (STable)tr.Lookup(rc.table); 
                        var n = asy.GetInt(); // # cols updated
                        SDict<Long, Serialisable> f = null;
                        Exception ex = null;
                        for (var i = 0; i < n; i++)
                        {
                            var cn = asy.GetString();
                            SColumn sc = null;
                            if (tb.names!=null)
                                sc = tb.names.Lookup(cn);
                            if (sc==null)
                                ex = new Exception("Column "+cn+" not found");
                            else
                                f = f.Add(sc.uid, asy._Get(db));
                        }
                        tr = tr.Add(new SUpdate(tr, rc, f));
                        if (ex != null)
                            throw (ex);
                        db = db.MaybeAutoCommit(tr);
                        asy.WriteByte(Responses.Done);
                        asy.Flush();
                        break;
                    }
                    case Protocol.Delete:
                    {
                        var tr = db.Transact(true);
                        var id = asy.GetLong();
                        var rc = db.Get(id);
                        if (rc==null)
                            throw new Exception("Record " + id + " not found");
                        tr = tr.Add(new SDelete(tr, rc.table,rc.uid));
                        db = db.MaybeAutoCommit(tr);
                        asy.WriteByte(Responses.Done);
                        asy.Flush();
                        break;
                    }
                }
            }
            catch (Exception e)
            {
                try
                {
                    db = db.Rollback();
           //       db.result = null;
                    asy.StartException();
                    asy.WriteByte(Responses.Exception);
                    asy.PutString(e.getMessage());
                    asy.Flush();
                }
                catch(Exception ee) {}
            }
        }
    }
}
