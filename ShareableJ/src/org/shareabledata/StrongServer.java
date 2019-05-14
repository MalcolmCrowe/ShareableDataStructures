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
    public static String path = System.getProperty("user.dir");

    /// <summary>
    /// Constructor: called on Accept
    /// </summary>
    /// <param name="c">the newly connected Client socket</param>
    public StrongServer(Socket c) {
        client = c;
    }

    public void run() {
        int p = -1;
        try {
            asy = new ServerStream(client);
            var fn = asy.rdr.GetString();
            //       Console.WriteLine("Received " + fn);
            db = SDatabase.Open(path, fn);
            asy.wtr.WriteByte((byte)Types.Done);
            asy.wtr.PutLong(0);
            asy.wtr.PutLong(0);
            asy.Flush();
        } catch (Exception e) {
            try {
                System.out.println(e.getMessage());
                asy.StartException();
                asy.wtr.WriteByte((byte)Types.Exception);
                asy.wtr.PutString(e.getMessage());
                asy.Flush();
            } catch (Exception ee) {
                System.out.println(ee.getMessage());
            }
            return;
        }
        // start a Strong protocol service
        for (;;) {
            var rdr = asy.rdr;
            var wtr = asy.wtr;
            p = -1;
            try {
                p = rdr.ReadByte();
            } catch (Exception e) {
                p = -1;
            }
            if (p < 0) {
                return;
            }
            try {
                switch ((byte) p) {
                    case Types.SNames:
                    {
                        var tr = db.Transact(rdr,true);
                        var us = tr.role.uids;
                        var n = rdr.GetInt();
                        for (var i = 0; i < n; i++)
                        {
                            var u = rdr.GetLong();
                            var s = rdr.GetString();
                            if (u < rdr.lastAlias)
                                rdr.lastAlias = u;
                            us = (us==null)?new SDict(u, s):us.Add(u,s);
                        }
                        db = new STransaction(tr,new SRole(tr.role,us));
                        break;
                    }
                    case Types.DescribedGet:
                    case Types.Get: {
                        var tr = db.Transact(rdr,true);
                        Serialisable q = Serialisable.Null;
                        try {
                            q = rdr._Get();
                            tr = (STransaction)rdr.db;
                        } catch(Exception e)
                        {
                            rdr.buf.len = 0;
                            throw e;
                        }
                        if (!(q instanceof SQuery))
                            throw new Exception("Bad query");
                        var qy = (SQuery)q;
                        qy = (SQuery)qy.Prepare(tr, qy.Names(tr,null));
                        RowSet rs = qy.RowSet(tr,qy,null);
                        var sb = new StringBuilder("[");
                        var cm = "";
                        for (var b = rs.First();b!=null;b=b.Next())
                        {
                            var ob = ((RowBookmark)b).Ob();
                            if (!(ob instanceof SRow))
                                continue;
                            var sr = (SRow)ob;
                            if (!sr.isValue())
                                continue;
                            sb.append(cm); cm = ",";
                            sr.Append(db,sb);
                        }
                        sb.append(']');
                        var ts = db.curpos;
                        db = rs._tr.MaybeAutoCommit();
                        wtr.Write(Types.Done);
                        wtr.PutLong(ts);
                        wtr.PutLong(db.curpos);
                        if (p==Types.DescribedGet)
                        {
                            var d = rs._qry.getDisplay();
                            wtr.PutInt(d.Length);
                            for (var b=d.First();b!=null;b=b.Next())
                                wtr.PutString(b.getValue().val.id);
                        }
                        wtr.PutString(sb.toString());
                        asy.Flush();
                        break;
                    }
                    case Types.SCreateTable: {
                        var tr = db.Transact(rdr,true);
                        var tn = db.role.uids.get(rdr.GetLong());// table name
                        if (db.role.globalNames!=null && 
                                db.role.globalNames.Contains(tn)) {
                            throw new Exception("Duplicate table name " + tn);
                        }
                        var tb = new STable(tr);
                        tr = (STransaction)tr.Install(tb,tn,tr.curpos);
                        rdr.db = tr;
                        var n = rdr.GetInt();
                        for (var i = 0; i < n; i++)
                            CreateColumn(rdr);
                        n = rdr.GetInt();
                        for (var i = 0; i < n; i++)
                        {
                            rdr.ReadByte();
                            CreateIndex(rdr);
                        }
                        var ts = db.curpos;
                        db = rdr.db.MaybeAutoCommit();
                        wtr.Write(Types.Done);
                        wtr.PutLong(ts);
                        wtr.PutLong(db.curpos);
                        asy.Flush();
                        break;
                    }
                    case Types.SCreateColumn:
                    {
                        var tr = db.Transact(rdr,true);
                        rdr.db = tr;
                        CreateColumn(rdr);
                        var ts = db.curpos;
                        db = rdr.db.MaybeAutoCommit();
                        wtr.Write(Types.Done);
                        wtr.PutLong(ts);
                        wtr.PutLong(db.curpos);
                        asy.Flush();
                        break;
                    }
                    case Types.SInsert:
                    {
                        var tr = db.Transact(rdr,true);
                        var t = rdr.GetLong();
                        var n = rdr.GetInt();
                        SList<Long> c = null;
                        for (var i=0;i<n;i++)
                            c = (c==null)?new SList(rdr.GetLong()):
                                    c.InsertAt(rdr.GetLong(),i);
                        tr = new SInsert(t,c,rdr._Get()).Prepare(tr,null)
                                .Obey(tr,null);
                        var ts = db.curpos;
                        db = tr.MaybeAutoCommit();
                        wtr.Write(Types.Done);
                        wtr.PutLong(ts);
                        wtr.PutLong(db.curpos);
                        asy.Flush();
                        break;
                    }
                    case Types.Insert: {
                        var tr = db.Transact(rdr,true);
                        var tn = db.role.uids.get(rdr.GetLong());
                        if (!db.role.globalNames.Contains(tn))
                           throw new Exception("Table " + tn + " not found");
                        var tb = (STable)db.objects.get(db.role.globalNames.get(tn));
                        rdr.context = tb;
                        var n = rdr.GetInt(); // # named cols
                        SList<SColumn> cs = null;
                        Exception ex = null;
                        for (var i = 0; i < n; i++) {
                            var cn = db.role.uids.get(rdr.GetLong());
                            var ss = db.role.subs.get(tb.uid);
                            if (ss.defs.Contains(cn))
                            {
                                var sc = (SColumn)db.objects.get(ss.obs.get(ss.defs.get(cn)).key);
                                cs = (cs==null)?new SList(sc):cs.InsertAt(sc,i);
                            }
                            else 
                                ex = new Exception("Column " + cn + " not found");
                        }
                        var nc = rdr.GetInt(); // #cols
                        if ((n == 0 && nc != tb.cpos.Length) || (n != 0 && n != nc)) {
                            ex = new Exception("Wrong number of columns");
                        }
                        var nr = rdr.GetInt(); // #records
                        for (var i = 0; i < nr; i++) {
                            SDict<Long, Serialisable> f = null;
                            if (n == 0) {
                                for (var b = tb.cpos.First(); b!=null; b = b.Next()) {
                                    var k = ((SDbObject)b.getValue().val).uid;
                                    var v = rdr._Get();
                                    f = (f==null)?new SDict(k,v):f.Add(k,v); // serialisable values
                                }
                            } else {
                                for (var b = cs; b!=null && b.Length != 0; b = b.next) {
                                    var k = b.element.uid;
                                    var v = rdr._Get();
                                    f = (f==null)?new SDict(k,v):f.Add(k,v); // serialisable values
                                }
                            }
                            tr = (STransaction)tr.Install(new SRecord(tr, tb.uid, f),tr.curpos);
                        }
                        if (ex != null) {
                            throw ex;
                        }
                        var ts = db.curpos;
                        db = tr.MaybeAutoCommit();
                        wtr.Write(Types.Done);
                        wtr.PutLong(ts);
                        wtr.PutLong(db.curpos);
                        asy.Flush();
                        break;
                    }
                    case Types.SAlter: {
                        var tr = db.Transact(rdr,true);
                        rdr.db = tr;
                        var at = SAlter.Get(rdr);
                        tr = (STransaction)rdr.db;
                        tr = at.Prepare(tr, null)
                            .Obey(tr, Context.Empty);
                        db = tr.MaybeAutoCommit();
                        wtr.WriteByte((byte)Types.Done);
                        asy.Flush();
                        break;
                    }
                    case Types.SDrop: {
                        var tr = db.Transact(rdr,true);
                        var dr = SDrop.Get(rdr).Prepare(tr,null);
                        tr = dr.Obey(tr,Context.Empty);
                        var ts = db.curpos;
                        db = tr.MaybeAutoCommit();
                        wtr.Write(Types.Done);
                        wtr.PutLong(ts);
                        wtr.PutLong(db.curpos);
                        asy.Flush();
                        break;
                    }
                    case Types.SIndex: {
                        var tr = db.Transact(rdr,true);
                        rdr.db = tr;
                        CreateIndex(rdr);
                        tr = (STransaction)rdr.db;
                        var ts = db.curpos;
                        db = tr.MaybeAutoCommit();
                        wtr.Write(Types.Done);
                        wtr.PutLong(ts);
                        wtr.PutLong(db.curpos);
                        asy.Flush();
                        break;
                    }
                    case Types.SDropIndex:
                    {
                        var tr = db.Transact(rdr,true);
                        rdr.db = tr;
                        var dr = new SDropIndex(rdr);
                        tr = (STransaction)rdr.db;
                        tr = dr.Prepare(tr, null)
                            .Obey(tr, Context.Empty);
                        var ts = db.curpos;
                        db = tr.MaybeAutoCommit();
                        wtr.Write(Types.Done);
                        wtr.PutLong(ts);
                        wtr.PutLong(db.curpos);
                        asy.Flush();
                        break;
                    }
                    case Types.Read: {
                        var id = rdr.GetLong();
                        var sb = new StringBuilder();
                        db.Get(id).Append(db,sb);
                        wtr.PutString(sb.toString());
                        asy.Flush();
                        break;
                    }
                    case Types.SUpdateSearch:
                    {
                        var tr = db.Transact(rdr,true);
                        var u = SUpdateSearch.Get(rdr);
                        tr = (STransaction)rdr.db;
                        u = (SUpdateSearch)u.Prepare(tr,u.qry.Names(tr,null));
                        tr = u.Obey(tr,Context.Empty);
                        var ts = db.curpos;
                        db = tr.MaybeAutoCommit();
                        wtr.Write(Types.Done);
                        wtr.PutLong(ts);
                        wtr.PutLong(db.curpos);
                        asy.Flush();
                        break;
                    }                    
                    case Types.SUpdate: {
                        var tr = db.Transact(rdr,true);
                        var id = rdr.GetLong();
                        var rc = db.Get(id);
                        var tb = (STable) tr.objects.Lookup(rc.table);
                        var n = rdr.GetInt(); // # cols updated
                        SDict<Long, Serialisable> f = null;
                        Exception ex = null;
                        for (var i = 0; i < n; i++)
                        {
                            var cn = rdr.GetLong();
                            f =(f==null)?new SDict(cn,rdr._Get())
                                    :f.Add(cn, rdr._Get());
                        }
                        tr = (STransaction)tr.Install(new SUpdate(tr, rc, f),
                                tr.curpos);
                        if (ex != null) {
                            throw (ex);
                        }
                        var ts = db.curpos;
                        db = tr.MaybeAutoCommit();
                        wtr.Write(Types.Done);
                        wtr.PutLong(ts);
                        wtr.PutLong(db.curpos);
                        asy.Flush();
                        break;
                    }
                    case Types.SDeleteSearch:
                    {
                        var tr = db.Transact(rdr,true);
                        var dr = SDeleteSearch.Get(rdr);
                        tr = dr.Prepare(tr,dr.qry.Names(tr,null))
                                .Obey(tr,Context.Empty);
                        var ts = db.curpos;
                        db = tr.MaybeAutoCommit();
                        wtr.Write(Types.Done);
                        wtr.PutLong(ts);
                        wtr.PutLong(db.curpos);
                        asy.Flush();
                        break;
                    }
                    case Types.SDelete: {
                        var tr = db.Transact(rdr,true);
                        var id = rdr.GetLong();
                        var rc = db.Get(id);
                        if (rc == null) {
                            throw new Exception("Record " + id + " not found");
                        }
                        tr = (STransaction)tr.Install(new SDelete(tr, rc),
                                tr.curpos);
                        var ts = db.curpos;
                        db = tr.MaybeAutoCommit();
                        wtr.Write(Types.Done);
                        wtr.PutLong(ts);
                        wtr.PutLong(db.curpos);
                        asy.Flush();
                        break;
                    }
                    case Types.SBegin:
                    {
                        db = db.Transact(rdr, false);
                        var ts = db.curpos;
                        wtr.Write(Types.Done);
                        wtr.PutLong(ts);
                        wtr.PutLong(ts);
                        asy.Flush();
                        break;
                    }
                    case Types.SRollback:
                    {
                        db = db.Rollback();
                        var ts = db.curpos;
                        wtr.Write(Types.Done);
                        wtr.PutLong(ts);
                        wtr.PutLong(ts);
                        asy.Flush();
                        break;
                    }
                    case Types.SCommit:
                        {
                            if (!(db instanceof STransaction))
                                throw new Exception("No transaction to commit");
                            var tr = (STransaction)db; 
                            var ts = db.curpos;
                            db = tr.Commit();
                            wtr.WriteByte((byte)Types.Done);
                            wtr.PutLong(ts);
                            wtr.PutLong(db.curpos);
                            asy.Flush();
                            break;
                        }
                    default:
                        System.out.println("Unknown protocol byte "+p);
                }
            } catch (Exception e) {
                try {
                    db = db.Rollback();
                    //       db.result = null;
                    asy.StartException();
                    wtr.WriteByte((byte)Types.Exception);
                    var m = e.getMessage();
                    if (m==null)
                        m = e.toString();
                    wtr.PutString(m);
                    asy.Flush();
                } catch (Exception ee) {
                }
            }
        }
    }
    void CreateColumn(ReaderBase rdr) throws Exception
    {
        var sc = (SColumn)rdr._Get();
        var db = (STransaction)rdr.db;
        sc = (SColumn)sc.Prepare(db,
                ((STable)db.objects.get(sc.table)).Names(db,null));
        var cn = db.role.uids.get(sc.uid);
        rdr.db = db.Install(new SColumn(db,sc.table,sc.dataType,sc.constraints),
                cn, db.curpos);
    }
    void CreateIndex(ReaderBase rdr) throws Exception
    {
        var db = (STransaction)rdr.db;
        rdr.db = db.Install((SIndex)SIndex.Get(rdr).Prepare(db,null), db.curpos);
    }
}
