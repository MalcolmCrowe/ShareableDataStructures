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
    Reader rdr;
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
            rdr = asy.rbuf;
            var fn = rdr.GetString();
            //       Console.WriteLine("Received " + fn);
            db = SDatabase.Open(path, fn);
            asy.WriteByte((byte)Types.Done);
            asy.Flush();
        } catch (Exception e) {
            try {
                System.out.println(e.getMessage());
                asy.StartException();
                asy.WriteByte((byte)Types.Exception);
                asy.PutString(e.getMessage());
                asy.Flush();
            } catch (Exception ee) {
                System.out.println(ee.getMessage());
            }
            return;
        }
        // start a Strong protocol service
        for (;;) {
            p = -1;
            try {
                p = rdr.ReadByte();
            } catch (Exception e) {
                p = -1;
            }
            if (p < 0) {
                return;
            }
            System.out.println("Protocol byte "+Types.types[p]);
            try {
                switch ((byte) p) {
                    case Types.DescribedGet:
                    case Types.Get: {
                        var tr = db.Transact(true);
                        var q = rdr._Get(db);
                        if (!(q instanceof SQuery))
                            throw new Exception("Bad query");
                        var qy = (SQuery) q;
                        RowSet rs = qy.RowSet(tr,null);
                        var sb = new StringBuilder("[");
                        var cm = "";
                        for (var b = rs.First();b!=null;b=b.Next())
                        {
                            sb.append(cm); cm = ",";
                            ((RowBookmark)b)._ob.Append(db,sb);
                        }
                        sb.append(']');
                        asy.WriteByte((byte)Types.Done);
                        if (p==Types.DescribedGet)
                        {
                            var d = rs._qry.getDisplay();
                            asy.PutInt(d.Length);
                            for (var b=d.First();b!=null;b=b.Next())
                                asy.PutString(b.getValue().val);
                        }
                        asy.PutString(sb.toString());
                        asy.Flush();
                        break;
                    }
                    case Types.SCreateTable: {
                        var tr = db.Transact(true);
                        var tn = rdr.GetString();// table name
                        if (tr.names != null && tr.names.Contains(tn)) {
                            throw new Exception("Duplicate table name " + tn);
                        }
                        var tb = new STable(tr, tn);
                        tr = (STransaction)tr.Install(tb,tr.curpos);
                        var n = rdr.GetInt(); // #cols
                        for (var i = 0; i < n; i++) {
                            var cn = rdr.GetString(); // column name
                            var dt = rdr.ReadByte(); // dataType
                            tr = (STransaction)tr.Install(new SColumn(tr, cn, dt, tb.uid),tr.curpos);
                        }
                        db = db.MaybeAutoCommit(tr);
                        asy.WriteByte((byte)Types.Done);
                        asy.Flush();
                        break;
                    }
                    case Types.SInsert:
                    {
                        var tr = db.Transact(true);
                        tr = SInsertStatement.Get(db,rdr).Obey(tr);
                        db = db.MaybeAutoCommit(tr);
                        asy.WriteByte((byte)Types.Done);
                        asy.Flush();
                        break;
                    }
                    case Types.Insert: {
                        var tr = db.Transact(true);
                        var tn = rdr.GetString();
                        SDbObject t = null;
                        if (tr.names != null) {
                            t = tr.names.Lookup(tn);
                        }
                        if (t == null || t.type != Types.STable) {
                            throw new Exception("No table " + tn);
                        }
                        var tb = (STable) t;
                        var n = rdr.GetInt(); // # named cols
                        SList<Long> cs = null;
                        Exception ex = null;
                        for (var i = 0; i < n; i++) {
                            var cn = rdr.GetString();
                            SColumn sc = null;
                            if (tb.names != null) {
                                sc = (SColumn)tb.names.Lookup(cn);
                            }
                            if (sc != null) {
                                cs = (cs==null)?new SList(sc.uid):
                                        cs.InsertAt(sc.uid, cs.Length);
                            } else {
                                ex = new Exception("Column " + cn + " not found");
                            }
                        }
                        var nc = rdr.GetInt(); // #cols
                        if ((n == 0 && nc != tb.cpos.Length) || (n != 0 && n != nc)) {
                            throw new Exception("Wrong number of columns");
                        }
                        var nr = rdr.GetInt(); // #records
                        for (var i = 0; i < nr; i++) {
                            SDict<Long, Serialisable> f = null;
                            if (n == 0) {
                                for (var b = tb.cpos.First(); b!=null; b = b.Next()) {
                                    var k = ((SDbObject)b.getValue().val).uid;
                                    var v = rdr._Get(tr);
                                    f = (f==null)?new SDict<>(k,v):f.Add(k,v); // serialisable values
                                }
                            } else {
                                for (var b = cs; b!=null && b.Length != 0; b = b.next) {
                                    var k = b.element;
                                    var v = rdr._Get(tr);
                                    f = (f==null)?new SDict<>(k,v):f.Add(k,v); // serialisable values
                                }
                            }
                            tr = (STransaction)tr.Install(new SRecord(tr, tb.uid, f),tr.curpos);
                        }
                        if (ex != null) {
                            throw ex;
                        }
                        db = db.MaybeAutoCommit(tr);
                        asy.WriteByte((byte)Types.Done);
                        asy.Flush();
                        break;
                    }
                    case Types.SAlter: {
                        var tr = db.Transact(true);
                        var tn = rdr.GetString(); // table name
                        SDbObject t = null;
                        if (tr.names != null) {
                            t = tr.names.Lookup(tn);
                        }
                        if (t == null || t.type != Types.STable) {
                            throw new Exception("Table " + tn + " not found");
                        }
                        var tb = (STable) t;
                        var cn = rdr.GetString(); // column name or ""
                        var nm = rdr.GetString(); // new name
                        if (cn.length() == 0) {
                            tr = (STransaction)tr.Install(new SAlter(tr, nm, Types.STable,
                                    tb.uid, 0),tr.curpos);
                        } else {
                            SColumn sc = null;
                            if (tb.names != null) {
                                sc = (SColumn)tb.names.Lookup(cn);
                            }
                            if (sc == null) {
                                throw new Exception("Column " + cn + " not found");
                            }
                            tr = (STransaction)tr.Install(new SAlter(tr, nm,
                                    Types.SColumn, tb.uid, sc.uid),tr.curpos);
                        }
                        db = db.MaybeAutoCommit(tr);
                        asy.WriteByte((byte)Types.Done);
                        asy.Flush();
                        break;
                    }
                    case Types.SDrop: {
                        var tr = db.Transact(true);
                        var nm = rdr.GetString(); // object name
                        var pt = (tr.names == null) ? null : tr.names.Lookup(nm);
                        if (pt == null) {
                            throw new Exception("Object " + nm + " not found");
                        }
                        var cn = rdr.GetString();
                        SDrop nd = null;
                        if (cn.length() == 0) {
                            nd = new SDrop(tr, pt.uid, -1);
                        } else if (pt.type == Types.STable) {
                            SColumn sc = null;
                            var tb = (STable) pt;
                            if (tb.names != null) {
                                sc = (SColumn)tb.names.Lookup(cn);
                            }
                            if (sc == null) {
                                throw new Exception("Column " + cn + " not found");
                            }
                            nd = new SDrop(tr, sc.uid, pt.uid);
                        } else {
                            throw new Exception("Table expected");
                        }
                        tr = (STransaction)tr.Install(nd,tr.curpos);
                        db = db.MaybeAutoCommit(tr);
                        asy.WriteByte((byte)Types.Done);
                        asy.Flush();
                        break;
                    }
                    case Types.SCreateIndex: {
                        var tr = db.Transact(true);
                        var tn = rdr.GetString(); // table name
                        var t = (tr.names == null) ? null : tr.names.Lookup(tn);
                        if (t == null || t.type != Types.STable) {
                            throw new Exception("Table " + tn + " not found");
                        }
                        var tb = (STable) t;
                        var xt = rdr.ReadByte();
                        var rn = rdr.GetString();
                        var nc = rdr.GetInt();
                        SList<Long> cs = null;
                        for (var i = 0; i < nc; i++) {
                            var cn = rdr.GetString();
                            SColumn sc = null;
                            if (tb.names != null) {
                                sc = (SColumn)tb.names.Lookup(cn);
                            }
                            if (sc == null) {
                                throw new Exception("Column " + cn + " not found");
                            }
                            cs = (cs == null) ? new SList<Long>(sc.uid)
                                    : cs.InsertAt(sc.uid, cs.Length);
                        }
                        var x = new SIndex(tr, tb.uid, xt < 2, cs);
                        tr = (STransaction)tr.Install(x,tr.curpos);
                        db = db.MaybeAutoCommit(tr);
                        asy.WriteByte((byte)Types.Done);
                        asy.Flush();
                        break;
                    }
                    case Types.Read: {
                        var id = rdr.GetLong();
                        var sb = new StringBuilder();
                        db.Get(id).Append(db,sb);
                        asy.PutString(sb.toString());
                        asy.Flush();
                        break;
                    }
                    case Types.SUpdateSearch:
                    {
                        var tr = db.Transact(true);
                        tr = SUpdateSearch.Get(db, rdr).Obey(tr,Context.Empty);
                        db = db.MaybeAutoCommit(tr);
                        asy.WriteByte((byte)Types.Done);
                        asy.Flush();
                        break;
                    }                    
                    case Types.SUpdate: {
                        var tr = db.Transact(true);
                        var id = rdr.GetLong();
                        var rc = db.Get(id);
                        var tb = (STable) tr.objects.Lookup(rc.table);
                        var n = rdr.GetInt(); // # cols updated
                        SDict<String, Serialisable> f = null;
                        Exception ex = null;
                        for (var i = 0; i < n; i++)
                        {
                            var cn = rdr.GetString();
                            f =(f==null)?new SDict(cn,rdr._Get(db))
                                    :f.Add(cn, rdr._Get(db));
                        }
                        tr = (STransaction)tr.Install(new SUpdate(tr, rc, f),
                                tr.curpos);
                        if (ex != null) {
                            throw (ex);
                        }
                        db = db.MaybeAutoCommit(tr);
                        asy.WriteByte((byte)Types.Done);
                        asy.Flush();
                        break;
                    }
                    case Types.SDeleteSearch:
                    {
                        var tr = db.Transact(true);
                        tr = SDeleteSearch.Get(db, rdr).Obey(tr,Context.Empty);
                        db = db.MaybeAutoCommit(tr);
                        asy.WriteByte((byte)Types.Done);
                        asy.Flush();
                        break;
                    }
                    case Types.SDelete: {
                        var tr = db.Transact(true);
                        var id = rdr.GetLong();
                        var rc = db.Get(id);
                        if (rc == null) {
                            throw new Exception("Record " + id + " not found");
                        }
                        tr = (STransaction)tr.Install(new SDelete(tr, rc.table, 
                                rc.uid),tr.curpos);
                        db = db.MaybeAutoCommit(tr);
                        asy.WriteByte((byte)Types.Done);
                        asy.Flush();
                        break;
                    }
                    case Types.SBegin:
                        db = new STransaction(db, false);
                        asy.WriteByte((byte)Types.Done);
                        asy.Flush();
                        break;
                    case Types.SRollback:
                        db = db.Rollback();
                        asy.WriteByte((byte)Types.Done);
                        asy.Flush();
                        break;
                    case Types.SCommit:
                        {
                            if (!(db instanceof STransaction))
                                throw new Exception("No transaction to commit");
                            var tr = (STransaction)db; 
                            db = tr.Commit();
                            asy.WriteByte((byte)Types.Done);
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
                    asy.WriteByte((byte)Types.Exception);
                    var m = e.getMessage();
                    if (m==null)
                        m = e.toString();
                    asy.PutString(m);
                    asy.Flush();
                } catch (Exception ee) {
                }
            }
        }
    }
}
