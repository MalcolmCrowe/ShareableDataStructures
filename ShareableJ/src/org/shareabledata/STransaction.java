/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.io.*;
/**
 *
 * @author Malcolm
 */
public class STransaction extends SDatabase {
        // uids above this number are for uncommitted objects
        public static final long _uid = 0x40000000;
        public final long uid;
        public final boolean autoCommit;
        public final SDatabase rollback;
        @Override
        protected boolean getCommitted(){
                return false;
        }
        @Override
        SDatabase getRollback() {
            return rollback;
        }
        public STransaction(SDatabase d, boolean auto)
        {
            super(d);
            uid = _uid;
            autoCommit = auto;
            rollback = d.getRollback();
        }
        private STransaction(STransaction tr,SDict<Long,SDbObject>obs,SDict<String,SDbObject> nms,long c) throws Exception
        {
            super(tr,obs,nms,c);
            uid =  tr.uid+1;
            autoCommit = tr.autoCommit;
            rollback = tr.rollback;
        }
        public Serialisable _Get(long pos) {
            var ob = objects.Lookup(pos);
            return (ob!=null)?ob:super._Get(pos);
        }
        @Override
        protected SDatabase New(SDict<Long, SDbObject> o, SDict<String, SDbObject> ns, long c)
        {
            STransaction r;
            try {
                r = new STransaction(this,o, ns, c);
            } catch (Exception e){ r = this; }
            return r;
        }
        /// <summary>
        /// If there are concurrent transactions there will be more code here.
        /// </summary>
        /// <returns>the steps as modified by the commit process</returns>
        public SDatabase Commit() throws Exception
        {
            AStream dbfile = dbfiles.Lookup(name);
            SDatabase db = databases.Lookup(name);
            var rdr = new Reader(dbfile,curpos);
            var since = rdr.GetAll(this, rollback.curpos, db.curpos);
            for (SDbObject since1 : since) {
                for (org.shareabledata.Bookmark<org.shareabledata.SSlot<java.lang.Long, org.shareabledata.SDbObject>> b = objects.PositionAt(_uid); b != null; b = b.Next()) {
                    if (since1.Conflicts(b.getValue().val)) {
                        throw new Exception("Transaction Conflict on " + b.getValue());
                    }
                }
            }
            synchronized (dbfile)
            {
                since = rdr.GetAll(this, db.curpos,dbfile.length);
                for (SDbObject since1 : since) {
                    for (org.shareabledata.Bookmark<org.shareabledata.SSlot<java.lang.Long, org.shareabledata.SDbObject>> b = objects.PositionAt(_uid); b != null; b= b.Next()) {
                        if (since1.Conflicts(b.getValue().val)) {
                            throw new Exception("Transaction Conflict on " + b.getValue());
                        }
                    }
                }
                db = dbfile.Commit(db,this);
                dbfile.CommitDone();
            }
            Install(db);
            return db;
        }
        @Override
        public STransaction Transact(boolean auto)
        {
            return this; // ignore the parameter
        }
        @Override
        public SDatabase Rollback()
        {
            return rollback;
        }
    }

