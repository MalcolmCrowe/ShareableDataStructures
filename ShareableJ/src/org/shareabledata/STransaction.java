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
        public final SDict<Integer,SDbObject> steps;
        protected boolean getCommitted(){
                return false;
        }
        SDatabase getRollback() {
            return rollback;
        }
        public STransaction Add(SDbObject s) throws Exception
        {
            return new STransaction(this,s);
        }
        public STransaction(SDatabase d, boolean auto)
        {
            super(d);
            uid = _uid;
            steps = null;
            autoCommit = auto;
            rollback = d.getRollback();
        }
        /// <summary>
        /// This clever routine indirectly calls the protected SDtabase constructors
        /// that add new objects to the SDatabase (see the call to tr.Add).
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="s"></param>
        private STransaction(STransaction tr,SDbObject s) throws Exception
        {
            super(tr._Add(s, tr.uid+1));
            uid =  tr.uid+1;
            steps = (tr.steps==null)?new SDict<Integer,SDbObject>(0,s):
                    tr.steps.Add(tr.steps.Length,s);
            autoCommit = tr.autoCommit;
            rollback = tr.rollback;
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
            for (var i = 0; i < since.length; i++)
                for (var b = steps.First(); b != null; b = b.Next())
                    if (since[i].Conflicts(b.getValue().val))
                        throw new Exception("Transaction Conflict on " + b.getValue());
            synchronized (dbfile)
            {
                since = rdr.GetAll(this, db.curpos,dbfile.length);
                for (var i = 0; i < since.length; i++)
                    for (var b = steps.First(); b != null; b = b.Next())
                        if (since[i].Conflicts(b.getValue().val))
                            throw new Exception("Transaction Conflict on " + b.getValue());
                db = dbfile.Commit(db,steps);
            }
            Install(db);
            return db;
        }
        /// <summary>
        /// We will single-quote transaction-local uids
        /// </summary>
        /// <returns>a more readable version of the uid</returns>
        static String Uid(long uid)
        {
            if (uid > _uid)
                return "'" + (uid - _uid);
            return "" + uid;
        }
        public STransaction Transact(boolean auto)
        {
            return this; // ignore the parameter
        }
        public SDatabase Rollback()
        {
            return rollback;
        }
    }

