/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
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
        public final SDict<Long,Boolean> readConstraints;
        @Override
        protected boolean getCommitted(){
                return false;
        }
        @Override
        SDatabase getRollback() {
            return rollback;
        }
        public STransaction(SDatabase d, ReaderBase rdr,boolean auto)
        {
            super(d);
            uid = _uid;
            autoCommit = auto;
            rollback = d.getRollback();
            readConstraints = null;
            rdr.db = this;
        }
        private STransaction(STransaction tr,SDict<Long,SDbObject>obs,SRole r,long c) throws Exception
        {
            super(tr,obs,r,c);
            uid =  tr.uid+1;
            autoCommit = tr.autoCommit;
            rollback = tr.rollback;
            readConstraints = tr.readConstraints;
        }
        protected STransaction(STransaction tr,long u)
        {
            super(tr);
            autoCommit = tr.autoCommit;
            rollback = tr.rollback;
            uid = tr.uid;
            readConstraints = (tr.readConstraints==null)?new SDict<>(u,true):
                    tr.readConstraints.Add(u, true);
        }
        public STransaction(STransaction tr,SRole r)
        {
            super(tr,tr.objects,r,tr.curpos);
            autoCommit = tr.autoCommit;
            rollback = tr.rollback;
            uid = tr.uid;
            readConstraints = tr.readConstraints; 
        }
        // Add a readConstraint : NB creates a new STransaction
        public STransaction Add(long u)
        {
            return new STransaction(this,u);
        }
        public STransaction Add(String s)
        {
            return (STransaction)New(objects,role.Add(uid,s),curpos);
        }
        public STransaction Add(long u,SDbObject ob)
        {
            return (STransaction)New(objects.Add(u,ob),role,curpos);
        }
        @Override
        public Serialisable _Get(long pos) {
            if (pos<0 || pos>=_uid)
                return objects.Lookup(pos);
            return super._Get(pos);
        }
        @Override
        protected SDatabase New(SDict<Long, SDbObject> o, SRole ro, long c)
        {
            STransaction r;
            try {
                r = new STransaction(this,o, ro, c);
            } catch (Exception e){ r = this; }
            return r;
        }
        @Override
        public String Name(long u)
        {
            return role.uids.get(u);
        }
        /// <summary>
        /// If there are concurrent transactions there will be more code here.
        /// </summary>
        /// <returns>the steps as modified by the commit process</returns>
        public SSlot<SDatabase,Long> Commit() throws Exception
        {
            SDatabase db = databases.Lookup(name);
            var ts = db.curpos;
            var f = new Writer(dbfiles.get(name));
            var rdr = new Reader(this);
            var tb = objects.PositionAt(_uid); // start of the work we want to commit
            var since = rdr.GetAll(f.length());
            for (SDbObject since1 : since) {
                if (since1.Check(readConstraints))
                    throw new Exception("Transaction conflict with read");
                for (org.shareabledata.Bookmark<org.shareabledata.SSlot<java.lang.Long, org.shareabledata.SDbObject>> b = objects.PositionAt(_uid); b != null; b = b.Next()) {
                    if (since1.Conflicts(db,this,b.getValue().val)) {
                        throw new Exception("Transaction conflict on " + b.getValue());
                    }
                }
            }
            synchronized (f.file)
            {
                db = databases.get(name).Load();
                ts = db.curpos;
                for (var b = tb; b != null; b = b.Next())
                {
                    var ov = b.getValue().val;
                    if (ov instanceof SRecord)
                    {
                        var sr = (SRecord)ov;
                        sr.CheckConstraints(db, (STable)objects.get(sr.table));
                    }
                    else if (ov instanceof SDelete)
                    {
                        var sd = (SDelete)ov;
                        sd.CheckConstraints(db, (STable)objects.get(sd.table));
                    }
                }
                since = rdr.GetAll(f.length());
                for (SDbObject since1 : since) {
                    if (since1.Check(readConstraints))
                        throw new Exception("Transaction conflict with read");
                    for (org.shareabledata.Bookmark<org.shareabledata.SSlot<java.lang.Long, org.shareabledata.SDbObject>> b = objects.PositionAt(_uid); b != null; b= b.Next()) {
                        if (since1.Conflicts(db,this,b.getValue().val)) {
                            throw new Exception("Transaction conflict on " + b.getValue());
                        }
                    }
                }
                db = f.Commit(db,this);
                f.CommitDone();
                Install(db);
            }
            return new SSlot(db,ts);
        }
        @Override
        public STransaction Transact(ReaderBase rdr,boolean auto)
        {
            rdr.db = this;
            return this; // ignore the parameter
        }
        public SSlot<SDatabase,Long> MaybeCommit() throws Exception
        {
            return autoCommit? Commit(): new SSlot(this,curpos);
        }
        @Override
        public SDatabase Rollback()
        {
            return rollback;
        }
        @Override
        public SDatabase Rdc(SIndex ix, SCList<Variant> _key)
        {
            if (_key.Length == 0)
                return new STransaction(this,ix.table);
            var mb = ix.rows.PositionAt(_key);
            if (mb == null)
                return this;
            if (mb.hasMore(this, ix.cols.Length))
                return new STransaction(this,ix.table);
            return new STransaction(this, (long)mb.getValue().val);
        }
        @Override
        public SDatabase Rdc(long uid)
        {
            return new STransaction(this,uid);
        }
    }

