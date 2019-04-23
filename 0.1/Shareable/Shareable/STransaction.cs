using System;

namespace Shareable
{
    public class STransaction :SDatabase 
    {
        // uids above this number are for uncommitted objects in this transaction
        // Note: uncommitted objects of any type are added to tr.objects 
        public static readonly long _uid = 0x4000000000000000;
        public readonly long uid;
        public readonly bool autoCommit;
        public readonly SDict<long, bool> readConstraints;
        protected override bool Committed => false;
        internal STransaction(SDatabase d,ReaderBase rdr,bool auto) :base(d)
        {
            autoCommit = auto;
            uid = _uid;
            readConstraints = SDict<long, bool>.Empty;
            rdr.db = this;
        }
        /// <summary>
        /// Some other set of updates to existing (and maybe named) objects 
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="obs"></param>
        /// <param name="nms"></param>
        protected STransaction(STransaction tr, SDict<long,SDbObject> obs,SRole r,long c)
            : base(tr, obs, r, c)
        {
            autoCommit = tr.autoCommit;
            uid = tr.uid + 1;
            readConstraints = tr.readConstraints;
        }
        protected STransaction(STransaction tr,long u) :base(tr)
        {
            autoCommit = tr.autoCommit;
            uid = tr.uid;
            readConstraints = tr.readConstraints + (u, true);
        }
        public STransaction(STransaction tr, SRole r) : base(tr, tr.objects, r, tr.curpos)
        {
            autoCommit = tr.autoCommit;
            uid = tr.uid;
            readConstraints = tr.readConstraints;
        }
        public static STransaction operator+(STransaction tr,long uid)
        {
            return new STransaction(tr, uid);
        }
        public static STransaction operator+(STransaction tr, string s)
        {
            return (STransaction)tr.New(tr.objects, tr.role + (tr.uid, s), tr.curpos);
        }
        public static STransaction operator +(STransaction tr, (long,SDbObject) s)
        {
            return (STransaction)tr.New(tr.objects+s, tr.role, tr.curpos);
        }
        protected override SDatabase New(SDict<long, SDbObject> o, SRole r, long c)
        {
            return new STransaction(this,o, r, c);
        }
        protected override Serialisable _Get(long pos)
        {
            if (pos < 0 || pos >= _uid)
                return objects[pos];
            return base._Get(pos);
        }
        public SDatabase Commit()
        {
            SDatabase db = databases[name];
            var f = new Writer(dbfiles[name]);
            var rdr = new Reader(this);
            var tb = objects.PositionAt(_uid); // start of the work we want to commit
            var since = rdr.GetAll(f.Length);
            for (var i = 0; i < since.Length; i++)
            {
                var ck = since[i].Check(readConstraints);
                if (ck!=0)
                {
                    rconflicts++;
                    throw new TransactionConflict("Transaction conflict "+ck+" with read");
                }
                for (var b = tb; b != null; b = b.Next())
                {
                    ck = since[i].Conflicts(db, this, b.Value.Item2);
                    if (ck!=0)
                    {
                        wconflicts++;
                        throw new TransactionConflict("Transaction conflict " + ck + " on " + b.Value);
                    }
                }
            }  
            if (tb!=null)
                lock (f)
                {
                    since = rdr.GetAll(f.Length);
                    for (var i = 0; i < since.Length; i++)
                    {
                        var ck = since[i].Check(readConstraints);
                        if (ck != 0)
                        {
                            rconflicts++;
                            throw new TransactionConflict("Transaction conflict " + ck + " with read");
                        }
                        for (var b = tb; b != null; b = b.Next())
                        {
                            ck = since[i].Conflicts(db, this, b.Value.Item2);
                            if (ck != 0)
                            {
                                wconflicts++;
                                throw new TransactionConflict("Transaction conflict " + ck + " on " + b.Value);
                            }
                        }
                    }
                    var ts = f.Length;
                    db = f.Commit(db, this);
                    f.CommitDone();
                    Install(db);
  //                 Console.WriteLine("Commit " + ts + " to " + db.curpos+ " "+f.Length);
                }
            commits++;
            return db;
        }
        /// <summary>
        /// We will single-quote transaction-local uids
        /// </summary>
        /// <returns>a more readable version of the uid</returns>
        internal static string Uid(long uid)
        {
            return SDbObject._Uid(uid);
        }
        public override STransaction Transact(ReaderBase rdr, bool auto=true)
        {
            rdr.db = this;
            return this; // ignore the parameter
        }
        public override SDatabase Rollback()
        {
            return databases[name];
        }
    }
    public class TransactionConflict: StrongException
    {
        public TransactionConflict(string m):base(m) { }
    }
}
