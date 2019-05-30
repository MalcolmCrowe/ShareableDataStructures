using System;

namespace Shareable
{
    public class STransaction :SDatabase 
    {
        // uids above this number are for uncommitted objects in this transaction
        // Note: uncommitted objects of any type are added to tr.objects 
        public static readonly long _uid = 0x4000000000000000;
        static object lck = new object();
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
        public (SDatabase,long) Commit(int cid)
        {
            SDatabase db = databases[name];
            var ts = db.curpos; // updated below
            var f = new Writer(dbfiles[name]);
            var rdr = new Reader(this);
            var pro = -1L;
            var tb = objects.PositionAt(_uid); // start of the work we want to commit
            for (var pb = tb; pb != null; pb = pb.Next())
                if (pb.Value.Item2 is SRecord pr)
                {
                    pro = pr.table;
                    break;
                }
            var since = rdr.GetAll(f.Length,rdr.limit);
            for (var i = 0; i < since.Length; i++)
            {
                var ck = since[i].Check(readConstraints);
                if (ck!=0)
                {
                    rconflicts++;
                    throw new TransactionConflict(pro,ck,0,"Transaction conflict "+ck+" with read");
                }
                for (var b = tb; b != null; b = b.Next())
                {
                    ck = since[i].Conflicts(db, this, b.Value.Item2);
                    if (ck!=0)
                    {
                        wconflicts++;
                        throw new TransactionConflict(pro,ck,b.Value.Item1,"Transaction conflict " + ck + " on " + b.Value);
                    }
                }
            }
            if (tb != null)
                lock (f.file)
                {
                    db = databases[name].Load();
                    ts = db.curpos;
                    for (var b = tb; b != null; b = b.Next())
                        if (b.Value.Item2 is SRecord sr)
                            sr.CheckConstraints(db, (STable)objects[sr.table]);
                        else if (b.Value.Item2 is SDelete sd)
                            sd.CheckConstraints(db, (STable)objects[sd.table]);
                    since = rdr.GetAll(f.Length, rdr.limit);
                    for (var i = 0; i < since.Length; i++)
                    {
                        var ck = since[i].Check(readConstraints);
                        if (ck != 0)
                        {
                            rconflicts++;
                            throw new TransactionConflict(pro, ck, 0, "Transaction conflict " + ck + " with read");
                        }
                        for (var b = tb; b != null; b = b.Next())
                        {
                            ck = since[i].Conflicts(db, this, b.Value.Item2);
                            if (ck != 0)
                            {
                                wconflicts++;
                                throw new TransactionConflict(pro, ck, b.Value.Item1, "Transaction conflict " + ck + " on " + b.Value);
                            }
                        }
                    }
                    db = f.Commit(db, this);
                    f.CommitDone();
                    Install(db);
                    //                Console.WriteLine("Commit " + ts + " to " + db.curpos+ " "+f.Length);
                }
            commits++;
            return (db,ts);
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
        /// <summary>
        /// Add in read constraints: a key specifies just one row as the read
        /// Constraint. Otherwise lock the entire table
        /// </summary>
        /// <param name="ix"></param>
        /// <param name="_key"></param>
        /// <returns></returns>
        public override SDatabase Rdc(SIndex ix, SCList<Variant> _key)
        {
            if (_key.Length == 0)
                return new STransaction(this,ix.table);
            var mb = ix.rows.PositionAt(_key);
            if (mb == null)
                return this;
            if (mb.hasMore(this, ix.cols.Length ?? 0))
                return new STransaction(this,ix.table);
            return new STransaction(this, mb.Value.Item2);
        }
        public override SDatabase Rdc(long uid)
        {
            return new STransaction(this,uid);
        }
        public override (SDatabase,long) MaybeAutoCommit(int c)
        { 
            return autoCommit ? Commit(c) : (this,curpos);
        }
        public override SDatabase Rollback()
        {
            return databases[name];
        }
    }
    public class TransactionConflict: StrongException
    {
        public TransactionConflict(long pr, long ck, long ob, string m)
            : base(m) { }
    }
}
