using System;
using System.Collections.Generic;

namespace Shareable
{
    public class STransaction :SDatabase 
    {
        // uids above this number are for uncommitted objects in this tranbsaction
        public static readonly long _uid = 0x40000000;
        public readonly long uid;
        public readonly bool autoCommit;
        public readonly SDatabase rollback;
        public readonly SDict<int,SDbObject> steps;
        internal override SDatabase _Rollback => rollback;
        protected override bool Committed => false;
        public STransaction Add(SDbObject s)
        {
            return new STransaction(this, s);
        }
        public STransaction(SDatabase d,bool auto) :base(d)
        {
            autoCommit = auto;
            rollback = d._Rollback;
            uid = _uid;
            steps = SDict<int,SDbObject>.Empty;
        }
        /// <summary>
        /// This clever routine indirectly calls the protected SDtabase constructors
        /// that add new objects to the SDatabase (see the call to tr._Add).
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="s"></param>
        STransaction(STransaction tr,SDbObject s) :base(tr._Add(s,tr.uid+1))
        {
            autoCommit = tr.autoCommit;
            rollback = tr.rollback;
            steps = tr.steps.Add(tr.steps.Length.Value,s);
            uid =  tr.uid+1;
        }
        public SDatabase Commit()
        {
            var f = dbfiles.Lookup(name);
            SDatabase db = databases.Lookup(name);
            var rdr = new Reader(f, curpos);
            var since = rdr.GetAll(db,db.curpos);
            for (var i = 0; i < since.Length; i++)
                for (var b = steps.First(); b != null; b = b.Next())
                    if (since[i].Conflicts(b.Value.val))
                        throw new Exception("Transaction Conflict on " + b.Value);
            lock (f)
            {
                since = rdr.GetAll(this, f.length);
                for (var i = 0; i < since.Length; i++)
                    for (var b = steps.First(); b != null; b = b.Next())
                        if (since[i].Conflicts(b.Value.val))
                            throw new Exception("Transaction Conflict on " + b.Value);
                db = f.Commit(db,steps);
            }
            Install(db);
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
        public override STransaction Transact(bool auto=true)
        {
            return this; // ignore the parameter
        }
        public override SDatabase Rollback()
        {
            return rollback;
        }
    }
}
