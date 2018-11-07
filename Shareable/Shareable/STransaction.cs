using System;
using System.Collections.Generic;

namespace Shareable
{
    public class STransaction :SDatabase 
    {
        // uids above this number are for uncommitted objects
        public static readonly long _uid = 0x80000000;
        public readonly long uid;
        public readonly bool autoCommit;
        public readonly SDatabase rollback;
        public readonly SDict<int,Serialisable> steps;
        protected override bool Committed => false;
        public STransaction(SDatabase d,bool auto) :base(d)
        {
            autoCommit = auto;
            rollback = (d is STransaction t)?t.rollback:d;
            uid = _uid;
            steps = SDict<int,Serialisable>.Empty;
        }
        /// <summary>
        /// This clever routine indirectly calls the protected SDtabase constructors
        /// that add new objects to the SDatabase (see the call to tr.Add).
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="s"></param>
        public STransaction(STransaction tr,Serialisable s) :base(tr.Add(s,tr.uid+1))
        {
            autoCommit = tr.autoCommit;
            rollback = tr.rollback;
            steps = tr.steps.Add(tr.steps.Count,s);
            uid =  tr.uid+1;
        }
        public SDatabase Commit()
        {
            AStream dbfile = dbfiles.Lookup(name);
            SDatabase db = databases.Lookup(name);
            var since = dbfile.GetAll(this, curpos, db.curpos);
            for (var i = 0; i < since.Length; i++)
                for (var b = steps.First(); b != null; b = b.Next())
                    if (since[i].Conflicts(b.Value.val))
                        throw new Exception("Transaction Conflict on " + b.Value);
            lock (dbfile.file)
            {
                since = dbfile.GetAll(this, db.curpos,dbfile.length);
                for (var i = 0; i < since.Length; i++)
                    for (var b = steps.First(); b != null; b = b.Next())
                        if (since[i].Conflicts(b.Value.val))
                            throw new Exception("Transaction Conflict on " + b.Value);
                db = dbfile.Commit(db,steps);
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
            if (uid > _uid)
                return "'" + (uid - _uid);
            return "" + uid;
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
