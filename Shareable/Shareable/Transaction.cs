using System;
using System.Collections.Generic;

namespace Shareable
{
    public class Transaction : SDatabase
    {
        // uids above this number are for uncommitted objects
        public static readonly long _uid = 0x80000000;
        public readonly long uid;
        public readonly SDict<int,Serialisable> steps;
        public readonly int committed;
        public readonly SDict<long, long> uids; // old->new
        public Transaction(SDatabase d) :base(d)
        {
            uid = _uid;
            steps = SDict<int,Serialisable>.Empty;
            committed = 0;
            uids = SDict<long, long>.Empty;
        }
        /// <summary>
        /// This routine is public only for testing the transaction mechanism
        /// on non-database objects.
        /// </summary>
        public Transaction(Transaction tr,Serialisable s) :base(tr)
        {
            steps = tr.steps.Add(tr.steps.Count,s);
            uid =  tr.uid+1;
            committed = tr.committed;
            uids = tr.uids;
        }
        public Transaction(Transaction tr,int c) :base(tr)
        {
            steps = tr.steps;
            uid = tr.uid;
            committed = c;
            uids = tr.uids;
        }
        public Transaction(Transaction tr, int c, STable old, STable t) :base(tr,t)
        {
            steps = tr.steps;
            uid = tr.uid;
            committed = c;
            uids = tr.uids.Add(old.uid,t.uid);
        }
        public Transaction(Transaction tr, int n, STable t, SColumn old, SColumn c) :base(tr, t.Add(c))
        {
            steps = tr.steps;
            uid = tr.uid;
            committed = n;
            uids = tr.uids.Add(old.uid,c.uid);
        }
        public Transaction(Transaction tr, int n,STable t,SRecord old, SRecord r) :base(tr,t.Add(r))
        {
            steps = tr.steps;
            uid = tr.uid;
            committed = n;
            uids = tr.uids.Add(old.uid, r.uid);
        }
        public Transaction(Transaction tr, int n,STable t,SDelete d) :base(tr,t.Remove(d.delpos))
        {
            steps = tr.steps;
            uid = tr.uid;
            committed = n;
            uids = tr.uids;
        }
        /// <summary>
        /// If there are concurrent transactions there will be more code here.
        /// </summary>
        /// <returns>the steps as modified by the commit process</returns>
        public Transaction Commit()
        {
            var dbfile = dbfiles.Lookup(name);
            lock (dbfile.file)
            {
                var since = dbfile.GetAll(this, curpos);
                for (var i = 0; i < since.Length; i++)
                    for (var b=steps.First();b!=null;b=b.Next())
                        if (since[i].Conflicts(b.Value.val))
                            throw new Exception("Transaction Conflict on "+b.Value);
                return dbfile.Commit(this);
            }
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
        internal long Fix(long uid)
        {
            return (uids.Contains(uid)) ? uids.Lookup(uid) : uid;
        }
        protected override SDatabase Install(STable t)
        {
            return new Transaction(this, t);
        }
        protected override SDatabase Install(SColumn c)
        {
            return new Transaction(this, tables.Lookup(c.table).Add(c));
        }
        protected override SDatabase Install(SRecord r)
        {
            return new Transaction(this, tables.Lookup(r.table).Add(r));
        }
        protected override SDatabase Install(SDelete d)
        {
            return new Transaction(this, tables.Lookup(d.table).Remove(d.delpos));
        }
    }
}
