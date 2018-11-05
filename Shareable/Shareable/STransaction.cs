using System;
using System.Collections.Generic;

namespace Shareable
{
    public class STransaction :SDatabase 
    {
        // uids above this number are for uncommitted objects
        public static readonly long _uid = 0x80000000;
        public readonly long uid;
        public readonly SDatabase rollback;
        public readonly SDict<int,Serialisable> steps;
        public readonly int committed;
        public readonly SDict<long, long> uids; // old->new
        public override bool Contains(long pos)
        { 
            return uids.Contains(pos) || base.Contains(pos);
        }
        public override SDbObject Lookup(long pos)
        {
            if (uids.Contains(pos))
                pos = uids.Lookup(pos);
            return base.Lookup(pos);
        }
        public STransaction(SDatabase d) :base(d)
        {
            rollback = (d is STransaction t)?t.rollback:d;
            uid = _uid;
            steps = SDict<int,Serialisable>.Empty;
            committed = 0;
            uids = SDict<long, long>.Empty;
        }
        public STransaction(STransaction tr,Serialisable s) :base(tr.Add(s,tr.uid+1))
        {
            rollback = tr.rollback;
            steps = tr.steps.Add(tr.steps.Count,s);
            uid =  tr.uid+1;
            committed = tr.committed;
            uids = tr.uids;
        }
        public STransaction(STransaction tr,int c) :base(tr)
        {
            rollback = tr.rollback;
            steps = tr.steps;
            uid = tr.uid;
            committed = c;
            uids = tr.uids;
        }
        public STransaction(STransaction tr, STable t, long p) : base(tr,t,p)
        {
            rollback = tr.rollback;
            steps = tr.steps;
            uid = tr.uid;
            committed = tr.committed;
            uids = tr.uids;
        }
        public STransaction(STransaction tr, SAlter t, long p) : base(tr, t, p)
        {
            rollback = tr.rollback;
            steps = tr.steps;
            uid = tr.uid;
            committed = tr.committed;
            uids = tr.uids;
        }
        public STransaction(STransaction tr, SDrop t, long p) : base(tr, t, p)
        {
            rollback = tr.rollback;
            steps = tr.steps;
            uid = tr.uid;
            committed = tr.committed;
            uids = tr.uids;
        }
        public STransaction(STransaction tr, SView t, long p) : base(tr, t, p)
        {
            rollback = tr.rollback;
            steps = tr.steps;
            uid = tr.uid;
            committed = tr.committed;
            uids = tr.uids;
        }
        public STransaction(STransaction tr, int c, STable old, STable t,long p) :base(tr.Remove(old.uid),t,p)
        {
            rollback = tr.rollback;
            steps = tr.steps;
            uid = tr.uid;
            committed = c;
            uids = tr.uids.Add(old.uid,t.uid);
        }
        public STransaction(STransaction tr, int c, SAlter old, SAlter t, long p) : base(tr, t, p)
        {
            rollback = tr.rollback;
            steps = tr.steps;
            uid = tr.uid;
            committed = c;
            uids = tr.uids.Add(old.uid, t.uid);
        }
        public STransaction(STransaction tr, int c, SDrop old, SDrop t, long p) : base(tr, t, p)
        {
            rollback = tr.rollback;
            steps = tr.steps;
            uid = tr.uid;
            committed = c;
            uids = tr.uids.Add(old.uid, t.uid);
        }
        public STransaction(STransaction tr, int c, SView old, SView t, long p) : base(tr, t, p)
        {
            rollback = tr.rollback;
            steps = tr.steps;
            uid = tr.uid;
            committed = c;
            uids = tr.uids.Add(old.uid, t.uid);
        }
        public STransaction(STransaction tr, int c, SIndex old, SIndex t, long p) : base(tr.Remove(old.uid), t, p)
        {
            rollback = tr.rollback;
            steps = tr.steps;
            uid = tr.uid;
            committed = c;
            uids = tr.uids.Add(old.uid, t.uid);
        }
        public STransaction(STransaction tr, int n, STable t, SColumn old, SColumn c,long p) :base(tr, t.Remove(old.uid).Add(c),p)
        {
            rollback = tr.rollback;
            steps = tr.steps;
            uid = tr.uid;
            committed = n;
            uids = tr.uids.Add(old.uid,c.uid);
        }
        public STransaction(STransaction tr, int n,STable t,SRecord old, SRecord r,long p) :base(tr,t.Remove(old.uid).Add(r),p)
        {
            rollback = tr.rollback;
            steps = tr.steps;
            uid = tr.uid;
            committed = n;
            uids = tr.uids.Add(old.uid, r.uid);
        }
        public STransaction(STransaction tr, int n,STable t,SDelete d,long p) :base(tr,t.Remove(d.delpos),p)
        {
            rollback = tr.rollback;
            steps = tr.steps;
            uid = tr.uid;
            committed = n;
            uids = tr.uids;
        }
        /// <summary>
        /// If there are concurrent transactions there will be more code here.
        /// </summary>
        /// <returns>the steps as modified by the commit process</returns>
        public STransaction Commit()
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
        protected override SDatabase Install(STable t,long p)
        {
            return new STransaction(this,t,p);
        }
        protected override SDatabase Install(SColumn c,long p)
        {
            return new STransaction(this,((STable)Lookup(c.table)).Add(c),p);
        }
        protected override SDatabase Install(SRecord r,long p)
        {
            return new STransaction(this,((STable)Lookup(r.table)).Add(r),p);
        }
        protected override SDatabase Install(SDelete d, long p)
        {
            return new STransaction(this,((STable)Lookup(d.table)).Remove(d.delpos),p);
        }
        protected override SDatabase Install(SAlter a, long p)
        {
            return new STransaction(this, a, p);
        }
        protected override SDatabase Install(SDrop d, long p)
        {
            return new STransaction(this, d, p);
        }
        protected override SDatabase Install(SView v, long p)
        {
            return new STransaction(this, v, p);
        }
        public override STransaction Transact()
        {
            return this;
        }
        public override STransaction MaybeAutoCommit(STransaction tr)
        {
            return tr;
        }
        public override SDatabase Rollback()
        {
            return rollback;
        }
    }
}
