using System;
using System.Collections.Generic;

namespace Shareable
{
    /// <summary>
    /// This class is not shareable
    /// </summary>
    public class Transaction : SDatabase
    {
        // uids above this number are for uncommitted objects
        public static readonly long _uid = 0x80000000;
        long uid;
        internal List<Serialisable> steps;
        public Transaction(SDatabase d) :base(d)
        {
            uid = _uid;
            steps = new List<Serialisable>();
        }
        /// <summary>
        /// This routine is public only for testing the transaction mechanism
        /// on non-database objects.
        /// Constructors of Database objects will internally call this method.
        /// </summary>
        /// <param name="s"></param>
        /// <returns>The transaction-based uid</returns>
        public long Add(Serialisable s)
        {
            steps.Add(s);
            return ++uid;
        }
        /// <summary>
        /// If there are concurrent transactions there will be more code here.
        /// </summary>
        /// <returns>the steps as modified by the commit process</returns>
        public Serialisable[] Commit()
        {
            var dbfile = dbfiles.Lookup(name);
            lock (dbfile.file)
            {
                var since = dbfile.GetAll(this, curpos);
                for (var i = 0; i < since.Length; i++)
                    for (var j = 0; j < steps.Count; j++)
                        if (since[i].Conflicts(steps[j]))
                            throw new Exception("Transaction Conflict on "+steps[j]);
                return dbfile.Commit(this,steps.ToArray());
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
    }
}
