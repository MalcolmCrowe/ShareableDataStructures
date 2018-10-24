using System.Collections.Generic;

namespace Shareable
{
    /// <summary>
    /// This class is not shareable
    /// </summary>
    public class Transaction
    {
        public static readonly long _uid = 0x80000000;
        long uid;
        AStream dbfile;
        List<Serialisable> steps;
        public Transaction(AStream f)
        {
            uid = _uid;
            dbfile = f;
            steps = new List<Serialisable>();
        }
        /// <summary>
        /// This routine is public only for testing the transaction mechanism
        /// on non-database objects.
        /// Database objects will be added to the transaction by their
        /// constructors.
        /// </summary>
        /// <param name="s"></param>
        public long Add(Serialisable s)
        {
            steps.Add(s);
            return ++uid;
        }
        public Serialisable[] Commit()
        {
            return dbfile.Commit(steps.ToArray());
        }
        public static bool Committed(long uid)
        {
            return uid < _uid;
        }
        public static string Pos(long uid)
        {
            if (uid > _uid)
                return "'" + (uid - _uid);
            return "" + uid;
        }
    }
}
