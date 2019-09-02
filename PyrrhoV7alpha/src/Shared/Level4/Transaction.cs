using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;

namespace Pyrrho.Level4
{
    internal class Transaction : Database
    {
        internal override Role role => roles[context.role];
        internal override User user => (User)roles[context.user];
        internal List<Physical> physicals;
        internal ATree<string, Query> queries = BTree<string,Query>.Empty; // by blockid
        internal Context context;
        internal DBObject refObj;
        static long _uid = TransPos;
        long uid = ++_uid;
        /// <summary>
        /// Physicals will use virtual positions above the 16TB mark
        /// </summary>
        public const long TransPos = 0x400000000000;
        protected Transaction(ATree<long, Role> rs,Role ro,User us,long c,ATree<long,object>m)
            : base(rs, c, m)
        {
            context = new Context(ro,us);
        }
        public string Blockid()
        {
            return "B" + ++_uid;
        }
        public long NewUid()
        {
            return ++_uid;
        }
        public override Database New(ATree<long, DBObject> obs, long c)
        {
            return new Transaction(this,obs, c);
        }
        /// <summary>
        /// Set the given user and role (this is in a stack)
        /// </summary>
        /// <param name="db">The database</param>
        /// <param name="role">The role</param>
        /// <param name="user">The user</param>
        /// <returns>The previous Authority (for use with PopTo)</returns>
        internal Evaluation PushRole(Database db, long role, long user)
        {
            var r = _authority;
            _authority = new Evaluation(db.name, role, user, _authority);
            return r;
        }
        /// <summary>
        /// Restore a previous Authority i.e. user and role (from PushRole)
        /// </summary>
        /// <param name="a">The Authority to restore</param>
        internal void PopTo(Evaluation a)
        {
            if (a == null || _authority == null)
                throw new PEException("unreasonable authority state");
            _authority = _authority.PopTo(a);
        }
        public override void Audit(Audit a)
        {
            var wr = new Writer(df);
            lock(df)
                new Audit(a,wr);
        }
    }
}
