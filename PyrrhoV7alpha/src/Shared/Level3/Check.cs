using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

namespace Pyrrho.Level3
{
    /// <summary>
    /// Database Object for Check constraints
    /// Immutable
    /// </summary>
    internal class Check : DBObject
    {
        internal const long
            Condition = -51, // long SqlValue
            Source = -52; // string
        /// <summary>
        /// The object to which the check applies
        /// </summary>
        internal long checkobjpos => (long)mem[From.Target];
        public string name => (string)mem[Name] ?? "";
        /// <summary>
        /// The source SQL for the check constraint
        /// </summary>
        internal string source => (string)mem[Source];
        internal long search => (long)(mem[Condition]??-1L);
        /// <summary>
        /// Constructor: from the level 2 information
        /// </summary>
        /// <param name="c">The PCheck</param>
        /// <param name="definer">The defining role</param>
        /// <param name="owner">the owner</param>
		public Check(PCheck c, Database db) 
            : base(c.name, c.ppos, c.ppos, db.role.defpos,BTree<long,object>.Empty
                  + (From.Target,c.ckobjdefpos)+(Source,c.check)
                  + (Condition, c.test)+(Framing,c.framing))
        { }
        public Check(PCheck2 c, Database db)
            : base(c.name, c.ppos, c.ppos, db.role.defpos, BTree<long, object>.Empty
          + (From.Target, c.subobjdefpos) + (Source, c.check)
          + (Condition, c.test) + (Framing, c.framing))
        { }
        /// <summary>
        /// for system types
        /// </summary>
        /// <param name="dp"></param>
        /// <param name="s"></param>
        public Check(long dp,string s)
            : base(dp,new BTree<long,object>(Source,s)+(Condition,
                  new Parser(Database._system).ParseSqlValue(s,Domain.Bool.defpos))) { }
        /// <summary>
        /// Constructor: copy with changes
        /// </summary>
        /// <param name="c">The check</param>
        /// <param name="us">The new list of grantees (including ownership)</param>
        /// <param name="ow">the owner</param>
        protected Check(long dp, BTree<long, object> m) : base(dp, m) { }
        public static Check operator+(Check c,(long,object)x)
        {
            return (Check)c.New(c.mem + x);
        }
        /// <summary>
        /// a readable version of the object
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" From.Target="); sb.Append(Uid(checkobjpos));
            sb.Append(" Source="); sb.Append(source);
            return sb.ToString();
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Check(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new Check(dp, mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = (Check)base._Relocate(wr);
            var s = (SqlValue)wr.Fixed(search);
            if (s.defpos != search)
                r += (Condition, s);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (Check)base._Relocate(cx);
            var s = (SqlValue)cx.Fixed(search);
            if (s.defpos != search)
                r += (Condition, s);
            return r;
        }
        internal override Database Drop(Database d, Database nd, long p)
        {
            nd = ((DBObject)nd.objects[checkobjpos]).DropCheck(defpos, nd, p);
            return base.Drop(d, nd, p);
        }
    }
}
