using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
 
namespace Pyrrho.Level3
{
    /// <summary>
    /// Database Object for CheckFields constraints
    /// Immutable
    /// shareable
    /// </summary>
    internal class Check : Domain
    {
        internal const long
            Condition = -51, // long QlValue
            Source = -52; // string
        /// <summary>
        /// The object to which the check applies
        /// </summary>
        internal long checkobjpos => (long)(mem[RowSet.Target]??-1L);
        /// <summary>
        /// The source SQL for the check constraint
        /// </summary>
        internal string source => (string?)mem[Source]??"";
        public new string? name => (string?)mem[ObInfo.Name]; // constraints cannot be renamed
        internal long search => (long)(mem[Condition]??-1L);
        /// <summary>
        /// Constructor: from the level 2 information
        /// </summary>
        /// <param name="c">The PCheck</param>
        /// <param name="definer">The defining role</param>
        /// <param name="owner">the owner</param>
		public Check(PCheck c, Database db) 
            : base(c.ppos, Bool.mem
                  + (RowSet.Target,c.ckobjdefpos)+(Source,c.check ?? "")
                  + (Owner, c.owner) + (Definer,c.definer)
                  + (Infos, c.infos) +(ObInfo.Name,c.name)
                  + (Condition, c.test)+(_Framing,c.framing)+(LastChange,c.ppos)
                  + (ObInfo.Name,c.name??""))
        { }
        public Check(PCheck2 c, Database db)
            : base(c.ppos, Bool.mem  + (Owner, c.owner) + (Definer, c.definer)
                  + (Infos, c.infos) + (ObInfo.Name, c.name)
          + (RowSet.Target, c.subobjdefpos) + (Source, c.check ?? "")
          + (Condition, c.test) + (_Framing, c.framing)+(LastChange,c.ppos)
          + (ObInfo.Name, c.name ?? ""))
        { }
        /// <summary>
        /// Constructor: copy with changes
        /// </summary>
        /// <param name="c">The check</param>
        /// <param name="us">The new tree of grantees (including ownership)</param>
        /// <param name="ow">the owner</param>
        protected Check(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Database db)
        {
            var ro = db.role ?? throw new DBException("42105").Add(Qlx.ROLE);
            return BTree<long, object>.Empty + (Definer, ro.defpos) + (Owner,db.user?.defpos??-501L);
        }
        public static Check operator+(Check c,(long,object)x)
        {
            return (c.mem[x.Item1] == x.Item2)?c:(Check)c.New(c.mem + x);
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
            sb.Append(" Search="); sb.Append(Uid(search));
            return sb.ToString();
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Check(defpos,m);
        }
        internal override DBObject New(long dp,BTree<long,object>m)
        {
            return new Check(dp,m);
        }
        protected override BTree<long,object> _Fix(Context cx,BTree<long,object>m)
        {
            var r = base._Fix(cx,m);
            var ns = cx.Fix(search);
            if (ns!=search)
                r += (Condition, ns);
            return r;
        }
        internal override DBObject Apply(Context cx, Domain dm)
        {
            cx.Add(framing);
            var f = ObTree.Empty;
            if (cx.obs[search] is QlValue se)
                f = se._Apply(cx, dm, f);
            var r = this + (_Framing, framing + (Framing.Obs, f));
            cx.Add(r);
            cx.Add(r.framing);
            cx.db += r;
            return r;
        }
        internal override Database Drop(Database d, Database nd)
        {
            if (nd.objects[checkobjpos] is DBObject ob)
                nd = ob.DropCheck(defpos, nd);
            for (var b = d.roles.First(); b != null; b = b.Next())
                if (b.value() is long bp && d.objects[bp] is Role ro 
                    && infos[ro.defpos] is ObInfo oi && oi.name is not null)
                {
                    ro += (Role.DBObjects, ro.dbobjects - oi.name);
                    nd += ro;
                }
            return base.Drop(d, nd);
        }
        internal override void Note(Context cx, StringBuilder sb, string pre="/// ")
        {
            sb.Append("// Check "); sb.Append(source); 
        }
    }
}
