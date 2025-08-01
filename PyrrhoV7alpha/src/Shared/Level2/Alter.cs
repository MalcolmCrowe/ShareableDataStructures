using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level2
{
    /// <summary>
    /// Alter records in the database are for modifying TableColumn information
    /// (There are other records for altering domains etc)
    /// Care: the Alter classes do not inherit from each other, but from the different PColumn classes
    /// </summary>
    internal class Alter : PColumn
    {
        /// <summary>
        /// Alter is obsolete: use Alter3.
        /// We remember the previous entry for this table column (the most recent Alter or the definition)
        /// </summary>
        public long _defpos;
        public override long defpos =>_defpos;
        /// <summary>
        /// Constructor: a new Alter record from the disk file
        /// </summary>
        /// <param name="bp">The buffer being read</param>
        /// <param name="pos">The defining position</param>
        public Alter(Reader rdr) : base(Type.Alter, rdr) { }
        protected Alter(Alter x, Writer wr) : base(x, wr)
        {
            _defpos = wr.cx.Fix(x.defpos);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Alter(this, wr);
        }
        /// <summary>
        /// Deserialise the Alter from the disk file buffer
        /// </summary>
        /// <param name="buf">The buffer</param>
        public override void Deserialise(Reader rdr)
        {
            var prev = rdr.GetLong();
            _defpos = rdr.Prev(prev) ?? ppos;
            base.Deserialise(rdr);
        }
        internal override DBObject? Install(Context cx)
        {
            var ro = cx.db.role;
            if (table == null)
                return null;
            table = (Table?)cx.db.objects[table.defpos];
            if (table == null)
                return null;
            if (table.infos[ro.defpos] is not ObInfo ti)
                throw new PEException("PE47120");
            ti += (ObInfo.SchemaKey, ppos);
            var tc = new TableColumn(table, this, dataType, cx);
            // the given role is the definer
            var priv = ti.priv & ~(Grant.Privilege.Delete | Grant.Privilege.GrantDelete);
            var ci = new ObInfo(name, priv);
            tc += (DBObject.Infos, new BTree<long, ObInfo>(ro.defpos, ci));
            tc += (TableColumn.Seq, seq);
            table += (cx, tc);
            tc = (TableColumn)(cx.obs[tc.defpos] ?? throw new DBException("42105").Add(Qlx.CREATE_GRAPH_TYPE_STATEMENT));
            seq = tc.seq;
            cx.Install(table);
            cx.db += ro;
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            cx.Install(tc);
            return tc;
        }
        /// <summary>
        /// Provide a string version of the Alter
        /// </summary>
        /// <returns>a string representation</returns>
        public override string ToString()
        {
            return "Alter TableColumn  [" + Pos(defpos) + "] to " + base.ToString();
        }
    }
    /// <summary>
    /// Alter records in the database are for modifying base column information
    /// (There are other records for altering domains etc)
    /// </summary>
    internal class Alter2 : PColumn2
    {
        /// <summary>
        /// Alter2 is obsolete - use Alter3
        /// We remember the previous entry for this TableColumn (the most recent Alter or the definition)
        /// </summary>
        public long _defpos;
        public override long defpos => _defpos;
        /// <summary>
        /// Constructor: a new Alter2 record from the disk
        /// </summary>
        /// <param name="bp">The buffer being read</param>
        /// <param name="pos">The defining position</param>
        public Alter2(Reader rdr) : base(Type.Alter2, rdr) { }
        protected Alter2(Alter2 x, Writer wr) : base(x, wr)
        {
            _defpos = wr.cx.Fix(x.defpos);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Alter2(this, wr);
        }
        /// <summary>
        /// Deserialise the Alter from the disk file buffer
        /// </summary>
        /// <param name="buf"></param>
        public override void Deserialise(Reader rdr)
        {
            var prev = rdr.GetLong();
            _defpos = rdr.Prev(prev) ?? -1L;
            base.Deserialise(rdr);
        }
        internal override DBObject? Install(Context cx)
        {
            var ro = cx.db.role;
            if (table == null)
                return null;
            table = (Table?)cx.db.objects[table.defpos];
            if (table == null)
                return null;
            if (table.infos[ro.defpos] is not ObInfo ti)
                throw new PEException("PE427121");
            ti += (ObInfo.SchemaKey, ppos);
            var tc = new TableColumn(table, this, dataType, cx);
            // the given role is the definer
            var priv = ti.priv & ~(Grant.Privilege.Delete | Grant.Privilege.GrantDelete);
            var ci = new ObInfo(name, priv);
            tc += (TableColumn.Seq, seq);
            table += (DBObject.Infos, new BTree<long, ObInfo>(ro.defpos, ci));
            table += (cx, tc);
            tc = (TableColumn)(cx.obs[tc.defpos] ?? throw new DBException("42105").Add(Qlx.CREATE_GRAPH_TYPE_STATEMENT));
            seq = tc.seq;
            cx.Install(table);
            cx.db += ro;
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            cx.Install(tc);
            return tc;
        }
    }
	/// <summary>
	/// Alter records in the database are for modifying base column information
    /// (There are other records for altering domains etc)
	/// </summary>
	internal class Alter3 : PColumn3
	{
		public long _defpos;
        public override long defpos => _defpos;
        /// <summary>
        /// Constructor: a new Alter record from the Parser
        /// </summary>
        /// <param name="co">The ident defining position</param>
        /// <param name="nm">The (new) table column name</param>
        /// <param name="sq">The (new) table column position (0,1,..)</param>
        /// <param name="tb">The table</param>
        /// <param name="dm">The (new) domain </param>
        /// <param name="db">The local database</param>
        public Alter3(long co, string nm, int sq, Table tb, Domain dm,
            string ds,bool opt,GenerationRule ge, TypedValue md, long pp, Context cx) :
            base(Type.Alter3, tb, nm, -1, dm, ds, opt, ge, md, pp, cx)
		{
            _defpos = co;
		}
        /// <summary>
        /// Constructor: a new Alter record from the disk
        /// </summary>
        /// <param name="bp">The buffer being read</param>
        /// <param name="pos">The defining position</param>
		public Alter3(Reader rdr) : base(Type.Alter3, rdr) { }
        protected Alter3(Alter3 x, Writer wr) : base(x, wr)
        {
            _defpos = wr.cx.Fix(x.defpos);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Alter3(this, wr);
        }
        /// <summary>
        /// Serialise the Alter to the disk
        /// </summary>
        /// <param name="r">Relocate some Position references</param>
        public override void Serialise(Writer wr)
		{
            wr.PutLong(defpos);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise the Alter from the disk file buffer
        /// </summary>
        /// <param name="rdr"></param>
        public override void Deserialise(Reader rdr) 
		{ 
			var previous = rdr.GetLong();
            _defpos = rdr.Prev(previous) ?? -1L;
			base.Deserialise(rdr);
		}
        internal override DBObject? Install(Context cx)
        {
            var ro = cx.db.role;
            if (table == null)
                return null;
            table = (Table?)cx.db.objects[table.defpos];
            if (table == null)
                return null;
            cx.obs += table.framing.obs;
            if (table.infos[ro.defpos] is not ObInfo ti)
                throw new PEException("PE47122");
            ti += (ObInfo.SchemaKey, ppos);
            var tc = new TableColumn(table, this, dataType, cx);
            cx.db += dataType;
            // the given role is the definer
            var priv = ti.priv & ~(Grant.Privilege.Delete | Grant.Privilege.GrantDelete);
            var oc = new ObInfo(name, priv);
            tc += (ro.defpos, oc);
            tc += (TableColumn.Seq, seq);
            cx.obs += (tc.defpos, tc);
            table += (cx, tc);
            tc = (TableColumn)(cx.obs[tc.defpos] ?? throw new DBException("42105").Add(Qlx.CREATE_GRAPH_TYPE_STATEMENT));
     //       seq = tc.seq;
            cx.Install(table);
            cx.db += ro;
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            cx.Install(tc);
            return tc;
        }
        /// <summary>
        /// Provide a string version of the Alter
        /// </summary>
        /// <returns></returns>
		public override string ToString() 
		{ 
			return "Alter3 TableColumn  ["+defpos+"] "+base.ToString(); 
		}
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            if (table == null)
                return new DBException("42105").Add(Qlx.CREATE_GRAPH_TYPE_STATEMENT);
            switch (that.type)
            {
                case Type.Alter3:
                    {
                        var a = (Alter3)that;
                        if (defpos == a.defpos || a.table==null ||
                            (table.defpos == a.table.defpos && name.CompareTo(a.name) == 0))
                            return new DBException("40032", table.defpos, that, ct);
                        break;
                    }
                case Type.Alter2:
                    {
                        var a = (Alter2)that;
                        if (defpos == a.defpos || a.table==null ||
                            (table.defpos == a.table.defpos && name.CompareTo(a.name) == 0))
                            return new DBException("40032", table.defpos, that, ct);
                        break;
                    }
                case Type.Alter:
                    {
                        var a = (Alter)that;
                        if (defpos == a.defpos || a.table==null ||
                            (table.defpos == a.table.defpos && name.CompareTo(a.name) == 0))
                            return new DBException("40032", table.defpos, that, ct);
                        break;
                    }
                case Type.PColumn3:
                case Type.PColumn2:
                case Type.PColumn:
                    {
                        var a = (PColumn)that;
                        if (a.table==null || table.defpos == a.table.defpos && name.CompareTo(a.name) == 0)
                            return new DBException("40045", DBObject.Uid(table.defpos), that, ct);
                        break;
                    }
                case Type.Record4:
                case Type.Record3:
                case Type.Record2:
                case Type.Record:
                    {
                        var r = (Record)that;
                        if (r.tabledefpos==table.defpos && r.fields.Contains(defpos)) 
                            return new DBException("40079", ppos, that, ct);
                        break;
                    }
                case Type.Update2:
                case Type.Update1:
                case Type.Update:
                    {
                        var r = (Update)that;
                        if (r.tabledefpos == table.defpos && r.fields.Contains(defpos))
                            return new DBException("40080", ppos, that, ct);
                        break;
                    }
                case Type.Drop:
                    {
                        var d = (Drop)that;
                        if (table.defpos == d.delpos || defpos == d.delpos) 
                            return new DBException("40010", ppos, that, ct);
                        break;
                    }
                case Type.PIndex1:
                case Type.PIndex2:
                case Type.PIndex:
                    {
                        var c = (PIndex)that;
                        if (table.defpos==c.tabledefpos)
                            for (int j = 0; j < c.columns.Length; j++)
                                if (c.columns[j] == defpos)
                                    return new DBException("40042", ppos, that, ct);
                        break;
                    }
                case Type.Grant:
                    {
                        var g = (Grant)that;
                        if (table.defpos == g.obj || defpos == g.obj)
                            return new DBException("40051", ppos, that, ct);
                        break;
                    }
                case Type.PCheck2:
                case Type.PCheck:
                    {
                        var c = (PCheck)that;
                        if (table.defpos == c.ckobjdefpos || defpos == c.ckobjdefpos) 
                            return new DBException("40077", ppos, that, ct);
                        break;
                    }
            }
            return base.Conflicts(db, cx, that, ct);
        }
	}
}
