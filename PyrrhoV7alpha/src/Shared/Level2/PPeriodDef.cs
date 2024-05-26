using Pyrrho.Level4;
using Pyrrho.Level3;
using Pyrrho.Common;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level2
{
    /// <summary>
    /// SQL2011 specifies Period Definitions for Tables
    /// Specifies a time period (-1 means SYSTEM_TIME or defpos) and start and end column
    /// </summary>
    internal class PPeriodDef : Defined
    {
        /// <summary>
        /// The defining position of this table
        /// </summary>
        public virtual long defpos { get { return ppos; } }
        /// <summary>
        /// The table this defeinition is for
        /// </summary>
        public long tabledefpos = -1L;
        /// <summary>
        /// The period name (e.g. "SYSTEM_TIME")
        /// </summary>
        public string periodname = "";
        /// <summary>
        /// The colum  with the starting time
        /// </summary>
        public long startcol = -1L;
        /// <summary>
        /// The colum with the ening time
        /// </summary>
        public long endcol = -1L;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (defpos!=ppos && !Committed(wr,defpos)) return defpos;
            if (!Committed(wr,tabledefpos)) return tabledefpos;
            if (!Committed(wr,startcol)) return startcol;
            if (!Committed(wr,endcol)) return endcol;
            return -1;
        }
        /// <summary>
        /// Constructor: A PeriodDef from the Parser
        /// </summary>
        /// <param name="t">The table referred to</param>
        /// <param name="p">The period name (e.g. "SYSTEM_TIME")</param>
        /// <param name="s">The start column</param>
        /// <param name="e">The end column</param>
        /// <param name="wh">The PhysBase</param>
        /// <param name="curpos">The current position in the datafile</param>
        public PPeriodDef(Table tb, string p, long s, long e, long pp, Context cx)
            : base(Type.PeriodDef, pp, cx, "", Grant.AllPrivileges)
        {
            tabledefpos = tb.defpos;
            periodname = p;
            startcol = s;
            endcol = e;
        }
        /// <summary>
        /// Constructor: a PeriodDef from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
        public PPeriodDef(Reader rdr)
            : base(Type.PeriodDef, rdr)
        { }
        protected PPeriodDef(PPeriodDef x, Writer wr) : base(x, wr)
        {
            tabledefpos = wr.cx.Fix(x.tabledefpos);
            startcol = wr.cx.Fix(x.startcol);
            endcol = wr.cx.Fix(x.endcol);
            periodname = x.periodname;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PPeriodDef(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Reclocation information for positions</param>
        public override void Serialise(Writer wr)
        {
            tabledefpos = wr.cx.Fix(tabledefpos);
            wr.PutLong(tabledefpos);
            wr.PutString(periodname.ToString());
            startcol = wr.cx.Fix(startcol);
            wr.PutLong(startcol);
            endcol = wr.cx.Fix(endcol);
            wr.PutLong(endcol);
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            tabledefpos = rdr.GetLong();
            periodname = rdr.GetString();
            startcol = rdr.GetLong();
            endcol = rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override DBException? Conflicts(Database db, Context cx, Physical that,PTransaction ct)
        {
            switch(that.type)
            {
                case Type.PeriodDef:
                    if (tabledefpos == ((PPeriodDef)that).tabledefpos)
                        return new DBException("40032", tabledefpos, that, ct);
                    break;
                case Type.PTable1:
                case Type.PTable:
                    if (tabledefpos == ((PTable)that).defpos)
                        return new DBException("40032", tabledefpos, that, ct);
                    break;
                case Type.Alter3:
                    {
                        var a = (Alter3)that;
                        if (startcol == a.defpos || endcol == a.defpos)
                            return new DBException("40043", tabledefpos, that, ct);
                        break;
                    }
                case Type.Alter2:
                    {
                        var a = (Alter2)that;
                        if (startcol == a.defpos || endcol == a.defpos)
                            return new DBException("40043", tabledefpos, that, ct);
                        break;
                    }
                case Type.Alter:
                    {
                        var a = (Alter)that;
                        if (startcol == a.defpos || endcol == a.defpos)
                            return new DBException("40043", tabledefpos, that, ct);
                        break;
                    }
                case Type.Drop:
                    {
                        var t = (Drop)that;
                        if (t.delpos == tabledefpos)
                            return new DBException("40012", tabledefpos, that, ct);
                        if (t.delpos == startcol)
                            return new DBException("40013", startcol, that, ct);                        
                        if (t.delpos == endcol)
                            return new DBException("40013", endcol, that, ct);  
                        break;
                    }
            }
            return base.Conflicts(db, cx, that, ct);
        }
        internal override DBObject Install(Context cx)
        {
            var ro = cx.db.role;
            var pd = new PeriodDef(ppos, tabledefpos, startcol, endcol,cx.db);
            if (cx.db.objects[tabledefpos] is not Table tb || tb.infos[ro.defpos] is not ObInfo ti)
                throw new PEException("PE1439");
            ti += (ObInfo.SchemaKey, ppos);
            tb += (DBObject.Infos, tb.infos + (ro.defpos,ti));
            var priv = Grant.Privilege.Select | Grant.Privilege.GrantSelect;
            var oc = new ObInfo(periodname, priv);
            pd += (DBObject.Infos, new BTree<long, ObInfo>(ro.defpos, oc));
            ro += (periodname, ppos);
            cx.db = cx.db + tb + pd + ro;
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            cx.Install(pd);
            return tb;
        }
    }
}
