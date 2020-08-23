using Pyrrho.Level4;
using Pyrrho.Level3;
using Pyrrho.Common;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.
namespace Pyrrho.Level2
{
    /// <summary>
    /// SQL2011 specifies Period Definitions for Tables
    /// Specifies a time period (-1 means SYSTEM_TIME or defpos) and start and end column
    /// </summary>
    internal class PPeriodDef : Physical
    {
        /// <summary>
        /// The defining position of this table
        /// </summary>
        public virtual long defpos { get { return ppos; } }
        /// <summary>
        /// The table this defeinition is for
        /// </summary>
        public long tabledefpos;
        /// <summary>
        /// The period name (e.g. "SYSTEM_TIME")
        /// </summary>
        public string periodname;
        /// <summary>
        /// The colum  with the starting time
        /// </summary>
        public long startcol;
        /// <summary>
        /// The colum with the ening time
        /// </summary>
        public long endcol;
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
        public PPeriodDef(long t, string p, long s, long e, long pp, Context cx)
            : base(Type.PeriodDef, pp, cx)
        {
            tabledefpos = t;
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
            tabledefpos = wr.Fix(x.tabledefpos);
            startcol = wr.Fix(x.startcol);
            endcol = wr.Fix(x.endcol);
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
            tabledefpos = wr.Fix(tabledefpos);
            wr.PutLong(tabledefpos);
            wr.PutString(periodname.ToString());
            startcol = wr.Fix(startcol);
            wr.PutLong(startcol);
            endcol = wr.Fix(endcol);
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
        public override DBException Conflicts(Database db, Context cx, Physical that,PTransaction ct)
        {
            switch(that.type)
            {
                case Type.PeriodDef:
                    if (tabledefpos == ((PPeriodDef)that).tabledefpos)
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.PTable1:
                case Type.PTable:
                    if (tabledefpos == ((PTable)that).defpos)
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.Alter3:
                    {
                        var a = (Alter3)that;
                        if (startcol == a.defpos || endcol == a.defpos)
                            return new DBException("40043", ppos, that, ct);
                        break;
                    }
                case Type.Alter2:
                    {
                        var a = (Alter2)that;
                        if (startcol == a.defpos || endcol == a.defpos)
                            return new DBException("40043", ppos, that, ct);
                        break;
                    }
                case Type.Alter:
                    {
                        var a = (Alter)that;
                        if (startcol == a.defpos || endcol == a.defpos)
                            return new DBException("40043", ppos, that, ct);
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
        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            var pd = new PeriodDef(ppos, tabledefpos, startcol, endcol,cx.db);
            var tb = (Table)cx.db.objects[tabledefpos];
            var priv = Grant.Privilege.Select | Grant.Privilege.GrantSelect;
            var oc = new ObInfo(ppos, periodname, Domain.Period)+(ObInfo.Privilege, priv);
            var oi = (ObInfo)ro.infos[tabledefpos];
            ro = ro + oc + (oi + (ppos,oc.domain));
            cx.db += (ro, p);
            cx.Install(pd, p);
            cx.db += (Database.Log, cx.db.log + (ppos, type));
        }
    }
}
