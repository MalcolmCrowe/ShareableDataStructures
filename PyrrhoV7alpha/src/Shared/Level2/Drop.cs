using System;
using Pyrrho.Level1;
using Pyrrho.Level3;
using Pyrrho.Level4;
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
	/// A Drop request is used for dropping database objects
	/// </summary>
	internal class Drop : Physical
	{
        /// <summary>
        /// The defining position of the object to drop
        /// </summary>
		public long delpos;
        public enum DropAction { Restrict=0,Null=1,Default=2,Cascade=3}
        public DropAction dropAction=DropAction.Restrict;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr,delpos)) return delpos;
            return -1;
        }
        /// <summary>
        /// Constructor: a Drop request from the Parser
        /// </summary>
        /// <param name="dp">The object to drop</param>
        /// <param name="db">The local database</param>
        public Drop(long dp,long pp)
            : this(Type.Drop, dp, pp)
		{
		}
        /// <summary>
        /// Constructor: a Drop request from the Parser
        /// </summary>
        /// <param name="t">The Drop type</param>
        /// <param name="tb">The PhysBase</param>
        /// <param name="ob">The defining position of the object being dropped</param>
        /// <param name="curpos">The current position in the datafile</param>
        protected Drop(Type t, long ob, long pp)
            : base(t, pp)
		{
			delpos = ob;
		}
        /// <summary>
        /// Constructor: a Drop request from the buffer
        /// </summary>
        /// <param name="bp">the buffer</param>
        /// <param name="pos">the defining position</param>
		public Drop(Reader rdr) : base (Type.Drop,rdr) {}
        protected Drop(Type t, Reader rdr) : base(t, rdr) { }
        protected Drop(Drop x, Writer wr) : base(x, wr)
        {
            delpos = wr.cx.Fix(x.delpos);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Drop(this, wr);
        }
        /// <summary>
        /// The Affected object is the object being dropped
        /// </summary>
		public override long Affects
		{
			get
			{
				return delpos;
			}
		}
        /// <summary>
        /// Serialise this to the database
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr)
		{
            wr.PutLong(delpos);
			base.Serialise(wr);
		}
        /// <summary>
        /// deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr) 
		{ 
			delpos = rdr.GetLong();
			base.Deserialise(rdr);
		}
        internal long _Tbl()
        {
            return delpos;
        }
        /// <summary>
        /// A readable version of the Drop request
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString() 
		{ 
			return GetType().Name+" ["+Pos(delpos)+"]"; 
		}
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch(that.type)
            {
                case Type.Drop1:
                case Type.Drop: if (delpos==((Drop)that).delpos)
                        return new DBException("40010", delpos, that, ct);
                    break;
                case Type.Change: if (delpos == ((Change)that).Affects)
                        return new DBException("40047", delpos, that, ct);
                    break;
                case Type.Record3:
                case Type.Record2:
                case Type.Record:
                case Type.Update:
                case Type.Update1:
                    {
                        var r = (Record)that;
                        if (delpos == r.tabledefpos)
                            return new DBException("40055", delpos, that, ct);
                        for (var b = r.fields.PositionAt(0); b != null; b = b.Next())
                            if (b.key() == delpos)
                                return new DBException("40055", delpos, that, ct);
                        break;
                    }
                case Type.Delete1:
                case Type.Delete: 
                    if (db.GetD(((Delete)that).delpos) is Delete td && td.tabledefpos == delpos)
                        return new DBException("40057", delpos, that, ct);
                    break;
                case Type.PColumn3:
                case Type.PColumn2:
                case Type.PColumn: if (delpos == ((PColumn)that).table?.defpos)
                        return new DBException("40043", delpos, that, ct);
                    break;
                case Type.Alter3:
                    {
                        var a = (Alter3)that;
                        if (delpos == a.defpos || delpos == a.table?.defpos)
                            return new DBException("40043", ppos, that, ct);
                        break;
                    }
                case Type.Alter2:
                    {
                        var a = (Alter2)that;
                        if (delpos == a.defpos || delpos == a.table?.defpos)
                            return new DBException("40043", ppos, that, ct);
                        break;
                    }
                case Type.Alter:
                    {
                        var a = (Alter)that;
                        if (delpos == a.table?.defpos || delpos == a.defpos)
                            return new DBException("40043", delpos, that, ct);
                        break;
                    }
                case Type.PIndex2:
                case Type.PIndex1:
                case Type.PIndex:
                    {
                        var c = (PIndex)that;
                        if (delpos == c.tabledefpos || delpos == c.defpos || delpos == c.reference)
                            return new DBException("40058", delpos, that, ct);
                        for (var i = 0; i < c.columns.Length; i++)
                            if (delpos == c.columns[i])
                                return new DBException("40058", delpos, that, ct);
                        break;
                    }
                case Type.Grant: if (delpos == ((Grant)that).obj)
                        return new DBException("40051", delpos, that, ct);
                    break;
                case Type.PCheck: if (delpos == ((PCheck)that).ckobjdefpos)
                        return new DBException("40059", delpos, that, ct);
                    break;
                case Type.PMethod2:
                case Type.PMethod: if (delpos == ((PMethod)that).udt?.defpos)
                        return new DBException("40060", delpos, that, ct);
                    break;
                case Type.Edit:
                case Type.PDomain: if (delpos == ((PDomain)that).Affects)
                        return new DBException("40068", delpos, that, ct);
                    break;
                case Type.Modify: if (delpos == ((Modify)that).modifydefpos)
                        return new DBException("40069", delpos, that, ct);
                    break;
                case Type.Ordering: if (delpos == db.Find(((Ordering)that).domain)?.defpos)
                        return new DBException("40049", delpos, that, ct);
                    break;
                case Type.PeriodDef:
                    {
                        var p = (PPeriodDef)that;
                        if (delpos == p.defpos || delpos == p.tabledefpos || delpos == p.startcol || delpos == p.endcol) 
                            return new DBException("40077", delpos, that, ct);
                        break;
                    }
                case Type.Versioning: if (delpos == ((Versioning)that).perioddefpos)
                        return new DBException("40072", delpos, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that, ct);
        }
        /// <summary>
        /// A ReadCheck will occur on the dropped object
        /// </summary>
        /// <param name="pos">the defining position to check</param>
        /// <returns>whether a conflict has occurred</returns>
		public override DBException? ReadCheck(long pos,Physical ph,PTransaction ct)
		{
			return (pos==delpos)?new DBException("40073",delpos,ph,ct).Mix():null;
		}

        internal override DBObject? Install(Context cx, long p)
        {
            if (cx.db != null && cx.db.objects[delpos] is DBObject ob)
            {
                cx.db = ob.Drop(cx.db, cx.db, p);
                if (cx.db.mem.Contains(Database.Log))
                    cx.db += (Database.Log, cx.db.log + (ppos, type));
            }
            cx.obs -= delpos;
            return null;
        }
    }
    internal class Drop1 : Drop
    {
        public Drop1(long dp, DropAction a, long pp)
            : base(Type.Drop1, dp, pp) 
        {
            dropAction = a;
        }
        public Drop1(Reader rdr) : base(Type.Drop1, rdr) { }
        public Drop1(Drop1 d, Writer wr) : base(d, wr) 
        {
            dropAction = d.dropAction;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Drop1(this, wr);
        }
        public override void Serialise(Writer wr)
        {
            wr.WriteByte((byte)dropAction);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            dropAction = (DropAction)rdr.ReadByte();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return base.ToString() + " " + dropAction.ToString();
        }
    }
}
