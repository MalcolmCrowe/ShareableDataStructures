using System;
using Pyrrho.Level1;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Common;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code 
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland
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
        public override long Dependent(Writer wr)
        {
            if (!Committed(wr,delpos)) return delpos;
            return -1;
        }
        /// <summary>
        /// Constructor: a Drop request from the Parser
        /// </summary>
        /// <param name="dp">The object to drop</param>
        /// <param name="db">The local database</param>
        public Drop(long dp,long u, Transaction db)
            : this(Type.Drop, dp, u, db)
		{
		}
        /// <summary>
        /// Constructor: a Drop request from the Parser
        /// </summary>
        /// <param name="t">The Drop type</param>
        /// <param name="tb">The PhysBase</param>
        /// <param name="ob">The defining position of the object being dropped</param>
        /// <param name="curpos">The current position in the datafile</param>
        protected Drop(Type t, long ob, long u,Transaction db)
            : base(t, u, db)
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
            delpos = wr.Fix(x.delpos);
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
        /// <summary>
        /// A readable version of the Drop request
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString() 
		{ 
			return GetType().Name+" ["+Pos(delpos)+"]"; 
		}
        public override long Conflicts(Database db, Transaction tr, Physical that)
        {
            switch(that.type)
            {
                case Type.Drop: return (delpos==((Drop)that).delpos)? ppos:-1;
                case Type.Change: return (delpos == ((Change)that).Affects) ? ppos : -1;
                case Type.Record3:
                case Type.Record2:
                case Type.Record1:
                case Type.Record:
                case Type.Update:
                    {
                        var r = (Record)that;
                        if (delpos == r.tabledefpos)
                            return ppos;
                        for (var b = r.fields.PositionAt(0); b != null; b = b.Next())
                            if (b.key() == delpos)
                                return ppos;
                        break;
                    }
                case Type.Delete: return (delpos == 
                        ((Record)db.GetD(((Delete)that).delpos)).tabledefpos) ? ppos : -1;
                case Type.PColumn3:
                case Type.PColumn2:
                case Type.PColumn: return (delpos == ((PColumn)that).tabledefpos) ? ppos : -1;
                case Type.Alter:
                    {
                        var a = (Alter)that;
                        return (delpos == a.tabledefpos || delpos == a.defpos) ? ppos : -1;
                    }
                case Type.PIndex2:
                case Type.PIndex1:
                case Type.PIndex:
                    {
                        var c = (PIndex)that;
                        if (delpos == c.tabledefpos || delpos == c.defpos || delpos == c.reference)
                            return ppos;
                        for (var i = 0; i < c.columns.Count; i++)
                            if (delpos == c.columns[i].defpos)
                                return ppos;
                        break;
                    }
                case Type.Grant: return (delpos == ((Grant)that).obj) ? ppos : -1;
                case Type.PCheck: return (delpos == ((PCheck)that).ckobjdefpos) ? ppos : -1;
                case Type.PMethod2:
                case Type.PMethod: return (delpos == ((PMethod)that).typedefpos) ? ppos : -1;
                case Type.Edit:
                case Type.PDomain: return (delpos == ((PDomain)that).Affects) ? ppos : -1;
                case Type.Modify: return (delpos == ((Modify)that).modifydefpos) ? ppos : -1;
                case Type.Ordering: return (delpos == ((Ordering)that).typedefpos) ? ppos : -1;
                case Type.PeriodDef:
                    {
                        var p = (PPeriodDef)that;
                        return (delpos == p.defpos || delpos == p.tabledefpos || delpos == p.startcol || delpos == p.endcol) ?
                            ppos : -1;
                    }
                case Type.Versioning: return (delpos == ((Versioning)that).perioddefpos) ? ppos : -1;
            }
            return base.Conflicts(db, tr, that);
        }
        /// <summary>
        /// A ReadCheck will occur on the dropped object
        /// </summary>
        /// <param name="pos">the defining position to check</param>
        /// <returns>whether a conflict has occurred</returns>
		public override DBException ReadCheck(long pos)
		{
			return (pos==delpos)?new DBException("40073",delpos).Mix():null;
		}

        internal override (Database, Role) Install(Database db, Role ro, long p)
        {
            var ob = (DBObject)db.objects[delpos];
            return (ob==null)?(db,ro):ob.Cascade(db,db,ro,dropAction);
        }
    }
    internal class Drop1 : Drop
    {
        public Drop1(long dp, Drop.DropAction a, long u, Transaction db) : base(Type.Drop1, dp, u, db) 
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
