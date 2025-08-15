using System;
using Pyrrho.Common;
using Pyrrho.Level1;
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
	/// The Change record in the database is used for renaming of objects.
    /// Most object names are role-specific
	/// </summary>
	internal class Change : Defined
	{
        public long defpos;
        /// <summary>
        /// Constructor: a new Change object from the Parser
        /// </summary>
        /// <param name="pt">The defining position for this object</param>
        /// <param name="nm">The (new) name</param>
        /// <param name="pp">The transaction position of this Physical</param>
        /// <param name="cx">The Context</param>
        public Change(long pt, string nm, long pp, Context cx)
            : this(Type.Change, pt, nm, pp, cx)
		{  }
        /// <summary>
        /// Constructor: a new Change object from the Parser
        /// </summary>
        /// <param name="t">The Change type</param>
        /// <param name="pt">The defining position for this object</param>
        /// <param name="nm">The (new) name</param>
        /// <param name="pp">The transaction position of this Physical</param>
        /// <param name="cx">The Context</param>
        protected Change(Type t, long pt, string nm, long pp, Context cx)
			:base(t,pp,cx,nm,cx.Priv(pt))
		{
            defpos = pt;
		}
        /// <summary>
        /// Constructor: a new Change object from the buffer
        /// </summary>
        /// <param name="rdr">the Reader for the file</param>
		public Change(Reader rdr) :base(Type.Change,rdr) { }
        /// <summary>
        /// Commit this Physical to the database file
        /// </summary>
        /// <param name="x">The transaction version of the Physical</param>
        /// <param name="wr">The Writer for the file</param>
        protected Change(Change x, Writer wr) : base(x, wr)
        {
            defpos = wr.cx.Fix(x.defpos);
            name = x.name;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Change(this, wr);
        }
        /// <summary>
        /// The object affected (being renamed)
        /// </summary>
		public override long Affects
		{
			get
			{
                return defpos;
			}
		}
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for Positions</param>
        public override void Serialise(Writer wr)
		{
            wr.PutLong(defpos);
            wr.PutString(name);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">The buffer</param>
        public override void Deserialise(Reader rdr) 
		{ 
			defpos = rdr.GetLong();
			name = rdr.GetString();
			base.Deserialise(rdr);
		}
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>A string representation</returns>
		public override string ToString() 
		{ 
			return "Change "+Pos(defpos)+" to "+name; 
		}
        /// <summary>
        /// During the validation step of the transaction, we review Physical records that
        /// have been committed by other threads since the start of our transaction.
        /// If any conflict with our changes, return an exception.
        /// </summary>
        /// <param name="db">The Database</param>
        /// <param name="cx">The Context</param>
        /// <param name="that">A possibly conflicting transaction</param>
        /// <param name="ct">The enclosing Transaction</param>
        /// <returns>The exceptio n to be raised if any</returns>
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch(that.type)
            {
                case Type.PTable1:
                case Type.PTable: if (name.CompareTo(((PTable)that).name) == 0)
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.PDomain1:
                case Type.PDomain: if (name.CompareTo(((PDomain)that).name) == 0)
                        return new DBException("40022", name, that, ct);
                    break;
                case Type.PType1:
                case Type.PType: if (name.CompareTo(((PType)that).dataType?.name) == 0)
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.PRole1:
                case Type.PRole: if (name.CompareTo(((PRole)that).name) == 0)
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.PView: if (name.CompareTo(((PView)that).name) == 0)
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.Drop: if (defpos == ((Drop)that).delpos)
                        return new DBException("40010", ppos, that, ct);
                    break;
                case Type.Change: 
                    {
                        var ch = (Change)that;
                        if (ch.Affects == Affects || ch.name.CompareTo(name) == 0)
                            return new DBException("40032", ppos, that, ct);
                        break;
                    }
                case Type.Grant:
                    return new DBException("40051", ppos, that, ct);
                case Type.PMethod:
                case Type.PMethod2:
                case Type.PProcedure2:
                case Type.PProcedure: if (name.CompareTo(((PProcedure)that).nameAndArity)==0) 
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.PTrigger: if (name.CompareTo(((PTrigger)that).name) == 0)
                        return new DBException("40032", ppos, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that, ct);
        }
        /// <summary>
        /// Update the Database to include the Physical
        /// </summary>
        /// <param name="cx">The Context</param>
        /// <returns>The new Database</returns>
        internal override DBObject? Install(Context cx)
        {
            var ro = cx.role;
            var ob = cx.db.objects[defpos] as DBObject;
            if (ob == null)
                return null;
            var oi = ob.infos[ro.defpos];
            if (oi == null)
                return null;
            var m = ob.mem;
            m += (DBObject.LastChange, ppos);
            m += (DBObject.Infos, new BTree<long, ObInfo>(ro.defpos, new ObInfo(name, oi.priv)));
            if (oi.name != null && ro.dbobjects.Contains(oi.name))
                ro += (Role.DBObjects, ro.dbobjects - oi.name + (name, defpos));
            ob = (DBObject)ob.New(m);
            cx.db += ob;
            cx.db += ro;
            cx.obs += (defpos, ob);
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            if (ob is TableColumn tc)
                ob = cx.db.objects[tc.tabledefpos] as Table;
            return ob as Table;
        }
    }
}
