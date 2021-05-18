using System;
using Pyrrho.Common;
using Pyrrho.Level1;
using Pyrrho.Level3;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
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
	/// The Change record in the database is used for renaming of objects other than TableColumns
	/// </summary>
	internal class Change : Physical
	{
        /// <summary>
        /// The previous physical record for this object
        /// </summary>
        long prev;
        /// <summary>
        /// We compute this when reading
        /// </summary>
        public long affects;
        /// <summary>
        /// The new name for the object
        /// </summary>
		public string name;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr,prev)) return prev;
            return -1;
        }
        /// <summary>
        /// Constructor: a new Change object from the Parser
        /// </summary>
        /// <param name="pt">The defining position for this object</param>
        /// <param name="nm">The (new) name</param>
        /// <param name="idType">The identifier type</param>
        /// <param name="tr">The transaction</param>
        public Change(long pt, string nm, long pp, Context cx) 
			:this(Type.Change,pt,nm, pp, cx)
		{
            prev = pt;
            name = nm;
        }
        /// <summary>
        /// Constructor: a new Change object from the Parser
        /// </summary>
        /// <param name="t">The Change type</param>
        /// <param name="pt">The defining position for this object</param>
        /// <param name="nm">The (new) name</param>
        /// <param name="idType">The identifier type</param>
        /// <param name="db">The local database</param>
        protected Change(Type t, long pt, string nm, long pp, Context cx)
			:base(t,pp,cx)
		{
            prev = pt;
            name = nm;
		}
        /// <summary>
        /// Constructor: a new Change object from the buffer
        /// </summary>
        /// <param name="bp">the buffer</param>
        /// <param name="pos">the defining position</param>
		public Change(ReaderBase rdr) :base(Type.Change,rdr)
        {
            prev = rdr.GetLong();
            name = rdr.GetString();
        }
        protected Change(Change x, Writer wr) : base(x, wr)
        {
            prev = wr.Fix(x.prev);
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
                return affects;
			}
		}
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for Positions</param>
        public override void Serialise(Writer wr)
		{
            wr.PutLong(prev);
            wr.PutString(name);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">The buffer</param>
        public override void Deserialise(ReaderBase rdr) 
		{ 
			prev = rdr.GetLong();
            affects = rdr.Prev(prev)??ppos;
			name = rdr.GetString();
			base.Deserialise(rdr);
		}
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>A string representation</returns>
		public override string ToString() 
		{ 
			return "Change "+Pos(prev)+" ["+Pos(Affects)+"] to "+name; 
		}
        /// <summary>
        /// The new name of the object
        /// </summary>
		public override string Name { get { return name; }}
        public override DBException Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch(that.type)
            {
                case Type.PTable1:
                case Type.PTable: if (name.CompareTo(((PTable)that).name) == 0)
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.PDomain1:
                case Type.PDomain: if (name.CompareTo(((PDomain)that).domain.name) == 0)
                        return new DBException("40022", name, that, ct);
                    break;
                case Type.PType1:
                case Type.PType: if (name.CompareTo(((PType)that).domain.name) == 0)
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.PRole1:
                case Type.PRole: if (name.CompareTo(((PRole)that).name) == 0)
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.PView1:
                case Type.PView: if (name.CompareTo(((PView)that).name) == 0)
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.Drop: if (prev == ((Drop)that).delpos)
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
        /// ReadCheck for change to the affected object
        /// </summary>
        /// <param name="pos">The object read</param>
        /// <returns>Whether a read conflict has occurred</returns>
		public override DBException ReadCheck(long pos,Physical r,PTransaction ct)
		{
			return (pos==Affects)?new DBException("40005",pos,r,ct).Mix():null;
		}

        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            var oi = ro.infos[affects] as ObInfo;
            ro = ro + (new ObInfo(affects, name,oi.domain, oi.priv),true);
            cx.db += (ro, p);
            cx.obs+=(affects,cx.obs[affects] + (Basis.Name, name));
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
        }
    }
}
