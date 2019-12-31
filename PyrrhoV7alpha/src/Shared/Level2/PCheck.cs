using System;
using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;

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
	/// A PCheck is for a check constraint for Table, Column, or Domain.
	/// </summary>
	internal class PCheck : Physical
	{
		public long ckobjdefpos; // of object (e.g. Domain,Table) to which this check applies
        public long subobjdefpos = -1; // of Column if a columns check
		public string name;
        public long defpos;
		public string check;
        public SqlValue test;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr,ckobjdefpos)) return ckobjdefpos;
            if (!Committed(wr,subobjdefpos)) return subobjdefpos;
            if (defpos!=ppos && !Committed(wr,defpos)) return defpos;
            return -1;
        }
        /// <summary>
        /// Constructor: A new check constraint from the Parser
        /// </summary>
        /// <param name="dm">The object to which the check applies</param>
        /// <param name="nm">The name of the constraint</param>
        /// <param name="cs">The constraint as a string</param>
        /// <param name="db">The local database</param>
        public PCheck(long dm, string nm, string cs, Transaction tr)
            : this(Type.PCheck, dm, nm, cs, tr) { }
        protected PCheck(Type tp, long dm, string nm, string cs, Transaction tr)
			:base(tp,tr)
		{
			ckobjdefpos = dm;
            defpos = ppos;
            name = nm ?? throw new DBException("42102");
			check = cs;
            var cx = new Context(tr);
            cx.Add(tr.objects[ckobjdefpos] as DBObject,tr.role.obinfos[ckobjdefpos] as ObInfo);
            if (subobjdefpos!=-1)
                cx.Add(tr.objects[subobjdefpos] as DBObject);
            test = new Parser(tr, cx).ParseSqlValue(check,Domain.Bool);
        }
        /// <summary>
        /// Constructor: A new check constraint from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PCheck(Reader rdr) : base (Type.PCheck,rdr)
		{}
        protected PCheck(PCheck x, Writer wr) : base(x, wr)
        {
            ckobjdefpos = wr.Fix(x.ckobjdefpos);
            defpos = wr.Fix(x.defpos);
            name = x.name;
            check = x.check;
            test = (SqlValue)x.test.Relocate(wr);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PCheck(this, wr);
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
			return "Check " +name+" ["+Pos(ckobjdefpos)+"]: "+check;
		}
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr)
		{
            wr.PutLong(ckobjdefpos);
            wr.PutString(name.ToString());
            wr.PutString(check);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
		{
			ckobjdefpos = rdr.GetLong();
			name = rdr.GetString();
            defpos = ppos;
			check = rdr.GetString();
			base.Deserialise(rdr);
            var cx = rdr.context;
            cx.Add(rdr.db.objects[ckobjdefpos] as DBObject,
                rdr.role.obinfos[ckobjdefpos] as ObInfo);
            test = new Parser(rdr.db,cx).ParseSqlValue(check,Domain.Bool);
        }
        public override long Conflicts(Database db, Transaction tr, Physical that)
        {
            switch(that.type)
            {
                case Type.PCheck2:
                case Type.PCheck:
                    return (name == ((PCheck)that).name) ? ppos : -1;
                case Type.Drop:
                    return (ckobjdefpos == ((Drop)that).delpos) ? ppos : -1;
                case Type.Change:
                    return (ckobjdefpos == ((Change)that).affects) ? ppos : -1;
                case Type.Alter:
                    return (ckobjdefpos == ((Alter)that).defpos) ? ppos : -1;
            }
            return base.Conflicts(db, tr, that);
        }
        internal override (Database, Role) Install(Database db, Role ro, long p)
        {
            var ck = new Check(this, db);
            db += (((DBObject)db.mem[ck.checkobjpos]).Add(ck, db),p);
            if (name != null && name != "")
            { 
                ro += new ObInfo(defpos, name);
                db += (ro, p);
            }
            return (db +(ck,p),ro); 
        }
    }
    /// <summary>
    /// A version of PCheck that deals with deeply-structured objects.
    /// </summary>
    internal class PCheck2 : PCheck
    {
      /// <summary>
        /// Constructor: A new check constraint from the Parser
        /// </summary>
        /// <param name="dm">The object to which the check applies</param>
        /// <param name="so">The subobject to which the check applies</param>
        /// <param name="nm">The name of the constraint</param>
        /// <param name="cs">The constraint as a string</param>
        /// <param name="pb">The local database</param>
        public PCheck2(long dm, long so, string nm, string cs, Transaction tr)
			:base(Type.PCheck2,dm,nm,cs,tr)
		{
            subobjdefpos=so;
		}
        /// <summary>
        /// Constructor: A new check constraint from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PCheck2(Reader rdr) : base(rdr)
		{}
        protected PCheck2(PCheck2 p, Writer wr) : base(p, wr) 
        {
            subobjdefpos = wr.Fix(p.subobjdefpos);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PCheck2(this,wr);
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
			return "Check " +name+" ["+ckobjdefpos+":"+subobjdefpos+"]: "+check;
		}
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr)
		{
			subobjdefpos = wr.Fix(subobjdefpos);
            wr.PutLong(subobjdefpos);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
		{
			subobjdefpos = rdr.GetLong();
			base.Deserialise(rdr);
		}
        /// <summary>
        /// Looks the same as PCheck::Install but gets a different constructor for Check
        /// and calls a different Add !
        /// </summary>
        /// <param name="db"></param>
        /// <param name="ro"></param>
        /// <param name="p"></param>
        /// <returns></returns>
        internal override (Database, Role) Install(Database db, Role ro, long p)
        {
            var ck = new Check(this, db);
            db += (((DBObject)db.mem[ck.checkobjpos]).Add(ck, db), p);
            if (name != null && name != "")
            {
                ro += new ObInfo(defpos, name);
                db += (ro, p);
            }
            return (db + (ck, p), ro);
        }
    }
}
