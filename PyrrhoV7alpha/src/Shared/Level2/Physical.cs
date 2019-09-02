using System;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Common;
using System.Text;

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
	/// The Physical classes are transient: created when the file is read, and stay just 
	/// long enough to build indexes and system tables (level 3)
	/// (except for Record/Update)
	/// They are created/recreated when needed in Transactions
	/// They should not be seen above level 3
	/// 
	/// Each subclass has a const PhysicalType.
    /// IMMUTABLE and SHAREABLE used in Record/Update
    /// Committed if pos.LT.Transaction.TransPos
	/// </summary>
    internal abstract class Physical
    {
        /// <summary>
        /// Physical Record Types. These types are recorded in the database files so should not be changed
        /// </summary>
        public enum Type
        {
            EndOfFile, PTable, PRole, PColumn, Record, //0-4
            Update, Change, Alter, Drop, Checkpoint, Delete, Edit, //5-11
            PIndex, Modify, PDomain, PCheck, //12-15
            PProcedure, PTrigger, PView, PUser, PTransaction, //16-20
            Grant, Revoke, PRole1, PColumn2, //21-24
            PType, PMethod, PTransaction2, Ordering, NotUsed, //25-29
            PDateType, PTemporalView, PImportTransaction, Record1, //30-33 PTemporalView is obsolete
            PType1, PProcedure2, PMethod2, PIndex1, Reference, Record2, Curated, //34-40
            Partitioned, PDomain1, Namespace, PTable1, Alter2, AlterRowIri, PColumn3, //41-47
            Alter3, PView1, Metadata, PeriodDef, Versioning, PCheck2, Partition, //48-54 PView1 is obsolete
            Reference1, ColumnPath, Metadata2, PIndex2, DeleteReference1, //55-59
            Authenticate, RestView, TriggeredAction, RestView1, Metadata3, //60-64
            RestView2, Audit, Clearance, Classify, Enforcement, Record3, // 65-70
            Update1
        };
        /// <summary>
        /// The Physical.Type of the Physical
        /// </summary>
        public readonly Type type;
        /// <summary>
        /// address in file of this object
        /// </summary>
        public readonly long ppos;
        public long trans;
        public User user;
        public Role role;
        public readonly long time;
        protected Physical(Type tp, long pp, Database db)
        {
            type = tp;
            ppos = pp;
            user = db.user;
            role = db.role;
            time = DateTime.Now.Ticks;
        }
        /// <summary>
        /// Constructor: A Physical from the buffer
        /// </summary>
        /// <param name="tp">The Type required</param>
        /// <param name="tb">The buffer</param>
        /// <param name="pos">The defining position</param>
        protected Physical(Type tp, Reader rdr)
        {
            type = tp;
            ppos = rdr.Position-1;
            user = rdr.user;
            role = rdr.role;
            time = rdr.time;
        }
        protected Physical(Physical ph,Writer wr)
        {
            type = ph.type;
            ppos = wr.Length;
            wr.uids += (ph.ppos, ppos);
            user = wr.db.user;
            role = wr.db.role;
            time = ph.time;
        }
        string _Pos => Pos(ppos);
        /// <summary>
        /// Many Physicals affect another: we expose this in Log tables
        /// </summary>
        public virtual long Affects
        {
            get { return 0L; }
        }
        public static bool Committed(Writer wr,long pos)
        {
            return pos>=-1 && wr.Fix(pos) < Transaction.TransPos;
        }
        /// <summary>
        /// On commit, dependent Physicals must be committed first
        /// </summary>
        /// <returns>An uncommitted Physical ppos or null if there are none</returns>
        public abstract long Dependent(Writer wr);
        /// <summary>
        /// Install a single Physical. 
        /// </summary>
        internal abstract Database Install(Database db, Role ro, long p);
        /// <summary>
        /// Commit (Serialise) ourselves to the datafile.
        /// Overridden by PTransaction.
        /// Suppose we have two physicals a and b in a transaction with a earlier than b. 
        /// We need to be sure that nothing in a uses b's defpos: 
        /// when we serialise b we will know about a's new position, but not the other way round.
        /// </summary> 
        /// <param name="wr">The writer</param>
        public virtual void Commit(Writer wr,Transaction tr)
        {
            if (Committed(wr,ppos)) // already done
                return;
            for (; ; ) // check for uncommitted dependents
            {
                var pd = Dependent(wr);
                if (Committed(wr,pd))
                    break;
                // commit the dependent physical and update wr relocation info
                tr.physicals[pd].Commit(wr,tr);
                // and try again
            }
            var ph = Relocate(wr);
            wr.WriteByte((byte)type);
            ph.Serialise(wr);
            wr.db = ph.Install(wr.db, wr.db.roles[tr.role.defpos], wr.Length);
        }
        protected abstract Physical Relocate(Writer wr);
        /// <summary>
        /// Serialise ourselves to the datafile. Called by Commit,
        /// which has already written the first byte of the log entry.
        /// All subclasses call their base.Serialise(wr) LAST.
        /// This class merely writes the transaction segment at
        /// the end of the physical log entry.
        /// </summary>
        public virtual void Serialise(Writer wr)
        {
            wr.PutLong(wr.segment);
        }
        /// <summary>
        /// Deserialise ourselves from the buffer
        /// </summary>
        /// <param name="buf">The buffer</param>
        public virtual void Deserialise(Reader rdr)
        {
            rdr.segment = rdr.GetLong();
            trans = rdr.segment;
        }
        /// <summary>
        /// Check to see if our data is before the given stop date
        /// </summary>
        /// <returns>whether we are to be used</returns>
        public virtual bool CheckDate()
        {
            return true;
        }
        /// <summary>
        /// The name of the record
        /// </summary>
        public virtual string Name { get { return null; } }
        public override string ToString() { return "Physical"; }
        protected string Pos(long p)
        {
            if (p < Level3.Transaction.TransPos)
                return "" + p;
            return "'" + (p - Level3.Transaction.TransPos);
        }
        /// <summary>
        /// The previous record affected by this one
        /// </summary>
        public virtual long Previous { get { return -1; } }
        /// <summary>
        /// Check a Read constraint: see ReadConstraint
        /// </summary>
        /// <param name="pos">a defining position</param>
        /// <returns>true if we conflict with this</returns>
        public virtual DBException ReadCheck(long pos)
        {
            return null;
        }
        public virtual long Conflicts(Database db, Transaction tr, Physical that)
        {
            return -1;
        }
    }
    internal class Curated : Physical
    {
        public Curated(Reader rdr) : base(Type.Curated, rdr) { }
        public Curated(long u,Transaction db) : base(Type.Curated, u, db) { }
        protected Curated(Curated x, Writer wr) : base(x, wr) { }
        public override long Dependent(Writer wr)
        {
            return -1;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Curated(this, wr);
        }
        public override string ToString()
        {
            return "SET Curated";
        }
        internal override Database Install(Database db, Role ro, long p)
        {
            return db+(Database.Curated,ppos);
        }

    }
    internal class Versioning : Physical
    {
        public long perioddefpos;
        public Versioning(Reader rdr) : base(Type.Versioning,rdr) { }
        public Versioning(long pd, long u, Transaction db)
            : base(Type.Versioning, u, db)
        {
            perioddefpos = pd;
        }
        protected Versioning(Versioning x, Writer wr) : base(x, wr)
        {
            perioddefpos = wr.Fix(x.perioddefpos);
        }
        public override long Dependent(Writer wr)
        {
            if (!Committed(wr,perioddefpos)) return perioddefpos;
            return -1;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Versioning(this, wr);
        }
        public override long Conflicts(Database db, Transaction tr, Physical that)
        {
            switch(that.type)
            {
                case Type.PeriodDef:
                    return (perioddefpos == ((PPeriodDef)that).defpos) ? ppos : -1;
                case Type.Versioning:
                    return (perioddefpos == ((Versioning)that).perioddefpos) ? ppos : -1;
            }
            return base.Conflicts(db, tr, that);
        }
        /// <summary>
        /// Serialise the Versioning to the PhysBase
        /// </summary>
        /// <param name="r">Reclocation of position information</param>
        public override void Serialise(Writer wr)
        {
            perioddefpos = wr.Fix(perioddefpos);
            wr.PutLong(perioddefpos);
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise the Delete from the buffer
        /// </summary>
        /// <param name="buf">The buffer</param>
        public override void Deserialise(Reader rdr)
        {
            perioddefpos = rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return "Versioning for "+perioddefpos;
        }

        internal override Database Install(Database db, Role ro, long p)
        {
            var pd = (PeriodDef)db.mem[perioddefpos];
            var tb = (Table)db.mem[pd.tabledefpos]+(Table.SystemTime,pd);
            return db + tb;
        }
    }
 

    internal class Namespace : Physical
    {
        public string prefix = "";
        public string uri;
        public Namespace(Reader rdr) : base(Type.Namespace, rdr) 
        {
        }
        public Namespace(string pf, string ur, long u, Transaction db)
            : base(Physical.Type.Namespace, u, db) 
        {
            prefix = pf;
            uri = ur;
        }
        protected Namespace(Namespace x, Writer wr) : base(x, wr)
        {
            prefix = x.prefix;
            uri = x.uri;
        }
        public override long Dependent(Writer wr)
        {
            return -1;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Namespace(this, wr);
        }
        public override void Serialise(Writer wr)
        {
            wr.PutString(prefix);
            wr.PutString(uri);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            prefix = rdr.GetString();
            uri = rdr.GetString();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return "Namespace " + prefix + "=" + uri;
        }
        public override long Conflicts(Database db, Transaction tr, Physical that)
        {
            return (that.type == Type.Namespace) ? ppos : -1;
        }

        internal override Database Install(Database db, Role ro, long p)
        {
            throw new NotImplementedException();
        }
    }
    internal class Classify : Physical
    {
        public long obj;
        public Level classification; 
        public Classify(Reader rdr) : base(Type.Classify,rdr)
        { }
        protected Classify(Classify x, Writer wr) : base(x, wr)
        {
            obj = wr.Fix(x.obj);
            classification = x.classification;
        }
        public override long Dependent(Writer wr)
        {
            if (!Committed(wr,obj)) return obj;
            return -1;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Classify(this, wr);
        }
        public Classify(long ob, Level cl, long u, Transaction db)
            : base(Type.Classify, u, db)
        {
            obj = ob;
            classification = cl;
        }
        public override void Serialise(Writer wr)
        {
            Level.SerialiseLevel(wr,classification);
            obj = wr.Fix(obj);
            wr.PutLong(obj);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            classification = Level.DeserialiseLevel(rdr);
            obj = rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Classify " + obj);
            classification.Append(sb);
            return sb.ToString();
        }

        internal override Database Install(Database db, Role ro, long p)
        {
            var ob = (DBObject)ro.objects[obj];
            if (ro.defpos != ob.definer && ro.defpos != Database.Schema)
                throw new DBException("42105");
            var nb = ob+ (DBObject.Classification,classification);
            return db + nb;
        }
    }
}
