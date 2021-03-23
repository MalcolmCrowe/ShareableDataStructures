using System;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Common;
using System.Text;
using System.Diagnostics.Eventing.Reader;
using System.Configuration;
using System.CodeDom.Compiler;
using System.Net.NetworkInformation;

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
            Update1, Delete1, Drop1, RefAction // 71-74
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
        // for format<51 compatibility
        public BTree<long, (string, long)> digested = BTree<long, (string, long)>.Empty;
        public long time;
        protected Physical(Type tp, long pp, Context cx)
        {
            type = tp;
            ppos = pp;
            time = DateTime.Now.Ticks;
        }
        /// <summary>
        /// Constructor: A Physical from the buffer
        /// </summary>
        /// <param name="tp">The Type required</param>
        /// <param name="tb">The buffer</param>
        /// <param name="pos">The defining position</param>
        protected Physical(Type tp, ReaderBase rdr)
        {
            type = tp;
            ppos = rdr.Position-1;
            rdr.Set(this);
        }
        protected Physical(Physical ph,Writer wr)
        {
            type = ph.type;
            digested = ph.digested;
            ppos = wr.Length;
            wr.uids += (ph.ppos, ppos);
            time = ph.time;
        }
        internal static int IntLength(long p)
        {
            return 1 + new Integer(p).bytes.Length;
        }
        internal static int ColsLength(CList<long> cols)
        {
            var r = IntLength(cols.Length);
            for (var b = cols.First(); b!=null;b=b.Next())
                r += IntLength(b.value());
            return r;
        }
        internal static int StringLength(object o)
        {
            if (o == null)
                return 1;
            var p = Encoding.UTF8.GetBytes(o.ToString()).Length;
            return p + IntLength(p);
        }
        internal static int RepresentationLength(Context cx,BTree<long,Domain> rep)
        {
            var r = 1 + IntLength((int)rep.Count);
            for (var b=rep.First();b!=null;b=b.Next())
                r += IntLength(b.key()) + IntLength(cx.db.types[b.value()]);
            return r;
        }
        /// <summary>
        /// Many Physicals affect another: we expose this in Log tables
        /// </summary>
        public virtual long Affects
        {
            get { return ppos; }
        }
        public virtual bool Committed(Writer wr,long pos)
        {
            return pos < wr.Length || wr.uids.Contains(pos);
        }
        /// <summary>
        /// On commit, dependent Physicals must be committed first
        /// </summary>
        /// <returns>An uncommitted Physical ppos or null if there are none</returns>
        public abstract long Dependent(Writer wr,Transaction tr);
        /// <summary>
        /// Install a single Physical. 
        /// </summary>
        internal abstract void Install(Context cx, long p);
        internal virtual void OnLoad(Reader rdr)
        { }
        /// <summary>
        /// Commit (Serialise) ourselves to the datafile.
        /// Overridden by PTransaction, PTrigger, PMethod, PProcedure, PCheck
        /// Suppose we have two physicals a and b in a transaction with a earlier than b. 
        /// We need to be sure that nothing in a uses b's defpos: 
        /// when we serialise b we will know about a's new position, but not the other way round.
        /// </summary> 
        /// <param name="wr">The writer</param>
        public virtual (Transaction,Physical) Commit(Writer wr,Transaction tr)
        {
            if (Committed(wr,ppos)) // already done
                return (tr,this);
            for (; ; ) // check for uncommitted dependents
            {
                var pd = Dependent(wr,tr);
                if (Committed(wr,pd))
                    break;
                // commit the dependent physical and update wr relocation info
                tr?.physicals[pd].Commit(wr,tr);
                // and try again
            }
            wr.srcPos = wr.Length + 1;
            var ph = Relocate(wr);
            wr.WriteByte((byte)type);
            ph.Serialise(wr);
            ph.Install(wr.cx, wr.Length);
            return (tr,ph);
        }
        protected abstract Physical Relocate(Writer wr);
        internal virtual void Relocate(Context cx) { }
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
        public virtual void Deserialise(ReaderBase rdr)
        {
            rdr.Segment(this);
        }
        /// <summary>
        /// The name of the record
        /// </summary>
        public virtual string Name { get { return null; } }
        public override string ToString() { return "Physical"; }
        protected string Pos(long p)
        {
            return DBObject.Uid(p);
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
        public virtual DBException ReadCheck(long pos,Physical r,PTransaction ct)
        {
            return null;
        }
        public virtual DBException Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            return null;
        }
        protected string DigestSql(Writer wr,string s)
        {
            if (digested.Count == 0)
                return s;
            var sb = new StringBuilder();
            var cp = 0;
            for (var b=digested.First();b!=null;b=b.Next())
            {
                var sp = wr.Fix(b.key())-ppos;
                if (sp <= 0)
                    continue;
                while(cp<sp)
                    sb.Append(s[cp++]);
                var (os, dp) = b.value();
                cp += os.Length;
                sb.Append('"'); sb.Append(wr.Fix(dp)); sb.Append('"');
            }
            while (cp < s.Length)
                sb.Append(s[cp++]);
            return sb.ToString();
        }
        internal virtual void Affected(ref BTree<long,BTree<long,long>> aff)
        { }
    }
    internal class Curated : Physical
    {
        public Curated(ReaderBase rdr) : base(Type.Curated, rdr) { }
        public Curated(long pp, Context cx) : base(Type.Curated, pp, cx) { }
        protected Curated(Curated x, Writer wr) : base(x, wr) { }
        public override long Dependent(Writer wr, Transaction tr)
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
        internal override void Install(Context cx, long p)
        {
            cx.db += (Database.Curated, ppos);
            cx.db += (Database.Log, cx.db.log + (ppos, type));
        }

    }
    internal class Versioning : Physical
    {
        public long perioddefpos;
        public Versioning(ReaderBase rdr) : base(Type.Versioning,rdr) { }
        public Versioning(long pd, long pp, Context cx)
            : base(Type.Versioning, pp, cx)
        {
            perioddefpos = pd;
        }
        protected Versioning(Versioning x, Writer wr) : base(x, wr)
        {
            perioddefpos = wr.Fix(x.perioddefpos);
        }
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr,perioddefpos)) return perioddefpos;
            return -1;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Versioning(this, wr);
        }
        public override DBException Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch(that.type)
            {
                case Type.PeriodDef:
                    if (perioddefpos == ((PPeriodDef)that).defpos)
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.Versioning:
                    if (perioddefpos == ((Versioning)that).perioddefpos)
                        return new DBException("40032", ppos, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that, ct);
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
        public override void Deserialise(ReaderBase rdr)
        {
            perioddefpos = rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return "Versioning for "+perioddefpos;
        }

        internal override void Install(Context cx, long p)
        {
            var pd = (PeriodDef)cx.db.mem[perioddefpos];
            var tb = (Table)cx.db.mem[pd.tabledefpos]+(Table.SystemPS,pd);
            cx.db += (tb, p);
            cx.db += (Database.Log, cx.db.log + (ppos, type));
        }
    }
 

    internal class Namespace : Physical
    {
        public string prefix = "";
        public string uri;
        public Namespace(ReaderBase rdr) : base(Type.Namespace, rdr) 
        {
        }
        public Namespace(string pf, string ur, long pp, Context cx)
            : base(Type.Namespace, pp, cx) 
        {
            prefix = pf;
            uri = ur;
        }
        protected Namespace(Namespace x, Writer wr) : base(x, wr)
        {
            prefix = x.prefix;
            uri = x.uri;
        }
        public override long Dependent(Writer wr, Transaction tr)
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
        public override void Deserialise(ReaderBase rdr)
        {
            prefix = rdr.GetString();
            uri = rdr.GetString();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return "Namespace " + prefix + "=" + uri;
        }
        public override DBException Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            if (that.type == Type.Namespace)
                return new DBException("40050", ppos, that, ct);
            return null;
        }

        internal override void Install(Context cx, long p)
        {
            throw new NotImplementedException();
        }
    }
    internal class Classify : Physical
    {
        public long obj;
        public Level classification; 
        public Classify(ReaderBase rdr) : base(Type.Classify,rdr)
        { }
        protected Classify(Classify x, Writer wr) : base(x, wr)
        {
            obj = wr.Fix(x.obj);
            classification = x.classification;
        }
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr,obj)) return obj;
            return -1;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Classify(this, wr);
        }
        public Classify(long ob, Level cl, long pp, Context cx)
            : base(Type.Classify, pp, cx)
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
        public override void Deserialise(ReaderBase rdr)
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

        internal override void Install(Context cx, long p)
        {
            var ob = (DBObject)cx.db.objects[obj];
            if (cx.role.defpos != ob.definer && cx.role.defpos != 0)
                throw new DBException("42105");
            for (var b = cx.db.roles.First(); b != null; b = b.Next())
            {
                var ro = (Role)cx.db.objects[b.value()];
                if (ro.infos[obj] is ObInfo oi)
                    cx.db += (ro + (obj, oi + (DBObject.Classification, classification)), p);
            }
            cx.db += (ob+(DBObject.Classification,classification), p);
            cx.db += (Database.Log, cx.db.log + (ppos, type));
        }
    }
    /// <summary>
    /// Compiled objects are Modify, PColumn, PCheck, PProcedure, PTrigger, PView
    /// </summary>
    internal abstract class Compiled : Physical
    {
        internal Framing framing;
        internal Domain domain;
        protected Compiled(Type tp, long pp, Context cx,Framing frame,Domain dom) 
            : base(tp, pp, cx) 
        {
            framing = frame;
            domain = dom;
        }
        protected Compiled(Type tp, long pp, Context cx,Domain dm)
            : base(tp, pp, cx)
        {
            domain = dm;
            Frame(cx);
        }
        protected Compiled(Type tp, ReaderBase rdr) :base(tp,rdr) 
        {
            framing = Framing.Empty; // fixed in OnLoad
            domain = Domain.Content;
        }
        protected Compiled(Compiled ph, Writer wr) : base(ph, wr) 
        {
            framing = (Framing)ph.framing._Relocate(wr);
            domain = (Domain)ph.domain._Relocate(wr);
        }
        internal override void Relocate(Context cx)
        {
            framing.Install(cx);
            cx.SrcFix(ppos + 1);
            framing.Relocate(cx);
            var of = framing;
            framing = (Framing)framing.Fix(cx);
            domain = (Domain)domain.Fix(cx);
            framing.Install(cx);
            if (of != framing)
                Install(cx,cx.db.loadpos);
        }
        /// <summary>
        /// Fix heap uids and install compiled DBObjects from the context
        /// </summary>
        /// <param name="cx">The parsing context</param>
        public void Frame(Context cx)
        {
            cx.SrcFix(ppos+1);
            framing = new Framing(cx);
            Relocate(cx);
            cx.frame = null;
        }
    }
}
