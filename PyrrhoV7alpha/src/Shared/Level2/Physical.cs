using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Common;
using System.Text;
using Pyrrho.Level5;
using System.Xml;
using System.Reflection.Metadata;
using System.Security.Cryptography;

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
            PType, PMethod, NotUsed, Ordering, NotUsed1, //25-29
            PDateType, NotUsed2, NotUsed3, NotUsed4, //30-33 
            PType1, PProcedure2, PMethod2, PIndex1, Reference, Record2, Curated, //34-40
            NotUsed5, PDomain1, Namespace, PTable1, Alter2, AlterRowIri, PColumn3, //41-47
            Alter3, PType2, Metadata, PeriodDef, Versioning, PCheck2, NotUsed7, //48-54 
            NotUsed8, ColumnPath, Metadata2, PIndex2, DeleteReference1, //55-59
            Authenticate, RestView, TriggeredAction, RestView1, Metadata3, //60-64
            RestView2, Audit, Clearance, Classify, Enforcement, Record3, // 65-70
            Update1, Delete1, Drop1, RefAction, Post, // 71-75
            PNodeType, PEdgeType, EditType, AlterIndex, AlterEdgeType, // 76-80
            Record4, Update2, Delete2, PSchema, PGraph, PGraphType // 81-87 (PGraph is currently unused)
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
        public long time;
        internal Database db; // for Context.BackTo, see GraphInsertStatement, JoinRecords
        public bool ifNeeded = false;
        protected Physical(Type tp, long pp, Database d)
        {
            type = tp;
            ppos = pp;
            db = d;
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
            db = rdr.context.db;
            rdr.Set(this);
        }
        protected Physical(Physical ph,Writer wr)
        {
            type = ph.type;
            ppos = wr.Length;
            wr.cx.uids += (ph.ppos, ppos);
            db = wr.cx.db;
            time = ph.time;
        }
        /// <summary>
        /// Many Physicals affect another: we expose this in Log tables
        /// </summary>
        public virtual long Affects => ppos;
        public virtual bool Committed(Writer wr,long pos)
        {
            return (pos < Transaction.TransPos|| pos>=Transaction.Executables) || wr.cx.uids.Contains(pos);
        }
        /// <summary>
        /// On commit, dependent Physicals must be committed first
        /// </summary>
        /// <returns>An uncommitted Physical ppos or null if there are none</returns>
        public abstract long Dependent(Writer wr,Transaction tr);
        /// <summary>
        /// Install a single Physical. 
        /// </summary>
        internal abstract DBObject? Install(Context cx);
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
        public virtual (Transaction?,Physical) Commit(Writer wr,Transaction? tr)
        {
            if (Committed(wr,ppos)) // already done
                return (tr,this);
            for (;tr is not null ; ) // check for uncommitted dependents
            {
                var pd = Dependent(wr,tr);
                if ((pd<Transaction.Analysing||pd>=Transaction.Executables) && Committed(wr,pd))
                    break;
                // commit the dependent physical and update wr relocation info
                if (pd>0)
                    tr.physicals[pd]?.Commit(wr,tr);
                // and try again
            }
            var ph = Relocate(wr);
            wr.WriteByte((byte)type);
            ph.Serialise(wr);
            ph.Install(wr.cx);
            return (tr,ph);
        }
        internal Physical _Commit(Writer wr,Transaction tr)
        {
            wr.WriteByte((byte)type);
            Serialise(wr);
            Install(wr.cx);
            return this;
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
            rdr.Segment(this);
        }
        public virtual long _table => -1L;
        public virtual CTree<long,bool> supTables => CTree<long,bool>.Empty;
        public virtual CTree<long, bool> subTables => CTree<long, bool>.Empty;
        public override string ToString() { return type.ToString(); }
        protected static string Pos(long p)
        {
            return DBObject.Uid(p);
        }
        /// <summary>
        /// The previous record affected by this one
        /// </summary>
        public virtual long Previous { get { return -1; } }
        public virtual DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            return null;
        }
        public virtual DBException? Conflicts(CTree<long,bool> t,PTransaction ct)
        {
            return null;
        }

        internal virtual bool NeededFor(BTree<long, Physical> physicals)
        {
            return !ifNeeded;
        }
    }
    internal class Curated : Physical
    {
        public Curated(Reader rdr) : base(Type.Curated, rdr) { }
        public Curated(long pp,Database d) : base(Type.Curated, pp, d) { }
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
        internal override DBObject? Install(Context cx)
        {
            cx.db += (Database.Curated, ppos);
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            return null;
        }
    }
    // Added for NodeTypes: make the given unique index the primary index
    internal class AlterIndex : Physical
    {
        public long idindexdefpos;
        public AlterIndex(Reader rdr) : base(Type.AlterIndex, rdr) { }
        public AlterIndex(long ix,long pp, Database d) : base(Type.AlterIndex, pp, d) 
        {
            idindexdefpos = ix;
        }
        protected AlterIndex(AlterIndex x, Writer wr) : base(x, wr) 
        {
            idindexdefpos = x.idindexdefpos;
        }
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr, idindexdefpos)) return idindexdefpos;
            return -1;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new AlterIndex(this, wr);
        }
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            var ux = cx.db.objects[idindexdefpos] as Level3.Index;
            switch (that.type)
            {
                case Type.AlterIndex:
                    if (idindexdefpos == ((AlterIndex)that).idindexdefpos)
                        return new DBException("40032", idindexdefpos, that, ct);
                    break;
                case Type.Drop:
                    if (idindexdefpos == ((Drop)that).delpos)
                        return new DBException("40032", idindexdefpos, that, ct);
                    if (ux?.tabledefpos == ((Drop)that).delpos)
                        return new DBException("40032", ux.tabledefpos, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that, ct);
        }
        public override void Serialise(Writer wr)
        {
            idindexdefpos = wr.cx.Fix(idindexdefpos);
            wr.PutLong(idindexdefpos);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            idindexdefpos = rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return "AlterIndex" + " new primary index " + DBObject.Uid(idindexdefpos);
        }
        internal override DBObject? Install(Context cx)
        {
            if (cx.db.objects[idindexdefpos] is not Level3.Index ux
                || cx.db.objects[ux.tabledefpos] is not Table ut
                || (ut is NodeType && ux.keys.Length != 1)
                || !ux.flags.HasFlag(PIndex.ConstraintType.Unique)
                || ut.FindPrimaryIndex(cx) is not Level3.Index px)
                return null;
            var pf = (int)px.flags - (int)PIndex.ConstraintType.PrimaryKey + (int)PIndex.ConstraintType.Unique;
            var uf = (int)ux.flags + (int)PIndex.ConstraintType.PrimaryKey - (int)PIndex.ConstraintType.Unique;
            px += (Level3.Index.IndexConstraint, (PIndex.ConstraintType)pf);
            ux += (Level3.Index.IndexConstraint, (PIndex.ConstraintType)uf);
            if (ut is NodeType nt && cx.db.objects[ux.keys[0] ?? -1L] is TableColumn nk)
            {
                var ok = cx.db.objects[nt.idCol] as TableColumn;
  /*              cx.db += (nk + (TableColumn.GraphFlag, PColumn.GraphFlags.IdCol));
                if (ok is not null)
                    cx.db += (ok - TableColumn.GraphFlag); 
                nt = nt + (NodeType.IdIx, idindexdefpos) + (NodeType.IdCol, nk.defpos); */
                cx.db += nt;
                nt.Refresh(cx);
                var cd = ok is null || ok.domain.kind!=nk.domain.kind;
                var us = ut.rindexes;
                for (var b = ut.rindexes.First(); b != null; b = b.Next())
                    if (cx.db.objects[b.key()] is EdgeType et)
                    {
                        var ks = CTree<long,bool>.Empty;
                        var rs = et.tableRows;
                        var dd = us[et.defpos];
                        long rk = 0L;
                        for (var c = b.value().First(); c != null; c = c.Next())
                            for (var d = et.indexes[c.key()]?.First(); d != null; d = d.Next())
                                if (cx.db.objects[d.key()] is Level3.Index rx && rx.rows is not null && dd is not null
                                    && rx.keys[0] is long kp && dd[rx.keys] is Domain dk)
                                {
                                    ks += (kp, true);
                                    MTree? st = null;
                                    for (var e = rx.rows.First(); e != null; e = e.Next())
                                        if (e.key()[0] is TypedValue k && px.rows?.impl?[k] is TInt v
                                                && ut.tableRows[v.ToLong() ?? -1L] is TableRow tr
                                                && tr.vals[nk.defpos] is TypedValue sk
                                                && rs[e.Value() ?? -1L] is TableRow te
                                                && tr.vals[nk.defpos] is TypedValue tk
                                                && e.Value() is long ev)
                                        {
                                            tk = dk.Coerce(cx, tk);
                                            te += (kp, tk);
                                            rk = (k as TInt)?.value??0L;
                                            rs += (te.defpos, te);
                                            var sl = new CList<TypedValue>(sk);
                                            if (st is null)
                                                st = new MTree(ux.keys, rx.rows.nullsAndDuplicates,
                                                   sl, 0, ev);
                                            else
                                                st += (sl, 0, ev);
                                        }
                                    var os = rx.keys;
                                    if (cd)
                                    {
                                        dd -= os;
                                        os += (Domain.Representation, new CTree<long, Domain>(os[0] ?? -1L, nk.domain));
                                        rx += (Level3.Index.Keys, os);
                                    }
                                    rx += (Level3.Index.RefIndex, ux.defpos);
                                    dd += (os, ux.keys);
                                    us += (et.defpos, dd);
                                    if (st is not null)
                                        rx += (Level3.Index.Tree, st);
                                    else
                                        throw new DBException("42105").Add(Qlx.CONSTRAINT_CATALOG);
                                    cx.db += rx;
                                }
                        ut += (Table.RefIndexes, us);
                        var sx = ut.sindexes;
                        if (cx.db.objects[rk] is TableColumn tc)
                            for (var d = ut.sindexes.First();d!=null;d=d.Next())
                                if (d.value().Contains(tc.defpos))
                                    sx += (d.key(),d.value()- tc.defpos);
                        if (sx != ut.sindexes)
                            ut += (Table.SysRefIndexes, sx);
                        if (cd)
                        {
                            var er = et.representation;
                            for (var c = ks.First(); c != null; c = c.Next())
                                if (cx.db.objects[c.key()] is TableColumn ec)
                                {
                                    cx.db += (ec + (DBObject._Domain, nk.domain));
                                    er += (c.key(), nk.domain);
                                }
                            et += (Domain.Representation, er);
                        }
                        et += (Table.TableRows, rs);
                        cx.db += et;
                    }
                cx.db += px;
                cx.db += ux;
                cx.db += nt;
                return nt; 
            }
            throw new DBException("42105").Add(Qlx.CONSTRAINT_CATALOG);
        }
    }
    internal class AlterEdgeType : Physical
    {
        public int cid; // ARRIVING 0, LEAVING 1, ..
        public long reftype;
        public long edgetype;
        public AlterEdgeType(Reader rdr) : base(Type.AlterEdgeType, rdr) { }
        protected AlterEdgeType(Type t, Reader rdr) : base(t, rdr) { }
        public AlterEdgeType(int id, long rt, long et, long pp, Database d) 
            : base(Type.AlterEdgeType, pp, d)
        {
            cid = id; reftype = rt; edgetype = et;
        }
        protected AlterEdgeType(AlterEdgeType x, Writer wr) : base(x, wr) 
        {
            cid = x.cid;
            edgetype = x.edgetype;
            reftype = x.reftype;
        }
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr, reftype)) return reftype;
            if (!Committed(wr, edgetype)) return edgetype;
            return -1;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new AlterEdgeType(this, wr);
        }
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch (that.type)
            {
                case Type.AlterEdgeType:
                    if (reftype == ((AlterEdgeType)that).reftype)
                        return new DBException("40032", reftype, that, ct);
                    break;
                case Type.Drop:
                    if (reftype == ((Drop)that).delpos)
                        return new DBException("40032", reftype, that, ct);
                    if (edgetype == ((Drop)that).delpos)
                        return new DBException("40032", edgetype, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that, ct);
        }
        public override void Serialise(Writer wr)
        {
            reftype = wr.cx.Fix(reftype);
            edgetype = wr.cx.Fix(edgetype);
            wr.PutInt(cid);
            wr.PutLong(reftype);
            wr.PutLong(edgetype);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            cid = rdr.GetInt();
            reftype = rdr.GetLong();
            edgetype = rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return "AlterEdgeType " + DBObject.Uid(edgetype)
                + (cid switch { 0=> " ARRIVING ", 1=> " LEAVING ", _=>cid.ToString()}) 
                + DBObject.Uid(reftype);
        }
        internal override DBObject? Install(Context cx)
        {
            if (cx.db.objects[edgetype] is not Level5.EdgeType et
                || cx.db.objects[reftype] is not Level5.NodeType nt)
                return null;
            var nm = cid switch { 0 => "ARRIVING", 1 => "LEAVING", _ => "??" };
            long ip = -1L;
            for (var b = nt.rindexes[et.defpos]?.First(); b != null && ip<0; b = b.Next())
                if (nt.representation.Contains(b.value()?.First()?.value() ?? -1L))
                    for (var c = et.indexes[b.key()]?.First(); c != null && ip<0L; c = c.Next())
                        if (cx.db.objects[c.key()] is Level3.Index ax)
                            if(ax.name == nm)
                                if (ax.reftabledefpos == nt.defpos)
                                    ip = ax.defpos;
            if (ip < 0)
                throw new PEException("PE40802");
            cx.db += et;
            cx.Add(et);
            return et;
        }
    }
    internal class Versioning : Physical
    {
        public long perioddefpos;
        public Versioning(Reader rdr) : base(Type.Versioning,rdr) { }
        public Versioning(long pd, long pp, Database d)
            : base(Type.Versioning, pp, d)
        {
            perioddefpos = pd;
        }
        protected Versioning(Versioning x, Writer wr) : base(x, wr)
        {
            perioddefpos = wr.cx.Fix(x.perioddefpos);
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
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch(that.type)
            {
                case Type.PeriodDef:
                    if (perioddefpos == ((PPeriodDef)that).defpos)
                        return new DBException("40032", perioddefpos, that, ct);
                    break;
                case Type.Versioning:
                    if (perioddefpos == ((Versioning)that).perioddefpos)
                        return new DBException("40032", perioddefpos, that, ct);
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
            perioddefpos = wr.cx.Fix(perioddefpos);
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
            return "Versioning for "+DBObject.Uid(perioddefpos);
        }

        internal override DBObject? Install(Context cx)
        {
            if (cx.db == null || cx.db.mem[perioddefpos] is not PeriodDef pd
            || cx.db.mem[pd.tabledefpos] is not Table tb)
                return null;
            tb += (Table.SystemPS, pd);
            cx.db += tb;
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            return tb;
        }
    }
 

    internal class Namespace : Physical
    {
        public string prefix = "";
        public string uri ="";
        public Namespace(Reader rdr) : base(Type.Namespace, rdr) 
        {
        }
        public Namespace(string pf, string ur, long pp, Database d)
            : base(Type.Namespace, pp, d) 
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
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            if (that.type == Type.Namespace)
                return new DBException("40050", ppos, that, ct);
            return null;
        }

        internal override DBObject? Install(Context cx)
        {
            throw new NotImplementedException();
        }
    }
    internal class Classify : Physical
    {
        public long obj;
        public Level classification = Level.D; 
        public Classify(Reader rdr) : base(Type.Classify,rdr)
        { }
        protected Classify(Classify x, Writer wr) : base(x, wr)
        {
            obj = wr.cx.Fix(x.obj);
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
        public Classify(long ob, Level cl, long pp, Database d) 
            : base(Type.Classify, pp, d)
        {
            obj = ob;
            classification = cl;
        }
        public override void Serialise(Writer wr)
        {
            Level.SerialiseLevel(wr,classification);
            obj = wr.cx.Fix(obj);
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

        internal override DBObject? Install(Context cx)
        {
            if (cx.db.objects[obj] is not DBObject ob)
                throw new DBException("42000","Classify");
            if (cx.role.defpos != ob.definer)
                throw new DBException("42105").Add(Qlx.SECURITY);
            for (var b = cx.db.roles.First(); b != null; b = b.Next())
                if (b.value() is long bp && cx.db.objects[bp] is Role ro && ob.infos[ro.defpos] is ObInfo oi)
                    cx.db += ro + (obj, oi + (DBObject.Classification, classification));
            ob = (DBObject)ob.New(ob.mem+ (DBObject.Classification, classification));
            cx.db += ob;
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            return ob;
        }
    }
    /// <summary>
    /// The constructor of a defined database object gains privileges including admin privilges on the new object.
    /// These privileges are recorded in the new object (not the definer's role!)
    /// </summary>
    internal abstract class Defined : Physical
    {
        public long definer;
        public long owner;
        internal Domain dataType = Domain.Null;
        public BTree<long, ObInfo> infos = BTree<long, ObInfo>.Empty;
        public string name
        {
            get { return infos[definer]?.name ?? ""; }
            set { 
                if (infos[definer] is ObInfo oi)
                    infos += (definer, oi + (ObInfo.Name, value));  
            }
        }
        /// <summary>
        /// Create a new Defined object
        /// </summary>
        /// <param name="tp">Type of the object</param>
        /// <param name="pp">Its physical position</param>
        /// <param name="cx">A Context with the definer's details</param>
        /// <param name="nm">The definer's name for the new object</param>
        /// <param name="priv">The definer's privileges on the new object</param>
        protected Defined(Type tp, long pp, Context cx, string nm, Grant.Privilege priv) 
            : base(tp, pp, cx.db)
        {
            definer = cx.role.defpos;
            owner = cx.user?.defpos ?? -1L;
            infos += (definer, new ObInfo(nm, priv));
        }
        /// <summary>
        /// Create a new Defined object
        /// </summary>
        /// <param name="tp">Type of the object</param>
        /// <param name="pp">Its physical position</param>
        /// <param name="cx">A Reader with the definer's details</param>
        /// <param name="nm">The definer's name for the new object</param>
        /// <param name="priv">The definer's privileges on the new object</param>
        protected Defined(Type tp,Reader rdr, string nm = "", Grant.Privilege priv = Grant.Privilege.NoPrivilege) 
            : base(tp, rdr)
        {
            definer = rdr.context.role.defpos;
            owner = rdr.context.user?.defpos ?? -1L;
            infos += (definer, new ObInfo(nm, priv));
        }
        protected Defined(Defined ph, Writer writer) : base(ph, writer) 
        {
            definer = Fix(ph.ppos, ph.definer, writer);
            owner = Fix(ph.ppos, ph.owner,writer);
            var r = BTree<long, ObInfo>.Empty;
            for (var b=ph.infos.First();b is not null;b=b.Next())
                r += (Fix(ph.ppos, b.key(), writer),(ObInfo)b.value().Fix(writer.cx));
            infos = r;
            dataType = ph.dataType;
        }
        long Fix(long pp,long p,Writer wr)
        {
            return (p == pp) ? ppos : (wr.cx.uids[p] is long np) ? np : p;
        }
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr, definer)) return definer;
            if (!Committed(wr,owner)) return owner;
            for (var b = infos.First(); b != null; b = b.Next())
                if (b.key()!=ppos && !Committed(wr,b.key())) 
                    return b.key();
            return -1;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' '); sb.Append(name);
            if (dataType.defpos >= 0 && dataType.defpos!=ppos && dataType!=Domain.NodeType && dataType!=Domain.EdgeType)
            {
                sb.Append('['); sb.Append(DBObject.Uid(dataType.defpos)); sb.Append(']');
            }
            if (dataType.Length == 0 && dataType.kind!=Qlx.EDGETYPE)
            { sb.Append(' '); sb.Append(dataType.kind); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// Compiled objects are Modify, PCheck, PProcedure, PTrigger
    /// </summary>
    internal abstract class Compiled : Defined
    {
        internal Framing framing = Framing.Empty;
        public long nst = Transaction.Executables; // must be set to cx.db.nextStmt before parsing of code
        protected Compiled(Type tp, long pp, Context cx, string nm, long tgt, Domain dom, long ns)
            : base(tp, pp, cx, nm, Grant.AllPrivileges)
        {
            var oc = cx.parse;
            framing = new Framing(cx,nst);
            nst = ns;
            dataType = framing.valueType;
            cx.parse = oc;
        }
        protected Compiled(Type tp, long pp, Context cx, string nm, Domain dm, long ns)
            : base(tp, pp, cx, nm, Grant.AllPrivileges)
        {
            dataType = dm;
            nst = ns;
        }
        // Reader will update the name and dataType
        protected Compiled(Type tp, Reader rdr) : base(tp, rdr,"",Grant.AllPrivileges)
        {
            framing = Framing.Empty; // fixed in OnLoad
            nst = rdr.context.db.nextStmt;
            dataType = tp switch
            {
                Type.PTable => Domain.TableType,
                _ => Domain.Content
            };
        }
        protected Compiled(Compiled ph, Writer wr) : base(ph, wr)
        {
            var oc = wr.cx.parse;
            wr.cx.parse = ExecuteStatus.Compile;
            wr.cx.offset = ppos - ph.ppos;
            framing = (Framing)(ph.framing?.Fix(wr.cx)??Framing.Empty);
            dataType = (Domain)ph.dataType.Fix(wr.cx);
            wr.cx.parse = oc;
            var ab = framing.obs.Last();
            if (ab != null)
                wr.cx.db += (Database.NextStmt, ab.key() + 1);
        }
        protected override Physical Relocate(Writer wr)
        {
            throw new NotImplementedException();
        }
        /// <summary>
        /// By the time this base method is called, dataType should have no 
        /// columns with uids !0..
        /// In some cases the overriding methods simply remove such old uids, 
        /// because the final install will add the new uids.
        /// </summary>
        /// <param name="wr"></param>
        /// <param name="tr"></param>
        /// <returns></returns>
        public override (Transaction?, Physical) Commit(Writer wr, Transaction? tr)
        {
            wr.cx.instDFirst = -1L;
            return base.Commit(wr, tr);
        }
    }
    internal class Post : Physical
    {
        internal string url;
        internal string target;
        internal string sql;
        internal string user;
        internal long _vw;
        internal PTrigger.TrigType tp;
        internal Context _cx;
        internal bool committed = false;
        internal Post(string u, string tn, string s, string us, long vw,
            PTrigger.TrigType t, long cp, Context cx) : base(Type.Post, cp, cx.db)
        {
            url = u;
            sql = s;
            target = tn;
            user = us;
            _vw = vw;
            tp = t;
            _cx = cx;
            if (cx.db is Database db)
                cx.db = db + (Transaction.Posts, true);
        }
        HttpRequestMessage GetRequest()
        {
            var vw = (RestView)(_cx.obs[_vw]??_cx.db.objects[_vw] 
                ??throw new DBException("42105").Add(Qlx.VIEW));
            string? user = _cx.user?.name, password = null;
            var ss = url.Split('/');
            if (ss.Length > 3)
            {
                var st = ss[2].Split('@');
                if (st.Length > 1)
                {
                    var su = st[0].Split(':');
                    user = su[0];
                    if (su.Length > 1)
                        password = su[1];
                }
            }
            var ix = url.LastIndexOf('/');
            HttpRequestMessage rq = new() { RequestUri = new Uri(url[0..ix]) };
            rq.Headers.Add("UserAgent","Pyrrho " + PyrrhoStart.Version[1]);
            if (user != null)
            {
                var cr = user + ":" + password;
                var d = Convert.ToBase64String(Encoding.UTF8.GetBytes(cr));
                rq.Headers.Add("Authorization","Basic "+d);
            }
            if (vw.infos[_cx.role.defpos] is ObInfo vi && vi.metadata.Contains(Qlx.ETAG))
            {
                if (_cx.result is RowSet rs && rs.First(_cx) is Cursor cu)
                        rq.Headers.Add("If-Match", cu._Rvv(_cx).ToString());
                else if (_cx.db is not null)
                {
                    rq.Headers.Add("If-Match", "W/\"" + _cx.db.length + "\"");
                    if (_cx.db.lastModified is DateTime dt)
                        rq.Headers.Add("If-Unmodified-Since",
                            "" + new THttpDate(dt, vi.metadata.Contains(Qlx.MILLI)));
                }
            }
            return rq;
        }
        public override (Transaction?,Physical) Commit(Writer wr, Transaction? tr)
        {
            if (!committed)
            {
                var rq = GetRequest();
                rq.Method = HttpMethod.Post;
                var sb = new StringBuilder();
                sb.Append(sql);
                sb.Append("\r\n");
                var ix = url.LastIndexOf('/');
                TargetActivation.RoundTrip(_cx, _vw, tp, rq, url[0..ix], sb);
                committed = true;
            }
            return (tr,this);
        }
        public override long Dependent(Writer wr, Transaction tr)
        {
            return -1L;
        }

        protected override Physical Relocate(Writer wr)
        {
            return this;
        }
        internal override DBObject? Install(Context cx)
        {
            return null;
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Post");
            sb.Append(' '); sb.Append(target);
            sb.Append(' '); sb.Append(url);
            sb.Append(' '); sb.Append(sql);
            sb.Append(' '); sb.Append(DBObject.Uid(_vw));
            return sb.ToString();
        }
    }
}
