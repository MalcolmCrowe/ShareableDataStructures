using System;
using Pyrrho.Common;
using System.Globalization;
using System.Text;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System.Runtime.CompilerServices;

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
    /// A new Domain definition
    /// </summary>
    internal class PDomain : Physical
    {
        /// <summary>
        /// The defining position of the Domain
        /// </summary>
		public virtual long defpos { get { return ppos; } }
        internal Domain domain;
        public override long Dependent(Writer wr, Transaction tr)
        {
            var udt = domain as UDType;
            if (udt?.super != null && !wr.cx.db.types.Contains(udt.super))
                udt.super.Create(wr, tr);
            if (domain.elType != null && !wr.cx.db.types.Contains(domain.elType))
                domain.elType.Create(wr, tr);
            if (udt!=null && !Committed(wr, udt.structure))
                return udt.structure;
            if (domain.orderFunc != null && !Committed(wr, domain.orderFunc.defpos))
                return domain.orderFunc.defpos;
            return -1L;
        }
        /// <summary>
        /// Constructor: a new Domain definition from the Parser
        /// </summary>
        /// <param name="t">The PDomain type</param>
        /// <param name="nm">The name of the domain, null for a standard type</param>
        /// <param name="dt">The base type</param>
        /// <param name="dl">The precision or length</param>
        /// <param name="sc">The scale</param>
        /// <param name="ch">The charset</param>
        /// <param name="co">The collation</param>
        /// <param name="dv">The default value</param>
        /// <param name="sd">The base structure definition if any</param>
        /// <param name="pb">The local database</param>
        public PDomain(Type t, string nm, Sqlx dt, int dl, int sc, CharSet ch,
            string co, string dv, long sd, long pp, Context cx)
            : base(t, pp, cx)
        {
            var k = (dt == Sqlx.ARRAY || dt == Sqlx.MULTISET) ? Domain.Element :
                    UDType.Structure;
            var v = (dv == "") ? null : Domain.For(dt).Parse(cx.db.uid, dv);
            domain = new Domain(dt, BTree<long, object>.Empty
                + (Domain.Precision, dl) + (Domain.Scale, sc)
                + (Domain.Charset, ch)
                + (Domain.Culture, CultureInfo.GetCultureInfo(co))
                + (Domain.DefaultString, dv)
                + (Domain.Default, v) + (k, sd) + (Basis.Name, nm));
        }
        public PDomain(string nm, Domain dt, long pp, Context cx)
            : this(Type.PDomain, nm, dt, pp, cx) { }

        protected PDomain(Type t, string nm, Domain dt, long pp, Context cx)
        : base(t, pp, cx)
        {
            domain = dt + (Basis.Name, nm);
        }
        /// <summary>
        /// This routine is called from Domain.Create().
        /// If dt has representation, the context will have a suitable structure
        /// </summary>
        /// <param name="dt">To be committed: may have representation</param>
        /// <param name="pp">The defpos to use</param>
        /// <param name="cx">The context</param>
        public PDomain(Domain dt, long pp, Context cx)
            : this(Type.PDomain, dt, pp, cx) { }
        protected PDomain(Type t, Domain dt, long pp, Context cx)
        : base(t, pp, cx)
        {
            if (dt.representation.Count > 0 && dt.structure < 0)
            {
                var cs = CList<Domain>.Empty;
                for (var b = dt.rowType.First(); b != null; b = b.Next())
                    cs += cx.obs[b.value()].domain;
                for (var b = cx.obs.First(); b != null; b = b.Next())
                {
                    var ob = b.value();
                    if (ob is Domain dc && _Match(cx, cs, dc))
                    {
                        dt += (Domain.Structure, dc.defpos);
                        break;
                    }
                    else if (ob.mem[DBObject._Domain] is Domain db && _Match(cx, cs, db))
                    {
                        dt += (Domain.Structure, db.defpos);
                        break;
                    }
                }
            }
            domain = dt;
        }
        /// <summary>
        /// Constructor: a new Domain definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PDomain(Reader rdr) : this(Type.PDomain, rdr)
        {
            rdr.context.db += (ppos, domain, rdr.context.db.loadpos);
        }
        /// <summary>
        /// Constructor: a new Domain definition from the buffer
        /// </summary>
        /// <param name="t">The PDomain type</param>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		protected PDomain(Type t, Reader rdr) : base(t, rdr) { }
        protected PDomain(PDomain x, Writer wr) : base(x, wr)
        {
            domain = (Domain)x.domain._Relocate(wr);
        }
        static bool _Match(Context cx, CList<Domain> cs, Domain dc)
        {
            if (dc.defpos == -1L)
                return false;
            if (dc.structure >= 0 && dc.rowType.Count == cs.Count)
            {
                var cb = cs.First();
                for (var c = dc.rowType.First(); c != null; c = c.Next(), cb = cb.Next())
                    if (cx.obs[c.value()].domain.CompareTo(cb.value()) != 0)
                        return false;
            }
            return true;
        }
        protected virtual PDomain New(Writer wr)
        {
            return new PDomain(this, wr);
        }
        protected override Physical Relocate(Writer wr)
        {
            return New(wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr) //LOCKED
		{
            if (domain.kind == Sqlx.UNION)
                throw new PEException("PE916");
            wr.PutString(domain.name);
            wr.PutInt((int)domain.kind);
            wr.PutInt(domain.prec);
            wr.PutInt(domain.scale);
            wr.PutInt((int)domain.charSet);
            wr.PutString(domain.culture.Name);
            wr.PutString(domain.defaultString);
            if (domain.kind == Sqlx.ARRAY || domain.kind == Sqlx.MULTISET)
                wr.PutLong(wr.cx.db.types[domain.elType]);
            else
                wr.PutLong(domain.structure);
 			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            var nm = rdr.GetString();
            var kind = (Sqlx)rdr.GetInt();
            domain = new Domain(kind, BTree<long, object>.Empty
                + (Basis.Name, nm)
                + (Domain.Precision, rdr.GetInt())
                + (Domain.Scale, rdr.GetInt())
                + (Domain.Charset, (CharSet)rdr.GetInt())
                + (Domain.Culture, GetCulture(rdr.GetString())));
            var ds = rdr.GetString();
            TypedValue dv = TNull.Value;
            if (ds.Length>0 
                && kind == Sqlx.CHAR && ds[0] != '\'')
                ds = "'" + ds + "'";
            if (ds != "")
                try {
                    dv = Domain.For(kind).Parse(rdr.context.db.uid, ds);
                } catch(Exception) { }
            domain = domain + (Domain.DefaultString, ds)
                + (Domain.Default, dv);
            var ep = rdr.GetLong();
            if (ep >= 0)
            {
                if (kind == Sqlx.ARRAY || kind == Sqlx.MULTISET)
                    domain += (Domain.Element, ep);
                else
                {
                    var tb = (Table)rdr.context.db.objects[ep];
                    var rs = CTree<long, Domain>.Empty;
                    for (var b = tb.domain.rowType.First(); b != null; b = b.Next())
                    {
                        var tc = (TableColumn)rdr.context.db.objects[b.value()];
                        rs += (b.value(), tc.domain);
                    }
                    domain = domain + (Domain.Structure, ep)
                        + (Domain.RowType, tb.domain.rowType)
                        + (Domain.Representation, rs);
                }
            }
            base.Deserialise(rdr);
        }
        CultureInfo GetCulture(string s)
        {
            if (s == "" || s == "UCS_BINARY")
                return CultureInfo.InvariantCulture;
#if SILVERLIGHT||WINDOWS_PHONE
            return new CultureInfo(s);
#else
            return CultureInfo.GetCultureInfo(s);
#endif
        }
        public override DBException Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            var nm = domain.name;
            switch(that.type)
            {
                case Type.PDomain1:
                case Type.PDomain:
                    if (nm == ((PDomain)that).domain.name)
                        return new DBException("40022", ppos, that, ct);
                    break;
                case Type.PTable:
                case Type.PTable1:
                    if (nm == ((PTable)that).name)
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.PView1:
                case Type.PView:
                    if (nm == ((PView)that).name)
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.PRole:
                    if (nm == ((PRole)that).name)
                        return new DBException("40035", ppos, that, ct);
                    break;
                case Type.RestView1:
                case Type.RestView:
                    if (nm == ((PRestView)that).name)
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.PType:
                    if (nm == ((PType)that).domain.name)
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.Change:
                    if (nm == ((Change)that).name)
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.Drop:
                    if (defpos==((Drop)that).delpos)
                        return new DBException("40016", ppos, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that, ct);
        }
        /// <summary>
        /// A readable version of the Physical
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            return domain.ToString();       
        }
        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            var dt = new Domain(this);
            var priv = Grant.Privilege.Usage | Grant.Privilege.GrantUsage;
            var oi = new ObInfo(ppos, domain.name, domain) + (ObInfo.Privilege, priv);
            ro += (oi,true);
            if (domain.name != "")
                ro = ro + (Role.DBObjects, ro.dbobjects + (domain.name, ppos));
            if (cx.db.format<51 && domain.structure > 0)
                ro += (Role.DBObjects, ro.dbobjects 
                    + ("" + domain.structure, domain.structure));
            cx.db = cx.db + (ro,p) + (ppos,dt,p);
            cx.db += (Database.Types, cx.db.types + (dt-Domain.Representation, ppos));
        }
    }

    /// <summary>
    /// used for Interval types
    /// </summary>
    internal class PDateType : PDomain
    {
         /// <summary>
        /// Constructor: a new date type from the Parser
        /// </summary>
        /// <param name="nm">the name of the type</param>
        /// <param name="ki">the base date type</param>
        /// <param name="st">the starting field</param>
        /// <param name="en">the ending field</param>
        /// <param name="pr">the precision</param>
        /// <param name="sc">the scale</param>
        /// <param name="dv">the default value</param>
        /// <param name="pb">the local database</param>
        public PDateType(string nm, Sqlx ki, Sqlx st, Sqlx en, int pr, byte sc, string dv, 
            long pp, Context cx)
            : base(Type.PDateType,nm,ki,pr,sc,CharSet.UCS,"",dv,
                  -1L, pp, cx)
        {
            domain = domain + (Domain.Start, st) + (Domain.End, en);
        }
        /// <summary>
        /// Constructor: a date type from the file buffer
        /// </summary>
        /// <param name="bp">the buffer</param>
        /// <param name="pos">the buffer position</param>
        public PDateType(Reader rdr) : base(Type.PDateType, rdr) { }
        protected PDateType(PDateType x, Writer wr) : base(x, wr) {  }
        protected override Physical Relocate(Writer wr)
        {
            return new PDateType(this, wr);
        }
        /// <summary>
        /// Serialise a date type descriptor
        /// </summary>
        /// <param name="r">the relocation information</param>
        public override void Serialise(Writer wr) //locked
        {
            wr.PutInt(IntervalPart(domain.start));
            wr.PutInt(IntervalPart(domain.end));
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise a date type descriptor
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            Sqlx start = GetIntervalPart(rdr);
            Sqlx end = GetIntervalPart(rdr);
            base.Deserialise(rdr);
        }
        /// <summary>
        /// Facilitate quick decoding of the interval fields
        /// </summary>
        internal static Sqlx[] intervalParts = new Sqlx[] { Sqlx.YEAR, Sqlx.MONTH, Sqlx.DAY, Sqlx.HOUR, Sqlx.MINUTE, Sqlx.SECOND };
        /// <summary>
        /// helper for encoding interval fields
        /// </summary>
        /// <param name="e">YEAR, MONTH, DAY, HOUR, MINUTE, SECOND</param>
        /// <returns>corresponding integer 0,1,2,3,4,5</returns>
        internal static int IntervalPart(Sqlx e)
        {
            switch (e)
            {
                case Sqlx.YEAR: return 0;
                case Sqlx.MONTH: return 1;
                case Sqlx.DAY: return 2;
                case Sqlx.HOUR: return 3;
                case Sqlx.MINUTE: return 4;
                case Sqlx.SECOND: return 5;
            }
            return -1;
        }
        Sqlx GetIntervalPart(Reader rdr)
        {
            int j = rdr.GetInt();
            if (j < 0)
                return Sqlx.NULL;
            return intervalParts[j];
        }
        public override string ToString()
        {
            return "PDateType " + base.ToString();
        }
    }

    internal class PDomain1 : PDomain
    {
        public PDomain1(string nm, Domain dt, long pp, Context cx)
            : base(Type.PDomain1, nm, dt, pp, cx) { }
        /// <summary>
        /// Constructor: a data type from the file buffer
        /// </summary>
        /// <param name="bp">the buffer</param>
        /// <param name="pos">the buffer position</param>
        public PDomain1(Reader rdr) : this(Type.PDomain1, rdr) { }
        protected PDomain1(Type t, Reader rdr) : base(t, rdr) { }
        protected PDomain1(PDomain1 x, Writer wr) : base(x, wr) {  }
        protected override Physical Relocate(Writer wr)
        {
            return new PDomain1(this, wr);
        }
        /// <summary>
        /// Deserialise an OWL domain descriptor
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            string ur = rdr.GetString();
            string ab = rdr.GetString();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return "PDomain1 " + base.ToString();
        }
    }
}
