using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System.Collections.ObjectModel;
using System.Configuration;
using System.Text;

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
	/// A View definition
	/// </summary>
    internal class PView : Compiled
    {
        /// <summary>
        /// The name of the View
        /// </summary>
        public string name;
        /// <summary>
        /// The definition of the view
        /// </summary>
        public string viewdef;
        public override long Dependent(Writer wr, Transaction tr)
        {
            for (var b = framing.obs.First(); b != null; b = b.Next())
                if (b.value() is TableColumn tc)
                {
                    if (!Committed(wr, tc.tabledefpos))
                        return tc.tabledefpos;
                    if (!Committed(wr, tc.defpos))
                        return tc.defpos;
                }
            return -1;
        }
        /// <summary>
        /// Constructor: A view definition from the Parser
        /// </summary>
        /// <param name="tp">The PView type</param>
        /// <param name="nm">The name of the view</param>
        /// <param name="vd">The definition of the view</param>
        /// <param name="pb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        internal PView(string nm, string vd, Domain dm, long pp, Context cx)
            : this(Type.PView, nm, vd, dm, pp, cx) 
        { }
        protected PView(Type pt,string nm,string vd, Domain dm, long pp, Context cx) 
            : base(pt,pp,cx,new Framing(cx),dm)
        {
            name = nm;
            viewdef = vd;
        }
        /// <summary>
        /// Constructor: A view definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
        public PView(ReaderBase rdr) : base(Type.PView, rdr) { }
        protected PView(Type tp, ReaderBase rdr) : base(tp, rdr) { }
        protected PView(PView x, Writer wr) : base(x, wr)
        {
            name = x.name;
            wr.srcPos = wr.Length + 1;
            viewdef = x.viewdef;
            domain = (Domain)x.domain.Relocate(wr);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PView(this, wr);
        }

        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr)
        {
            wr.PutString(name);
            wr.PutString(viewdef);
            base.Serialise(wr);
        }
        /// <summary>
        /// deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(ReaderBase rdr)
        {
            name = rdr.GetString();
            viewdef = rdr.GetString();
            base.Deserialise(rdr);
        }
        internal override void OnLoad(Reader rdr)
        {
            if (viewdef!="")
            {
                var psr = new Parser(rdr, new Ident(viewdef, ppos + 2), null);
                var ss = psr.ParseCursorSpecification(Domain.TableType);
                var cs = (CursorSpecification)psr.cx.obs[ss.cs];
                psr.cx._Add(cs);
                psr.cx.data += (ppos, psr.cx.data[psr.cx.result]);
                Frame(psr.cx);
                domain = psr.cx.data[psr.cx.result].domain;
            }
        }
        internal virtual BTree<long,object> _Dom(Database db,BTree<long,object>m)
        {
            var ns = CTree<string, CTree<long,long>>.Empty;
            var d = 2 + (domain?.depth ?? 0);
            for (var b = domain?.rowType.First(); b != null && b.key()<domain.display; 
                b = b.Next())
            {
                var p = b.value();
                var c = (SqlValue)framing.obs[p];
                d = DBObject._Max(d, 1 + c.depth);
                var r = ns[c.name] ?? CTree<long, long>.Empty;
                var tg = ((From)framing.obs[c.from]).target; 
                ns += (c.name, r+(tg,p));
                if (c.alias != null)
                {
                    r = ns[c.alias] ?? CTree<long, long>.Empty;
                    ns += (c.alias, r+(tg,p));
                }
            }
            var rs = framing.data[framing.result];
            var ts = CList<long>.Empty;
            for (var b = rs.rsTargets.First(); b != null; b = b.Next())
                ts += b.key();
            return (m??BTree<long,object>.Empty) 
                + (DBObject._Domain, domain) 
                + (View.ViewPpos, ppos) + (View.ViewCols, ns) 
                + (View.Targets,ts) 
                + (DBObject._Framing, framing)
                + (DBObject.Depth, d);
        } 
        /// <summary>
        /// a readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            sb.Append(" ");sb.Append(name); 
            sb.Append(" "); sb.Append(DBObject.Uid(ppos));
            sb.Append(" "); sb.Append(viewdef);
            return sb.ToString();
        }
        public override DBException Conflicts(Database db, Context cx, Physical that,PTransaction ct)
        {
            switch(that.type)
            {
                case Type.PTable1:
                case Type.PTable:
                    if (name == ((PTable)that).name)
                        return new DBException("40030", ppos, that, ct);
                    break;
                case Type.PView1:
                case Type.PView:
                case Type.RestView1:
                case Type.RestView2:
                case Type.RestView:
                    if (name == ((PView)that).name)
                        return new DBException("40012", ppos, that, ct);
                    break;
                case Type.Change:
                    if (name == ((Change)that).name)
                        return new DBException("40032", ppos, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that, ct);
        }
        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            // The definer is the given role
            var priv = Grant.Privilege.Owner | Grant.Privilege.Insert | Grant.Privilege.Select |
                Grant.Privilege.Update | Grant.Privilege.Delete | 
                Grant.Privilege.GrantDelete | Grant.Privilege.GrantSelect |
                Grant.Privilege.GrantInsert |
                Grant.Privilege.Usage | Grant.Privilege.GrantUsage;
            var vw = new View(this,cx.db)._Refs(ppos,cx);
            var ti = new ObInfo(ppos, name, domain, priv);
            ro = ro + (ti, true) + (Role.DBObjects, ro.dbobjects + (name, ppos));
            cx.db = cx.db + (ro,p)+ (vw,p);
            cx.db += (Database.Log, cx.db.log + (ppos, type));
            cx.Install(vw, p);
        }
        public override (Transaction, Physical) Commit(Writer wr, Transaction t)
        {
            var (tr, ph) = base.Commit(wr, t);
            if (this is PRestView)
                return (tr, ph);
            var pv = (PView)ph;
            var vw = (DBObject)((DBObject)tr.objects[ppos])._Relocate(wr) 
                + (DBObject._Framing, pv.framing);
            vw = ((View)vw)._Refs(ph.ppos,wr.cx);
            return ((Transaction)(tr + (vw, tr.loadpos)), ph);
        }
    }
    internal class PRestView : PView
    {
        internal long structpos,usingtbpos = -1L;
        internal string rname = null, rpass = null;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr, usingtbpos)) return usingtbpos;
            return -1;
        }
        public PRestView(ReaderBase rdr) : this(Type.RestView, rdr) { }
        protected PRestView(Type t, ReaderBase rdr) : base(t,rdr) { }
        public PRestView(string nm, long tp, Domain dm, long pp, Context cx)
            : this(Type.RestView, nm, tp, dm, pp, cx) { }
        protected PRestView(Type t,string nm,long tp,Domain dm,long pp, Context cx)
            : base(t,nm,"",dm,pp,cx)
        {
            structpos = tp;
        }
        protected PRestView(PRestView x, Writer wr) : base(x, wr)
        {
            structpos = wr.Fix(x.structpos);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PRestView(this, wr);
        }

        public override void Serialise(Writer wr)
        {
            wr.PutLong(structpos);
            base.Serialise(wr);
        }
        public override void Deserialise(ReaderBase rdr)
        {
            structpos = rdr.GetLong();
            base.Deserialise(rdr);
        }
        internal override void OnLoad(Reader rdr)
        {
            var db = rdr.context.db;
            var ro = db.role;
            var os = BTree<long, DBObject>.Empty;
            var ds = Ident.Idents.Empty;
            var tb = (Table)db.objects[structpos];
            var ti = (ObInfo)ro.infos[structpos];
            var cs = Ident.Idents.Empty;
            os += (structpos, tb);
            domain = ti.domain;
            for (var b=domain.rowType.First();b!=null;b=b.Next())
            {
                var cp = b.value();
                var ci = (ObInfo)ro.infos[cp];
                var tc = (DBObject)db.objects[cp];
                cs += (ci.name, cp, Ident.Idents.Empty);
                ds += (ci.name, cp, Ident.Idents.Empty);
                os += (cp, tc);
            }
            // fix r.union and lower structures during Instancing
            framing = Framing.Empty + (Framing.Obs, os);
        }
        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            // The definer is the given role
            var priv = Grant.Privilege.Owner | Grant.Privilege.Insert | Grant.Privilege.Select |
                Grant.Privilege.Update | Grant.Privilege.Delete |
                Grant.Privilege.GrantDelete | Grant.Privilege.GrantSelect |
                Grant.Privilege.GrantInsert |
                Grant.Privilege.Usage | Grant.Privilege.GrantUsage;
            var vw = new RestView(this, cx.db)._Refs(ppos,cx);
            var ti = new ObInfo(ppos, name, Domain.TableType, priv);
            ro = ro + (ti, true) + (Role.DBObjects, ro.dbobjects + (name, ppos));
            cx.db = cx.db + (ro, p) + (vw, p);
            cx.Install(vw, p);
        }
        internal override BTree<long, object> _Dom(Database db,BTree<long,object>m)
        {
            var ns = CTree<string, CTree<long,long>>.Empty;
            for (var b = domain.rowType.First(); b != null && b.key()<domain.display; 
                b = b.Next())
            {
                var p = b.value();
                var c = (ObInfo)db.role.infos[p];
                var r = ns[c.name] ?? CTree<long, long>.Empty;
                ns += (c.name, r + (ppos,p));
            } 
            return (m??BTree<long,object>.Empty) + (DBObject._Domain, domain)
                + (View.ViewPpos, ppos) + (View.ViewCols, ns)
                + (DBObject._Framing, framing)
                + (DBObject.Depth, 2);
        }
        public override string ToString()
        {
            return "PRestView "+name + "["+DBObject.Uid(structpos)+"]";
        }
    }
    /// <summary>
    /// This class is deprecated: credentials information can be safely provided in URL
    /// </summary>
    internal class PRestView1 : PRestView
    {
        public PRestView1(ReaderBase rdr) : base(Type.RestView1, rdr) { }
        public PRestView1(string nm, long tp, Domain dm, string rnm, string rpw, long pp, 
            Context cx) : base(Type.RestView1, nm, tp, dm, pp, cx)
        {
            rname = rnm;
            rpass = rpw;
        }
        protected PRestView1(PRestView1 x, Writer wr) : base(x, wr)
        {
            rname = x.rname;
            rpass = x.rpass;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PRestView1(this, wr);
        }

        public override void Serialise(Writer wr)
        {
            wr.PutString(rname);
            wr.PutString(rpass);
            base.Serialise(wr);
        }
        public override void Deserialise(ReaderBase rdr)
        {
            rname = rdr.GetString();
            rpass = rdr.GetString();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return "PRestView1 " + name + "(" + structpos + ") '" +rname+"':'"+rpass +"'";
        }
    }
    internal class PRestView2 : PRestView
    {
        public PRestView2(ReaderBase rdr) : base(Type.RestView1, rdr) { }
        public PRestView2(string nm, long tp, Domain dm, long utp, long pp, Context cx)
            : base(Type.RestView2, nm, tp, dm, pp, cx)
        {
            usingtbpos = utp;
        }
        protected PRestView2(PRestView2 x, Writer wr) : base(x, wr)
        {
            usingtbpos = wr.Fix(x.usingtbpos);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PRestView2(this, wr);
        }

        public override void Serialise(Writer wr)
        {
            wr.PutLong(usingtbpos);
            base.Serialise(wr);
        }
        public override void Deserialise(ReaderBase rdr)
        {
            usingtbpos = rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override DBException Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            if (that.type == Type.Drop && usingtbpos == ((Drop)that).delpos)
                return new DBException("40012",usingtbpos, that, ct);
            return base.Conflicts(db, cx, that, ct);
        }
        public override string ToString()
        {
            return "PRestView2 " + name + "(" + structpos + ") using " + usingtbpos;
        }
    }

    /// <summary>
    /// This class is obsolete: deserialisation of View1 definitions from a database file is supported for backward compatibility
    /// </summary>
    internal class PView1 : PView
    {
        /// <summary>
        /// Constructor: A view definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
        public PView1(ReaderBase rdr) : base(Type.PView1,rdr) { }
        protected PView1(PView1 x, Writer wr) : base(x, wr) { }
        protected override Physical Relocate(Writer wr)
        {
            return new PView1(this, wr);
        }

        /// <summary>
        /// deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(ReaderBase rdr)
        {
            rdr.GetString();
            rdr.GetString();
            rdr.GetString();
            base.Deserialise(rdr);
        }
    }
}
