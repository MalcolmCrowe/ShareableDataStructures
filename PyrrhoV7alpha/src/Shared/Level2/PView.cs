using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System.Collections.ObjectModel;
using System.Configuration;
using System.Text;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
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
            : base(pt,pp,cx,pp,dm)
        {
            name = nm;
            viewdef = vd;
        }
        /// <summary>
        /// Constructor: A view definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
        public PView(Reader rdr) : base(Type.PView, rdr) { }
        protected PView(Type tp, Reader rdr) : base(tp, rdr) { }
        protected PView(PView x, Writer wr) : base(x, wr)
        {
            name = x.name;
            viewdef = x.viewdef;
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
        public override void Deserialise(Reader rdr)
        {
            name = rdr.GetString();
            viewdef = rdr.GetString();
            base.Deserialise(rdr);
        }
        internal override void OnLoad(Reader rdr)
        {
            if (viewdef!="")
            {
                var psr = new Parser(rdr, 
                    new Ident(viewdef, rdr.context.Ix(ppos + 2)), null);
                psr.cx.nextStmt = rdr.context.nextStmt;
                psr.Next(); psr.Next();  // VIEW name
                var un = psr.ParseViewDefinition(name);
         //       var cs = psr.ParseCursorSpecification(Domain.TableType);
                dataType = psr.cx._Dom(psr.cx.obs[un.defpos]); // was cs.union
                rdr.context.nextStmt = psr.cx.nextStmt;
                psr.cx.result = un.defpos;
                framing = new Framing(psr.cx);
            }
        }
        internal virtual BTree<long,object> _Dom(Context cx,BTree<long,object>m)
        {
            var d = 2 + (dataType?.depth ?? 0);
            for (var b = dataType?.rowType.First(); b != null && b.key()<dataType.display; 
                b = b.Next())
            {
                var p = b.value();
                var c = (SqlValue)framing.obs[p];
                if (c!=null)
                    d = DBObject._Max(d, 1 + c.depth);
            }
/*            var rs = (RowSet)framing.obs[framing.result];
            var ts = CList<long>.Empty;
            for (var b = rs.rsTargets.First(); b != null; b = b.Next())
                ts += b.key(); */
            return (m??BTree<long,object>.Empty) 
                + (DBObject._Domain,dataType.defpos)
                + (View.ViewPpos, ppos)
                + (DBObject._Framing, framing)
                + (DBObject._Depth, d);
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
                        return new DBException("40030", name, that, ct);
                    break;
                case Type.PView1:
                case Type.PView:
                case Type.RestView1:
                case Type.RestView2:
                case Type.RestView:
                    if (name == ((PView)that).name)
                        return new DBException("40012", name, that, ct);
                    break;
                case Type.Change:
                    if (name == ((Change)that).name)
                        return new DBException("40032", name, that, ct);
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
            var vw = new View(this,cx);
            var ti = new ObInfo(ppos, name, dataType, priv);
            ti += (ObInfo.SchemaKey, p);
            ro = ro + (ti, true) + (Role.DBObjects, ro.dbobjects + (name, ppos));
            cx.db = cx.db + (ro,p)+ (vw,p);
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            cx.Install(vw, p);
            base.Install(cx, p);
        }
        public override (Transaction, Physical) Commit(Writer wr, Transaction t)
        {
            var (tr, ph) = base.Commit(wr, t);
            if (this is PRestView)
                return (tr, ph);
            var pv = (PView)ph;
            var vw = ((DBObject)tr.objects[ppos]).Relocate(wr.cx) 
                + (DBObject._Framing, pv.framing.Fix(wr.cx));
            wr.cx.instDFirst = -1;
            return ((Transaction)(tr + (vw, tr.loadpos)), ph);
        }
    }
    internal class PRestView : PView
    {
        internal long structpos,usingtbpos = -1L;
        internal string rname = null, rpass = null;
        internal long usingTableRowSet = -1L;
        internal CTree<string,long> names = CTree<string,long>.Empty;
        internal CTree<long,string> namesMap = CTree<long,string>.Empty;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr, usingtbpos)) return usingtbpos;
            return -1;
        }
        public PRestView(Reader rdr) : this(Type.RestView, rdr) { }
        protected PRestView(Type t, Reader rdr) : base(t,rdr) { }
        public PRestView(string nm, long tp, Domain dm, long pp, Context cx)
            : this(Type.RestView, nm, tp, dm, pp, cx) { }
        protected PRestView(Type t,string nm,long tp,Domain dm,long pp, Context cx)
            : base(t,nm,"",dm,pp,cx)
        {
            structpos = tp;
            for (var b = dm.rowType.First();b!=null;b=b.Next())
            {
                var p = b.value();
                var c = cx.obs[p];
                names += (c.name,p);
                namesMap += (p, c.name);
            }
        }
        protected PRestView(PRestView x, Writer wr) : base(x, wr)
        {
            structpos = wr.cx.Fix(x.structpos);
            namesMap = wr.cx.Fix(x.namesMap);
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
        public override void Deserialise(Reader rdr)
        {
            structpos = rdr.GetLong();
            base.Deserialise(rdr);
        }
        internal override void OnLoad(Reader rdr)
        {
            var db = rdr.context.db;
            var tb = (Table)db.objects[structpos];
            var psr = new Parser(rdr.context, tb.name);
            var op = rdr.context.parse;
            psr.cx.parse = ExecuteStatus.Parse;
            dataType = psr.ParseRestViewSpec(structpos);
            rdr.context.parse = op;
            rdr.context.nextStmt = psr.cx.nextStmt;
            framing = new Framing(psr.cx);
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
            var ti = new ObInfo(ppos, name, dataType, priv);
            ti += (ObInfo.SchemaKey, p);
            var vt = (VirtualTable)cx.db.objects[structpos] + (VirtualTable._RestView,ppos)
                +(ObInfo._DataType,dataType);
            var vi = (ObInfo)ro.infos[structpos]+(ObInfo._DataType,dataType);
            ro = ro + (ti, true) + (vi,true) + (Role.DBObjects, ro.dbobjects + (name, ppos));
            var rv = new RestView(this, cx);
            cx.db = cx.db + (ro, p) + (rv, p) + (vt,p);
            cx.Install(rv, p);
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
        public PRestView1(Reader rdr) : base(Type.RestView1, rdr) { }
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
        public override void Deserialise(Reader rdr)
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
        public PRestView2(Reader rdr) : base(Type.RestView2, rdr) { }
        public PRestView2(string nm, long tp, Domain dm, RowSet uf, long pp, Context cx)
            : base(Type.RestView2, nm, tp, dm, pp, cx)
        {
            usingtbpos = uf.target;
            usingTableRowSet = uf.rsTargets.First().value();
            FixCols(cx);
        }
        protected PRestView2(PRestView2 x, Writer wr) : base(x, wr)
        {
            usingtbpos = wr.cx.Fix(x.usingtbpos);
            usingTableRowSet = wr.cx.Fix(x.usingTableRowSet);
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
        public override void Deserialise(Reader rdr)
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
        // Identify the remote columns of the restview and adjust the framing
        void FixCols(Context cx)
        {
            var vs = BTree<string, DBObject>.Empty;
            for (var b = dataType.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var c = cx.obs[p];
                vs += (c.name, c);
            }
            var ts = (TableRowSet)cx.obs[usingTableRowSet];
            for (var b=cx._Dom(ts).rowType.First(); b != null; b=b.Next())
            {
                var p = b.value();
                var c = cx.obs[p];
                if (vs[c.name] is DBObject oc)
                    cx.Replace(oc,c);
            }
            framing = new Framing(cx);
        }
        internal override void OnLoad(Reader rdr)
        {
            var db = rdr.context.db;
            var tb = (Table)db.objects[structpos];
            var psr = new Parser(rdr.context, tb.name);
            var op = rdr.context.parse;
            psr.cx.parse = ExecuteStatus.Parse;
            dataType = new Domain(psr.cx,
                psr.ParseRowTypeSpec(Sqlx.VIEW) + (Domain.Structure, structpos));
            var ut = (Table)db.objects[usingtbpos];
            var ic = new Ident(ut.name, psr.cx.GetIid());
            usingTableRowSet = new From(ic, psr.cx, ut).source;
            rdr.context.parse = op;
            rdr.context.nextStmt = psr.cx.nextStmt;
            framing = new Framing(psr.cx);
            FixCols(psr.cx);
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
        public PView1(Reader rdr) : base(Type.PView1,rdr) { }
        protected PView1(PView1 x, Writer wr) : base(x, wr) { }
        protected override Physical Relocate(Writer wr)
        {
            return new PView1(this, wr);
        }

        /// <summary>
        /// deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            rdr.GetString();
            rdr.GetString();
            rdr.GetString();
            base.Deserialise(rdr);
        }
    }
}
