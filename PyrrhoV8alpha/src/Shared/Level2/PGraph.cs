// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2026
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Level5;
using System.Text;
namespace Pyrrho.Level2
{
    /// <summary>
    /// PNodeType defines a user defined type that will be installed as a node type in the current role:
    /// at the Database level the corresponding dataType is UDType.
    /// </summary>
    internal class PNodeType : PType2
    {
        internal PNodeType(string nm,PType pt,NodeType dm,Context cx)
            :base(Type.PNodeType,nm,dm+(ObInfo.Name,nm),dm.super,-1L,pt.ppos,cx)
        { }
        protected PNodeType(Type t, string nm, NodeType nt, CTree<Domain,bool> un, long ns, long pp, Context cx)
            : base(t, nm, (NodeType)nt.New(pp, nt.mem + (ObInfo.Name, nm)), un, ns, pp, cx)
        {
        }
        public PNodeType(string nm, NodeType nt, CTree<Domain,bool> un, long ns, long pp, Context cx,
            bool ifN = false)
            : base(Type.PNodeType, nm, (NodeType)nt.New(pp, nt.mem + (ObInfo.Name, nm)), un, ns, pp, cx)
        {
            ifNeeded = ifN;
        }
        public PNodeType(Reader rdr) : base(Type.PNodeType, rdr) 
        { }
        protected PNodeType(Type t, Reader rdr):base(t, rdr) { }
        protected PNodeType(PNodeType x,Writer wr) :base(x,wr) 
        { }
        public override (Transaction?, Physical) Commit(Writer wr, Transaction? tr)
        {
            return base.Commit(wr, tr);
        }
        internal override bool NeededFor(BTree<long, Physical> physicals)
        {
            if (!ifNeeded)
                return true;
            for (var b = physicals.First(); b != null; b = b.Next())
                if (b.value() is Record r && r.tabledefpos == defpos)
                    return true;
            return false;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PNodeType(this,wr);
        }
        internal override DBObject Install(Context cx)
        {
            var r = (UDType)base.Install(cx);
            r.AddNodeOrEdgeType(cx);
            return r;
        }
    }
    /// <summary>
    /// PEdgeType adds Edge characteristics to PNodeType
    /// </summary>
    internal class PEdgeType : PNodeType
    {
        public long leavingProperty = -1L;
        public long arrivingProperty = -1L;
        internal PEdgeType(string nm, PType pt, NodeType dm, Context cx)
    : base(Type.PEdgeType, nm, dm + (ObInfo.Name, nm), dm.super, -1L, pt.ppos, cx)
        { }
        public PEdgeType(string nm, EdgeType nt, CTree<Domain, bool> un, long ns,
            long pp, Context cx, bool IfN = false)
            : base(Type.PEdgeType, nm, nt, un, ns, pp, cx) 
        {
            ifNeeded = IfN;
            nt = (EdgeType)dataType;
            cx.Add(nt);
            dataType = nt;
            nt.AddNodeOrEdgeType(cx);
        }
        public PEdgeType(Reader rdr) : base(Type.PEdgeType, rdr) 
        { }
        protected PEdgeType(Type t, Reader rdr) : base(t, rdr) { }
        protected PEdgeType(PEdgeType x, Writer wr) : base(x, wr) 
        {
            dataType = (Domain)x.dataType.Fix(wr.cx);
            wr.cx.Add(dataType);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PEdgeType(this, wr);
        }
        public override void Serialise(Writer wr)
        {
            wr.PutLong(leavingProperty);
            wr.PutLong(arrivingProperty);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            leavingProperty = rdr.GetLong();
            arrivingProperty = rdr.GetLong();
            base.Deserialise(rdr);
            dataType.Fix(rdr.context); // add this edge type to the catalogue
        }
    }
    /*    internal class PGraph : Physical
        {
            public string iri = "";
            public string name = ""; 
            public CTree<long, bool> types = CTree<long,bool>.Empty;
            public CTree<long, TNode> records = CTree<long, TNode>.Empty;
            public PGraph(long pp,string s,CTree<long,bool> ts,CTree<long,TNode> ns) 
                : base(Type.PGraph, pp)
            {
                iri = s; // will be split by Graph constructor
                types = ts;
                records = ns;
            }
            public PGraph(Reader rdr) : base(Type.PGraph, rdr)
            { }
            public PGraph(PGraph x, Writer wr) : base(x, wr)
            {
                iri = x.iri;
                types = wr.cx.Fix(x.types);
                records = wr.cx.Fix(x.records);
            }
            public override void Deserialise(Reader rdr)
            {
                iri = rdr.GetString();
                var n = rdr.GetInt();
                for (var i = 0; i < n; i++)
                    types += (rdr.GetLong(), true);
                n = rdr.GetInt();
                for (var i = 0; i < n; i++)
                {
                    var p = rdr.GetLong();
                    if (rdr.context.db.objects[rdr.GetLong()] is not NodeType tb || tb.tableRows[p] is not TableRow tr)
                        Console.WriteLine("Warning: bad Graph record list");
                    else
                        records += (p, tb.Node(rdr.context,tr));
                }
                base.Deserialise(rdr);
            }
            public override void Serialise(Writer wr)
            {
                wr.PutString(iri);
                wr.PutInt((int)types.Count);
                for (var b = types.First(); b != null; b = b.Next())
                    wr.PutLong(b.key());
                wr.PutInt((int)records.Count);
                for (var b = records.First(); b != null; b = b.Next())
                {
                    wr.PutLong(b.value().dataType.defpos);
                    wr.PutLong(b.key());
                }
                base.Serialise(wr);
            }
            public override long Dependent(Writer wr, Transaction tr)
            {
                return -1L;
            }

            protected override Physical Relocate(Writer wr)
            {
                types = wr.cx.Fix(types);
                records = wr.cx.Fix(records);
                return new PGraph(this, wr);
            }

            internal override DBObject? Install(Context cx)
            {
                var g = new Graph(this,cx,0L);
                cx.db += g;
                var ro = cx.role;
                ro += (Role.GraphNames, ro.graphs + (name, ppos));
                cx.db += g;
                cx.db += ro;
                cx.db += (Database.Role, ro);
                cx.Add(ro);
                cx.Add(g);
                return g;
            }
            public override (Transaction?, Physical) Commit(Writer wr, Transaction? tr)
            {
                if (iri.StartsWith("http")) // do not commit
                    return (tr, this);
                return base.Commit(wr, tr);
            }
            public override string ToString()
            {
                var sb = new StringBuilder("PGraph ");
                sb.Append(name); sb.Append(" in ");
                sb.Append(iri);
                var cm = " [";
                for (var b = types.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ","; sb.Append(DBObject.Uid(b.key()));
                }
                if (cm == ",")
                    sb.Append(']');
                cm = " [";
                for (var b = records.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ","; sb.Append(DBObject.Uid(b.key()));
                }
                if (cm == ",")
                    sb.Append(']');
                return sb.ToString();
            }
        } */
    internal class PGraphType : Defined
    {
        public string iri = "";
        public CTree<string, long> types = CTree<string, long>.Empty;
        public PGraphType(long pp,  string s, CTree<string,long> ts, Context cx)
            : base(Type.PGraphType, pp, cx, s, Grant.Privilege.NoPrivilege)
        {
            iri = s;
            types = ts;
        }
        public PGraphType(Reader rdr) : base(Type.PGraphType, rdr)
        { }
        public PGraphType(PGraphType x, Writer wr) : base(x, wr)
        {
            iri = x.iri;
            name = x.name;
            types = wr.cx.Fix(x.types);
        }
        public override void Deserialise(Reader rdr)
        {
            iri = rdr.GetString();
            var ix = iri.LastIndexOf('/');
            if (ix >= 0)
                name = iri[(ix + 1)..];
            var n = rdr.GetInt();
            for (var i = 0; i < n; i++)
            {
                var t = rdr.context.db.objects[rdr.GetLong()] as Table??throw new DBException("3D000");
                types += (t.NameFor(rdr.context), t.defpos);
            }
            base.Deserialise(rdr);
        }
        public override void Serialise(Writer wr)
        {
            wr.PutString(iri);
            wr.PutInt((int)types.Count);
            for (var b = types.First(); b != null; b = b.Next())
                wr.PutLong(b.value());
            base.Serialise(wr);
        }
        public override long Dependent(Writer wr, Transaction tr)
        {
            return -1L;
        }
        protected override Physical Relocate(Writer wr)
        {
            types = wr.cx.Fix(types);
            return new PGraphType(this, wr);
        }

        internal override DBObject? Install(Context cx)
        {
            var ns = CTree<long, TNode>.Empty;
            var g = new GraphType(this,cx,0L);
            cx.db += g;
            cx.db += g;
            cx.Add(g);
            cx.graphType = g;
            return g;
        }
        public override (Transaction?, Physical) Commit(Writer wr, Transaction? tr)
        {
            if (iri.StartsWith("http")) // do not commit
                return (tr, this);
            return base.Commit(wr, tr);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("PGraphType ");
            sb.Append(iri); sb.Append('/'); sb.Append(name);
            var cm = " [";
            for (var b = types.First(); b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ","; sb.Append(DBObject.Uid(b.value()));
            }
            if (cm == ",")
                sb.Append(']');
            return sb.ToString();
        }
    }
    internal class PGraph : Defined
    {
        public PGraph(long pp, string s, Context cx) 
            : base(Type.PSchema, pp, cx, s, Grant.Privilege.NoPrivilege)
        {
            name = s;
        }
        public PGraph(Reader rdr) : base(Type.PSchema, rdr)
        { }
        public PGraph(PGraph x, Writer wr) : base(x, wr)
        { }

        protected override Physical Relocate(Writer wr)
        {
            return new PGraph(this, wr);
        }

        internal override Graph? Install(Context cx)
        {
            var g = new Graph(this, cx);
            cx.db += g;
            cx.Add(g);
            return g;
        }
        public override (Transaction?, Physical) Commit(Writer wr, Transaction? tr)
        {
            if (name.StartsWith("http")) // do not commit
                return (tr, this);
            return base.Commit(wr, tr);
        }
        public override string ToString()
        {
            return "PSchema " + name;
        }
    }
    internal class EditGraph : Physical
    {
        public long graph;
        public long table;
        public long node; // + or - 
        public EditGraph(long pp,long g, long t, long n, Context cx)
            :base(Type.EditGraph,pp,cx.db)
        {
            graph = g;
            table = t;
            node = n;
        }
        protected EditGraph(Type tp, Reader rdr) : base(tp, rdr)
        {
        }
        protected EditGraph(EditGraph x, Writer wr) : base(x, wr)
        {
            graph = wr.cx.Fix(x.graph);
            node = (x.node > 0) ? wr.cx.Fix(x.node) : -wr.cx.Fix(-x.node);
        }
        internal override DBObject? Install(Context cx)
        {
            if (cx.db.objects[graph] is Graph g && cx.db.objects[table] is Table t)
            {
                if (node > 0 && t.tableRows[node] is TableRow tr)
                    g += (Graph.Nodes, g.nodes + (node, new TNode(cx,tr)));
                else
                    g += (Graph.Nodes, g.nodes - (-node));
                cx.db += g;
            }
            return base.Install(cx);
        }
    }
    /*
    internal class PSchema : Defined
    {
        public PSchema(long pp, string s, Context cx) 
            : base(Type.PSchema, pp, cx, s, Grant.Privilege.NoPrivilege)
        {
            name = s;
        }
        public PSchema(Reader rdr) : base(Type.PSchema, rdr)
        { }
        public PSchema(PSchema x, Writer wr) : base(x, wr)
        { }

        protected override Physical Relocate(Writer wr)
        {
            return new PSchema(this, wr);
        }

        internal override DBObject? Install(Context cx)
        {
            var g = new Schema(this,cx);
            var ro = cx.role;
            ro += (Role.SchemaNames, ro.schemas + (name, ppos));
            cx.db += g;
            cx.db += ro;
            cx.db += (Database.Role, ro);
            cx.Add(ro);
            cx.Add(g);
            cx.ownerRole = g;
            return g;
        }
        public override (Transaction?, Physical) Commit(Writer wr, Transaction? tr)
        {
            if (name.StartsWith("http")) // do not commit
                return (tr, this);
            return base.Commit(wr, tr);
        }
        public override string ToString()
        {
            return "PSchema "+name;
        }
    } */
    internal class PSchema : Physical
    {
        public string name = ""; // may begin with http:// etc
        public PSchema(long pp, string s, Database d) : base(Type.PSchema, pp, d)
        {
            name = s;
        }
        public PSchema(Reader rdr) : base(Type.PSchema, rdr)
        { }
        public PSchema(PSchema x, Writer wr) : base(x, wr)
        {
            name = x.name;
        }
        public override void Deserialise(Reader rdr)
        {
            name = rdr.GetString();
            base.Deserialise(rdr);
        }
        public override void Serialise(Writer wr)
        {
            wr.PutString(name);
            base.Serialise(wr);
        }
        public override long Dependent(Writer wr, Transaction tr)
        {
            return -1L;
        }

        protected override Physical Relocate(Writer wr)
        {
            return new PSchema(this, wr);
        }

        internal override DBObject? Install(Context cx)
        {
            var g = new Schema(this, cx);
            cx.db += g;
            cx.Add(g);
            cx.schema = g;
            return g;
        }
        public override (Transaction?, Physical) Commit(Writer wr, Transaction? tr)
        {
            if (name.StartsWith("http")) // do not commit
                return (tr, this);
            return base.Commit(wr, tr);
        }
        public override string ToString()
        {
            return "PSchema " + name;
        }
    }
}