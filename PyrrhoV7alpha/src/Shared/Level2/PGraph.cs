// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
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
    /// PNodeType adds PTable behaviour to PType
    /// </summary>
    internal class PNodeType : PType2
    {
        internal PNodeType(string nm,PType pt,NodeType dm,Context cx)
            :base(Type.PNodeType,nm,dm+(ObInfo.Name,nm),dm.super,-1L,pt.ppos,cx)
        { }
        protected PNodeType(Type t, string nm, NodeType nt, CTree<Domain,bool> un, long ns, long pp, Context cx)
            : base(t, nm, (NodeType)nt.New(pp, nt.mem + (ObInfo.Name, nm)), un, ns, pp, cx)
        {
            ((NodeType)dataType).AddNodeOrEdgeType(cx);
        }
        public PNodeType(string nm, NodeType nt, CTree<Domain,bool> un, long ns, long pp, Context cx,
            bool ifN = false)
            : base(Type.PNodeType, nm, (NodeType)nt.New(pp, nt.mem + (ObInfo.Name, nm)), un, ns, pp, cx)
        {
            ifNeeded = ifN;
            ((NodeType)dataType).AddNodeOrEdgeType(cx);
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
    }
    /// <summary>
    /// PEdgeType adds Edge characteristics to PNodeType
    /// </summary>
    internal class PEdgeType : PNodeType
    {
        public long leavingType = -1L;
        public long arrivingType = -1L;
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
        internal override void OnLoad(Reader rdr)
        {
            base.OnLoad(rdr);
        }
        public override void Serialise(Writer wr)
        {
            wr.PutLong(leavingType);
            wr.PutLong(arrivingType);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            leavingType = rdr.GetLong();
            arrivingType = rdr.GetLong();
            base.Deserialise(rdr);
            var et = (EdgeType)dataType;
            et.Fix(rdr.context); // add this edge type to the catalogue
            dataType = et;
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
            var ns = CTree<long, TNode>.Empty;
            for (var b = records.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is Record r)
                {
                    var t = new TableRow(r, cx);
                    if (cx.db.objects[r.tabledefpos] is NodeType nt)
                    {
                        if (nt is JoinedNodeType)
                            for (var c = (r as Record4)?.extraTables.First(); c != null; c = c.Next())
                                ns += (c.key(), new TNode(cx, t));
                        else
                            ns += (r.defpos, nt.Node(cx, t));
                    }
                }
            var g = new Graph(this,cx,0L);
            cx.db += g;
            var ro = cx.role;
            ro += (Role.Graphs, ro.graphs + (name, ppos));
            cx.db += g;
            cx.db += ro;
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
    internal class PGraphType : Physical
    {
        public string iri = "";
        public string name = ""; // final component of iri, set in Graph constructor
        public CTree<long, bool> types = CTree<long, bool>.Empty;
        public PGraphType(long pp,  string s, CTree<long, bool> ts, Database d)
            : base(Type.PGraphType, pp, d)
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
            {
                name = iri[(ix + 1)..];
                iri = iri[0..ix];
            }
            var n = rdr.GetInt();
            for (var i = 0; i < n; i++)
                types += (rdr.GetLong(), true);
            base.Deserialise(rdr);
        }
        public override void Serialise(Writer wr)
        {
            var nm = iri;
            if (name != "")
            {
                if (!iri.EndsWith("/"))
                    nm = iri + "/" + name;
                else
                    nm = iri + name;
            }
            wr.PutString(iri);
            wr.PutInt((int)types.Count);
            for (var b = types.First(); b != null; b = b.Next())
                wr.PutLong(b.key());
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
            var g = new GraphType(this, cx, 0L);
            cx.db += g;
            var ro = cx.role;
            ro += (Role.Graphs, ro.graphs + (name, ppos));
            cx.db += g;
            cx.db += ro;
            cx.Add(ro);
            cx.Add(g);
            cx.graph = g;
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
                sb.Append(cm); cm = ","; sb.Append(DBObject.Uid(b.key()));
            }
            if (cm == ",")
                sb.Append(']');
            return sb.ToString();
        }
    }
    internal class PSchema : Physical
    {
        public string directoryPath = ""; // may begin with http:// etc
        public PSchema(long pp, string s, Database d) : base(Type.PSchema, pp, d)
        {
            directoryPath = s;
        }
        public PSchema(Reader rdr) : base(Type.PSchema, rdr)
        { }
        public PSchema(PSchema x, Writer wr) : base(x, wr)
        {
            directoryPath = x.directoryPath;
        }
        public override void Deserialise(Reader rdr)
        {
            directoryPath = rdr.GetString();
            base.Deserialise(rdr);
        }
        public override void Serialise(Writer wr)
        {
            wr.PutString(directoryPath);
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
            var g = new Schema(this,cx);
            var ro = cx.role;
            ro += (Role.Schemas, ro.schemas + (directoryPath, ppos));
            cx.db += g;
            cx.db += ro;
            cx.Add(ro);
            cx.Add(g);
            cx.schema = g;
            return g;
        }
        public override (Transaction?, Physical) Commit(Writer wr, Transaction? tr)
        {
            if (directoryPath.StartsWith("http")) // do not commit
                return (tr, this);
            return base.Commit(wr, tr);
        }
        public override string ToString()
        {
            return "PSchema "+directoryPath;
        }
    }
}