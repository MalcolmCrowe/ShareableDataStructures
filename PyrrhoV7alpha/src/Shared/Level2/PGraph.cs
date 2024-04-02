// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Level5;
using System.Xml.Linq;
namespace Pyrrho.Level2
{
    /// <summary>
    /// PNodeType adds PTable behaviour to PType
    /// </summary>
    internal class PNodeType : PType2
    {
        internal PNodeType(string nm,PType pt,NodeType dm,Context cx)
            :base(Type.PNodeType,nm,dm,dm.super,-1L,pt.ppos,cx)
        {  }
        internal PNodeType(Type t,string nm, long p, NodeType dm, Context cx)
            : base(t, nm, dm, dm.super, -1L, p, cx)
        { }
        protected PNodeType(Type t, string nm, NodeType nt, CTree<Domain,bool> un, long ns, long pp, Context cx)
            : base(t,nm,nt,un,ns, pp,cx) { }
        public PNodeType(string nm, NodeType nt, CTree<Domain,bool> un, long ns, long pp, Context cx)
            : base(Type.PNodeType, nm, nt, un, ns, pp, cx) 
        { }
        public PNodeType(Reader rdr) : base(Type.PNodeType, rdr) 
        { }
        protected PNodeType(Type t, Reader rdr):base(t, rdr) { }
        protected PNodeType(PNodeType x,Writer wr) :base(x,wr) 
        {
            var nt = (NodeType)dataType;
            dataType = (NodeType)nt.Fix(wr.cx);
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
        public long leavingType;
        public long arrivingType;
        internal PEdgeType(string nm, long p, EdgeType dm, Context cx)
            :base (Type.PEdgeType,nm, p, dm,cx)
        {
            leavingType = dm.leavingType;
            arrivingType = dm.arrivingType;
   //         GraphUse(cx, defpos, leavingType, arrivingType);
        }
        public override long Dependent(Writer wr, Transaction tr)
        {
            return base.Dependent(wr, tr);
        }
        public PEdgeType(string nm, EdgeType nt, CTree<Domain, bool> un, long ns, long pp, Context cx) 
            : base(Type.PEdgeType, nm, nt, un, ns, pp, cx) { }
        public PEdgeType(Reader rdr) : base(Type.PEdgeType, rdr) 
        { }
        protected PEdgeType(Type t, Reader rdr) : base(t, rdr) { }
        protected PEdgeType(PEdgeType x, Writer wr) : base(x, wr) 
        {
            var et = (EdgeType)dataType;
            leavingType = wr.cx.Fix(x.leavingType);
            arrivingType = wr.cx.Fix(x.arrivingType);
            dataType = (EdgeType)et.Fix(wr.cx);
   //         GraphUse(wr.cx, defpos, leavingType, arrivingType);
        }
        protected override Physical Relocate(Writer wr)
        {
            leavingType = wr.cx.Fix(leavingType);
            arrivingType = wr.cx.Fix(arrivingType);
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
            et = et + (EdgeType.LeavingType,leavingType) + (EdgeType.ArrivingType,arrivingType);
            et.Fix(rdr.context); // add this edge type to the catalogue
            dataType = et;
        }
        public override string ToString()
        {
            return base.ToString() + "(" + DBObject.Uid(leavingType) + "," + DBObject.Uid(arrivingType)+")";
        }
    }
    internal class PGraph : Physical
    {
        public string iri = "";
        public string name = ""; // placed by Graph constructor
        public CTree<long, bool> types = CTree<long,bool>.Empty;
        public CTree<long, bool> records = CTree<long, bool>.Empty;
        public PGraph(long pp,string s,CTree<long,bool> ts,CTree<long,bool> ns) 
            : base(Type.PGraph, pp)
        {
            iri = s;
            types = ts;
            records = ns;
        }

        public PGraph(Type tp, Reader rdr) : base(tp, rdr)
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
                records += (rdr.GetLong(), true);
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
            records = wr.cx.Fix(records);
            return new PGraph(this, wr);
        }

        internal override DBObject? Install(Context cx, long p)
        {
            var ns = CTree<long, TNode>.Empty;
            for (var b = records.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is Record r)
                {
                    var t = new TableRow(r, cx);
                    for (var c = r.tabledefpos.First(); c != null; c = c.Next())
                     if (cx.db.objects[c.key()] is NodeType nt)
                        ns += (r.defpos, new TNode(nt,t));
                }
            var g = new Graph(this,cx);
            cx.db += (g,p);
            var ro = cx.role;
            ro += (Role.Graphs, ro.graphs + (name, ppos));
            cx.db += (g, p);
            cx.db += (ro, p);
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
    }
    internal class PGraphType : Physical
    {
        public string iri = "";
        public string name = ""; // final component of iri, set in Graph constructor
        public CTree<long, bool> types = CTree<long, bool>.Empty;
        public PGraphType(long pp,  string s, CTree<long, bool> ts)
            : base(Type.PGraph, pp)
        {
            iri = s;
            types = ts;
        }

        public PGraphType(Type tp, Reader rdr) : base(tp, rdr)
        { }

        public PGraphType(PGraphType x, Writer wr) : base(x, wr)
        {
            iri = x.iri;
            types = wr.cx.Fix(x.types);
        }
        public override void Deserialise(Reader rdr)
        {
            iri = rdr.GetString();
            var n = rdr.GetInt();
            for (var i = 0; i < n; i++)
                types += (rdr.GetLong(), true);
            base.Deserialise(rdr);
        }
        public override void Serialise(Writer wr)
        {
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

        internal override DBObject? Install(Context cx, long p)
        {
            var ns = CTree<long, TNode>.Empty;
            var g = new GraphType(this, cx);
            cx.db += (g, p);
            var ro = cx.role;
            ro += (Role.Graphs, ro.graphs + (name, ppos));
            cx.db += (g, p);
            cx.db += (ro, p);
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
    }
    internal class PSchema : Physical
    {
        public string directoryPath = "";
        public PSchema(long pp, string s) : base(Type.PSchema, pp)
        {
            directoryPath = s;
        }

        public PSchema(Type tp, Reader rdr) : base(tp, rdr)
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

        internal override DBObject? Install(Context cx, long p)
        {
            var g = new Schema(this,cx);
            var ro = cx.role;
            ro += (Role.Schemas, ro.schemas + (directoryPath, ppos));
            cx.db += (g, p);
            cx.db += (ro, p);
            cx.Add(ro);
            cx.Add(g);
            return g;
        }
        public override (Transaction?, Physical) Commit(Writer wr, Transaction? tr)
        {
            if (directoryPath.StartsWith("http")) // do not commit
                return (tr, this);
            return base.Commit(wr, tr);
        }
    }
}