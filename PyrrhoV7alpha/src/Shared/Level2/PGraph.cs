// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.
using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System.Text;
using System.Xml;

namespace Pyrrho.Level2
{
    /// <summary>
    /// PNodeType adds PTable behaviour to PType
    /// </summary>
    internal class PNodeType : PType
    {
        protected PNodeType(Type t, Ident nm, NodeType nt, Domain? un, long pp, Context cx)
            : base(t,nm,nt,un,pp,cx) { }
        public PNodeType(Ident nm, NodeType nt, Domain? un, long pp, Context cx)
            : base(Type.PNodeType, nm, nt, un, pp, cx) { }
        public PNodeType(Ident nm, Table tb, Domain? un, long pp, Context cx)
            : base(Type.PNodeType, nm, _Dm(cx,nm,tb), un, pp, cx) { }
        public PNodeType(Reader rdr) : base(Type.PNodeType, rdr) 
        {  }
        protected PNodeType(Type t, Reader rdr):base(t, rdr) { }
        protected PNodeType(PNodeType x,Writer wr) :base(x,wr) { }
        static NodeType _Dm(Context cx,Ident nm,Table tb)
        {
            var dm = cx._Dom(tb) ?? throw new PEException("PE95521");
            dm = new NodeType(cx.GetUid(), dm.mem + (Domain.Structure, tb.defpos) + (ObInfo.Name,nm.ident));
            return (NodeType)cx.Add(dm);
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
        public PEdgeType(Ident nm, EdgeType nt, Domain? un, long pp, Context cx)
    : base(Type.PEdgeType, nm, nt, un, pp, cx) { }
        public PEdgeType(Ident nm, Table tb, Domain? un, long pp, Context cx)
            : base(Type.PEdgeType, nm, _Dm(cx, nm, tb), un, pp, cx) 
        { }
        public PEdgeType(Reader rdr) : base(Type.PEdgeType, rdr) 
        { }
        protected PEdgeType(Type t, Reader rdr) : base(t, rdr) { }
        protected PEdgeType(PEdgeType x, Writer wr) : base(x, wr) 
        {
            dataType = (Domain)x.dataType.Relocate(wr.cx.Fix(x.dataType.defpos),wr.cx);
        }
        static EdgeType _Dm(Context cx, Ident nm, Table tb)
        {
            var dm = cx._Dom(tb) ?? throw new PEException("PE95521");
            dm = new EdgeType(cx.GetUid(), dm.mem + (Domain.Structure, tb.defpos) + (ObInfo.Name, nm.ident));
            return (EdgeType)cx.Add(dm);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PEdgeType(this, wr);
        }
        public override void Serialise(Writer wr)
        {
            var et = (EdgeType)dataType;
            wr.PutLong(et.leavingType);
            wr.PutLong(et.arrivingType);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            var lt = rdr.GetLong();
            var at = rdr.GetLong();
            var op = rdr.context.parse;
            rdr.context.parse = ExecuteStatus.Compile;
            base.Deserialise(rdr);
            dataType = dataType
                + (EdgeType.LeavingType,lt) + (EdgeType.ArrivingType,at);
            rdr.context.parse = op;
        }
        public override string ToString()
        {
            var et = (EdgeType)dataType;
            return base.ToString() + "(" + et.leavingType + "," + et.arrivingType+")";
        }
    }
}