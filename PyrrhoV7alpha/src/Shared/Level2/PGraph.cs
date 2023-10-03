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
using Pyrrho.Level5;
namespace Pyrrho.Level2
{
    /// <summary>
    /// PNodeType adds PTable behaviour to PType
    /// </summary>
    internal class PNodeType : PType1
    {
        internal PNodeType(string nm,PType pt,NodeType dm,Context cx)
            :base (Type.PNodeType,nm,dm,dm.super,-1L,pt.ppos,cx)
        {  }
        internal PNodeType(Type t,string nm, PType pt, NodeType dm, Context cx)
            : base(t, nm, dm, dm.super, -1L, pt.ppos, cx)
        { }
        protected PNodeType(Type t, string nm, NodeType nt, Domain? un, long ns, long pp, Context cx)
            : base(t,nm,nt,un,ns, pp,cx) { }
        public PNodeType(string nm, NodeType nt, Domain? un, long ns, long pp, Context cx)
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
        internal PEdgeType(string nm, PType pt, EdgeType dm, Context cx)
            :base (Type.PEdgeType,nm,pt,dm,cx)
        {
            leavingType = dm.leavingType;
            arrivingType = dm.arrivingType;
   //         GraphUse(cx, defpos, leavingType, arrivingType);
        }
        public PEdgeType(string nm, EdgeType nt, Domain? un, long ns, long pp, Context cx) 
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
            dataType = et;
        }
        public override string ToString()
        {
            return base.ToString() + "(" + DBObject.Uid(leavingType) + "," + DBObject.Uid(arrivingType)+")";
        }
    }
}