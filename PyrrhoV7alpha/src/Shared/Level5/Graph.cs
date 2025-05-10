using Pyrrho.Level3;
using Pyrrho.Common;
using Pyrrho.Level4;
using System.Text;
using Pyrrho.Level2;

namespace Pyrrho.Level5
{
    // Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
    // (c) Malcolm Crowe, University of the West of Scotland 2004-2025
    //
    // This software is without support and no liability for damage consequential to use.
    // You can view and test this code
    // You may incorporate any part of this code in other software if its origin 
    // and authorship is suitably acknowledged.

    //The RDBMS view of graph data

    // A NodeType (or EdgeType) corresponds a single database object that defines
    // both a base Table in the database and a user-defined type for its rows. 

    // The UDType is managed by the database engine by default
    // but the usual ALTER operations are available for both Table and UDT.
    // Other columns are provided for any properties that are defined.
    // The UDT for a Node type also a set of possible LeavingTypes and ArrivingTypes
    // for edges, and the UDT for an Edge type specifies the LeavingType(s) and ArrivingType(s) for nodes.
    //
    // Each row of the NodeType (resp EdgeType) is a Node (resp Edge):
    // with an array-valued property of a Node explicitly identifying leavingNode edges by their uid,
    // while a generated array-valued property gives the set of arrivingNode edges, and
    // an Edge has two uid-valued properties giving the leavingNode and arrivingNode Node uid.
    // TNode and TEdge are uid-valued TypedValues whose dataType is NodeType (resp EdgeType).
    // Thus a row of a NodeType is a TNode, and a row of an EdgeType is a TEdge.

    // Nodes and edges are Records in the tables thus defined, and these can be updated and deleted
    // using SQL in the usual ways with the help of the Match mechanism described below.
    // ALTER TYPE, ALTER DOMAIN and ALTER TABLE statements can be applied to node and edge types.

    // The set of arrivingNode edges is a generated property, so an SQL INSERT cannot be used to insert an edge.
    // A statement requiring the construction of a graph fragment is automatically converted into a
    // sequence of database changes (insert and type operation) that are made directly to database objects.

    // Creating graph data in the RDBMS

    // A Neo4j-like syntax can be used to add one or more nodes and zero or more edges.
    // using the CREATE Node syntax defined in section 7.2 of the manual. The syntax is roughly
    // Create: CREATE Node {Edge Node} {',' Node { Edge Node }}.
    // Node: '(' id[':' label [':' label]] [doc] ')'.
    // Edge: '-['[id] ':' label [':' label] [doc] ']->' | '<-['[id] ':' label [':' label] [doc] ']-'.

    // In a batch session, such changes can be allowed (like CASCADE) or disallowed (like RESTRICT).

    // Label expressions in Match statements are converted to reverse polish using the mem fields op and operand.
    // For canonical label expressions, all the binary operators including : order their operands in order of history
    // so that with binary label operators the names of older types precedes younger types.
    internal class NodeType : UDType
    {
        internal const long
            IdCol = -472,  // long TableColumn (defining position used if not specified)
            IdColDomain = -493, // Domain (by default is POSITION)
            IdIx = -436,    // long Index (defining position used if not specified)
            LabelSet = -462; // CTree<string,bool> 
        internal Domain idColDomain => (Domain)(mem[IdColDomain] ?? Position);
        internal Domain label =>
    (Domain)(mem[GqlNode._Label] ?? GqlLabel.Empty);
        internal long idCol => (long)(mem[IdCol] ?? -1L);
        internal long idIx => (long)(mem[IdIx] ?? -1L);
        public override Domain domain => throw new NotImplementedException();
        internal CTree<string,bool> labels => 
            (CTree<string,bool>)(mem[LabelSet] ?? CTree<string,bool>.Empty);
        internal TRow? singleton => (TRow?)mem[TrivialRowSet.Singleton];
        internal NodeType(Qlx t) : base(t)
        { }
        public NodeType(long dp, BTree<long, object> m) : base(dp, m)
        { }
        internal NodeType(long lp,long dp, string nm, UDType dt, BTree<long,object> m, Context cx)
            : base(dp, _Mem(nm, lp, dt, m, cx))
        {
            AddNodeOrEdgeType(cx);
        }
        static BTree<long, object> _Mem(string nm, long lp, UDType dt, BTree<long,object>m, Context cx)
        {
            var r = m + dt.mem + (Kind, Qlx.NODETYPE);
            r += (ObInfo.Name, nm);
            var oi = new ObInfo(nm, Grant.AllPrivileges);
            oi += (ObInfo.Name, nm);
            r += (Definer, cx.role.defpos);
            var rt = CList<long>.Empty;
            var rs = CTree<long, Domain>.Empty;
            var ns = Names.Empty;
            var nn = Names.Empty;
            // At this stage we don't know anything about non-identity columns
            // add everything we find in direct supertypes to create the Domain
            if (m[Under] is CTree<Domain, bool> un)
            {
                un += dt.super;
                r += (Under, un);
                for (var tb = un.First(); tb != null; tb = tb.Next())
                    if (cx._Ob(tb.key().defpos) is Table pd)
                    {
                        var rp = pd.representation;
                        for (var c = pd.rowType.First(); c != null; c = c.Next())
                            if (c.value() is long p && rp[p] is Domain rd && rd.kind != Qlx.Null
                                && cx._Ob(p) is TableColumn tc && tc.infos[cx.role.defpos] is ObInfo ci
                                && ci.name is string cn && !ns.Contains(cn))
                            {
                                rt += p;
                                rs += (p, rd);
                                ns += (cn,(lp,p));
                                nn += (cn, (lp,p));
                            }
                    }
            }
            oi += (ObInfo._Names, ns);
            r += (_Domain, new Domain(cx, rs, rt, new BTree<long, ObInfo>(cx.role.defpos, oi)));
            return r;
        }
        internal TableRow? Get(Context cx, TypedValue? id)
        {
            if (id is null)
                return null;
            var px = FindPrimaryIndex(cx);
            return tableRows[px?.rows?.impl?[id]?.ToLong() ?? id?.ToLong() ?? -1L];
        }
        internal TableRow? GetS(Context cx, TypedValue? id)
        {
            if (id is null)
                return null;
            if (Get(cx, id) is TableRow rt)
                return new TableRow(rt.defpos,rt.ppos,defpos,rt.vals);
            for (var t = super.First(); t != null; t = t.Next())
                if (t.key() is NodeType nt && nt.Get(cx, id) is TableRow tr)
                    return tr;
            return null;
        }
        internal NodeType TopNodeType()
        {
            var t = this;
            for (var b = super.First(); b != null; b = b.Next())
                if (b.key() is NodeType a)
                {
                    var c = a.TopNodeType();
                    if (c.defpos < t.defpos && c.defpos > 0)
                        t = c;
                }
            return t;
        }
        internal override CTree<Domain, bool> _NodeTypes(Context cx)
        {
            return new CTree<Domain, bool>(this, true);
        }
/*        internal virtual void AddNodeOrEdgeType(Context cx)
        {
            var ro = cx.role;
            var ut = this;
            if (ro.nodeTypes[name] is long dp)
            {
                if (dp != defpos)
                    throw new DBException("42014", name);
                if (cx.db.objects[dp] is not NodeType nt)
                    throw new DBException("PE77101");
                ut = new NodeType(nt.defpos, nt.mem + mem);
            }
            else if (name != "")
                ro += (Role.NodeTypes, ro.nodeTypes + (name, defpos));
            cx.db += ut;
            cx.db += ro;
            cx.db += (Database.Role, cx.db.objects[cx.role.defpos] ?? throw new DBException("42105"));
        } */
        internal virtual void AddNodeOrEdgeType(Context cx)
        {
            var ro = cx.role;
            var nm = (label.kind == Qlx.NO) ? name : label.name;
            if (nm != "")
                ro += (Role.NodeTypes, ro.nodeTypes + (nm, defpos));
            else
            {
                var ps = CTree<long, bool>.Empty;
                var pn = CTree<string, bool>.Empty;
                for (var b = representation.First(); b != null; b = b.Next())
                    if (cx.NameFor(b.key()) is string n)
                    {
                        ps += (b.key(), true);
                        pn += (n, true);
                    }
                if (!ro.unlabelledNodeTypesInfo.Contains(pn))
                {
                    ro += (Role.UnlabelledNodeTypesInfo, ro.unlabelledNodeTypesInfo + (pn, defpos));
                    cx.db += (Database.UnlabelledNodeTypes, cx.db.unlabelledNodeTypes + (ps, defpos));
                }
            }
            cx.db += this;
            cx.db += ro;
            cx.db += (Database.Role, cx.db.objects[cx.role.defpos] ?? throw new DBException("42105"));
        } 
        internal virtual Domain? HaveNodeOrEdgeType(Context cx)
        {
            if (name != "")
                if (cx.role.nodeTypes[name] is long p && p < Transaction.Analysing)
                    return this;
            var pn = CTree<string, bool>.Empty;
            for (var b = representation.First(); b != null; b = b.Next())
                if (cx.NameFor(b.key()) is string n)
                    pn += (n, true);
            return (cx.role.unlabelledNodeTypesInfo[pn] is long q && q < Transaction.Analysing) ?
                this : null;
        }
        internal NodeType? SuperWith(Context cx,CTree<long,bool>p)
        {
            if (Props().CompareTo(p) == 0)
                return this;
            for (var b=super.First();b!=null;b=b.Next())
                if (cx.db.objects[b.key().defpos] is NodeType ub)
                {
                    if (ub.Props().CompareTo(p) == 0)
                        return ub;
                    if (ub.ContainsAll(p))
                        return ub.SuperWith(cx, p);
                }
            return null;
        }
        internal virtual TNode Node(Context cx, TableRow r)
        {
            return new TNode(cx, r);
        }
        internal CTree<long,bool> Props()
        {
            var pp = CTree<long, bool>.Empty;
            for (var b = rowType.First(); b != null; b = b.Next())
                if (b.value() is long bp)
                    pp += (bp, true);
            return pp;
        }
        bool ContainsAll(CTree<long,bool> p)
        {
            for (var b = p.First(); b != null; b = b.Next())
                if (!representation.Contains(b.key()))
                    return false;
            return true;
        }
        internal override TypedValue _Eval(Context cx)
        {
            if (singleton is not null && tableRows.First()?.value() is TableRow tr)
                return new TNode(cx, tr);
            return base._Eval(cx);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new NodeType(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new NodeType(dp, m + (Kind, Qlx.NODETYPE));
        }
        internal override UDType New(Ident pn, CTree<Domain, bool> un, long dp, Context cx)
        {
            return (UDType)(cx.Add(new PNodeType(pn.ident, (NodeType)NodeType.Relocate(dp),
                un, -1L, dp, cx)) ?? throw new DBException("42105").Add(Qlx.NODETYPE));
        }
        internal override Table _PathDomain(Context cx)
        {
            var rt = rowType;
            var rs = representation;
            var ii = infos;
            var gi = rs.Contains(idCol) || idCol < 0L;
            for (var tb = super.First(); tb != null; tb = tb.Next())
                if (cx._Ob(tb.key().defpos) is Table pd && pd.defpos>0)
                {
                    for (var b = infos.First(); b != null; b = b.Next())
                        if (b.value() is ObInfo ti)
                        {
                            if (pd.infos[cx.role.defpos] is ObInfo si)
                                ti += si;
                            else throw new DBException("42105").Add(Qlx.TYPE);
                            ii += (b.key(), ti);
                        }
                    if (pd is NodeType pn && (!gi) && (!rs.Contains(pn.idCol)))
                    {
                        gi = true;
                        rt += pn.idCol;
                        rs += (pn.idCol, cx._Ob(pn.idCol)?.domain??Position);
                    }
                    for (var b = pd?.rowType.First(); b != null; b = b.Next())
                        if (b.value() is long p && pd?.representation[p] is Domain cd && !rs.Contains(p))
                        {
                            rt += p;
                            rs += (p, cd);
                        }
                    for (var b = rowType.First(); b != null; b = b.Next())
                        if (b.value() is long p && representation[p] is Domain cd && !rs.Contains(p))
                        {
                            rt += p;
                            rs += (p, cd);
                        }
                }
            return new Table(cx, rs, rt, ii);
        }
        internal override UDType Inherit(UDType to)
        {
            if (idCol >= 0)
                to += (IdCol, idCol);
            return to;
        }
        // We have been updated. Ensure that all our subtypes are updated
        internal void Refresh(Context cx)
        {
            for (var b = subtypes?.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is NodeType bd)
                {
                    bd = bd + (Under, bd.super + (this, true)) + (IdIx, idIx) + (IdCol, idCol);
                    cx.db += bd;
                    bd.Refresh(cx);
                }
        }
        internal override DBObject Add(Context cx, PMetadata pm)
        {
            var ro = cx.role;
            if (pm.detail.Contains(Qlx.NODETYPE) && infos[ro.defpos] is ObInfo oi
                && oi.name is not null)
            {
                ro += (Role.NodeTypes, ro.nodeTypes + (oi.name, defpos));
                cx.db += ro;
            }
            return base.Add(cx, pm);
        }
  /*      internal override CList<long> Add(CList<long> a, int k, long v, long p)
        {
            if (p == idCol)
                k = 0;
            return base.Add(a, k, v, p);
        } */
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx, m);
            var ic = cx.Fix(idCol);
            if (ic != idCol)
                r += (IdCol, ic);
            var ix = cx.Fix(idIx);
            if (ix != idIx)
                r += (IdIx, ix);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var np = cx.Fix(defpos);
            var nm = _Fix(cx, mem);
            if (np == defpos && nm == mem)
                return this;
            var r = New(np, nm);
            if (defpos != -1L && cx.obs[defpos]?.dbg != r.dbg)
                cx.Add(r);
            return r;
        }
        public static NodeType operator +(NodeType et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (NodeType)et.New(m + x);
        }
        internal override Basis ShallowReplace(Context cx, long was, long now)
        {
            var r = (NodeType)base.ShallowReplace(cx, was, now);
            if (idCol == was && was>=0)
                r += (IdCol, now); 
            if (idIx == was && was>=0)
                r += (IdIx, now);
            if (r.dbg!=dbg)
                cx.Add(r);
            return r;
        }
        internal override (DBObject?, Ident?) _Lookup(long lp, Context cx, Ident ic, Ident? n, DBObject? r)
        {
            if (infos[cx.role.defpos] is ObInfo oi && cx._Ob(oi.names[ic.ident].Item2) is DBObject ob)
            {
                if (n is Ident ni)
                    switch (ni.ident)
                    {
                        case "ID":
                            break;
                        default:
                            return ob._Lookup(n.uid, cx, n, n.sub, null);
                    }
                return (new QlInstance(ic.lp, cx.GetUid(), cx, ic.ident, lp, ob), null);
            }
            return base._Lookup(lp, cx, ic, n, r);
        }
        internal override BTree<long, TableRow> For(Context cx, MatchStatement ms, GqlNode xn, BTree<long, TableRow>? ds)
        {
            var th = (NodeType)(cx.db.objects[defpos]??throw new PEException("PE50001"));
            ds ??= BTree<long, TableRow>.Empty;
            for (var b = cx.db.joinedNodes.First(); b != null; b = b.Next())
                if (b.value().Contains(this) && th.tableRows[b.key()] is TableRow tr)
                    ds += (cx.GetUid(),new TableRow(tr.defpos, tr.ppos, defpos, tr.vals));
            if (defpos < 0)
            {
                if (kind == Qlx.NODETYPE) // We are Domain.NODETYPE itself: do this for all nodetypes in the role
                {
                    for (var b = cx.db.role.nodeTypes.First(); b != null; b = b.Next())
                        if (b.value() is long p1 && cx.db.objects[p1] is NodeType nt1 && nt1.kind==kind)
                            ds = nt1.For(cx, ms, xn, ds);
                    for (var b = cx.db.unlabelledNodeTypes.First(); b != null; b = b.Next())
                        if (b.value() is long p2 && p2>=0 && cx.db.objects[p2] is NodeType nt2)
                            ds = nt2.For(cx, ms, xn, ds);
                }
                if (kind == Qlx.EDGETYPE) // We are Domain.EDGETYPE itself: do this for all edgetypes in the role
                {
                    for (var b = cx.db.role.edgeTypes.First(); b != null; b = b.Next())
                        if (b.value() is long p1 && cx.db.objects[p1] is Domain ed)
                        {
                            if (ed is EdgeType nt1)
                                ds = nt1.For(cx, ms, xn, ds);
                            else if (ed.kind == Qlx.UNION)
                                for (var c = ed.unionOf.First(); c != null; c = c.Next())
                                    if (cx.db.objects[c.key().defpos] is EdgeType ef)
                                        ds = ef.For(cx, ms, xn, ds);
                        }
                }
                return ds;
            }
            if (!ms.flags.HasFlag(MatchStatement.Flags.Schema))
            {
                var cl = xn.EvalProps(cx, th);
                if (th.FindPrimaryIndex(cx) is Level3.Index px
                    && px.MakeKey(cl) is CList<TypedValue> pk
                    && tableRows[px.rows?.Get(pk, 0) ?? -1L] is TableRow tr0)
                    return ds + (tr0.defpos, tr0);
                for (var c = indexes.First(); c != null; c = c.Next())
                    for (var d = c.value().First(); d != null; d = d.Next())
                        if (cx.db.objects[d.key()] is Level3.Index x
                            && x.MakeKey(cl) is CList<TypedValue> xk
                            && th.tableRows[x.rows?.Get(xk, 0) ?? -1L] is TableRow tr)
                            return ds + (tr.defpos, tr);
                // let DbNode check any given properties match
                var lm = ms.truncating.Contains(defpos) ? ms.truncating[defpos].Item1 : int.MaxValue;
                var la = ms.truncating.Contains(EdgeType.defpos) ? ms.truncating[EdgeType.defpos].Item1 : int.MaxValue;
                for (var b = th.tableRows.First(); b != null && lm-- > 0 && la-- > 0; b = b.Next())
                    if (b.value() is TableRow tr)
                        ds += (tr.defpos, tr);
            } else  // rowType flag
                ds += (defpos, th.Schema(cx));
            return ds;
        }
        /// <summary>
        /// Construct a fake TableRow for a nodetype rowType
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal TableRow Schema(Context cx)
        {
            var vals = CTree<long, TypedValue>.Empty;
            for (var b = rowType.First(); b != null; b = b.Next())
                if (cx.db.objects[b.value()] is TableColumn tc)
                    vals += (tc.defpos, new TTypeSpec(tc.domain));
            return new TableRow(defpos, -1L, defpos, vals);
        }
        public override Domain For()
        {
            return NodeType;
        }
        internal NodeType Specific(Context cx,TableRow tr)
        {
            for (var b = subtypes.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is NodeType t && t.tableRows.Contains(tr.defpos))
                    return t.Specific(cx, tr);
            return this;
        }
        internal override int Typecode()
        {
            return 3;
        }
        internal override RowSet RowSets(Ident id, Context cx, Domain q, long fm, long ap,
    Grant.Privilege pr = Grant.Privilege.Select, string? a = null,TableRowSet? ur = null)
        {
            cx.Add(this);
            var m = BTree<long, object>.Empty + (_From, fm) + (_Ident, id);
            if (a != null)
                m += (_Alias, a);
            var rowSet = (RowSet)cx._Add(new TableRowSet(id.uid, cx, defpos, ap, m));
            //#if MANDATORYACCESSCONTROL
            Audit(cx, rowSet);
            //#endif
            return rowSet;
        }
        internal override Table Base(Context cx)
        {
            throw new NotImplementedException(); // Node and Edge type do not have a unique base
        }
        internal override CTree<Domain, bool> OnInsert(Context cx,long ap,BTree<long,object>?m=null, 
            CTree<TypedValue,bool>? cs = null)
        {
            if (defpos < 0)
                return CTree<Domain, bool>.Empty;
            var nt = this;
            if (!cx.role.dbobjects.Contains(name))
            {
                var pn = new PNodeType(name, this, super, -1L, cx.db.nextPos, cx, true);
                nt = (NodeType)(cx.Add(pn)??throw new DBException("42105"));
                var dc = (CTree<string, QlValue>?)m?[GqlNode.DocValue];
                for (var b = dc?.First(); b != null; b = b.Next())
                    if (b.value() is QlValue sv && !nt.HierarchyCols(cx).Contains(b.key()))
                    {
                        var pc = new PColumn3(nt, b.key(), -1, sv.domain, TNull.Value, cx.db.nextPos, cx, true);
                        nt = (NodeType)(cx.Add(pc)??throw new DBException("42105"));
                    }
            }
            nt = (NodeType)(cx._Ob(nt.defpos) ?? nt);
            return new CTree<Domain,bool>(nt,true);
        }
        internal override Domain MakeUnder(Context cx,DBObject so)
        {
            return (so is NodeType sn) ? ((NodeType)New(defpos, mem + (Under, super + (sn, true)))) : this;
        }
        /// <summary>
        /// We have a new node type cs and have been given columns ls
        /// New columns specified are added or inserted.
        /// We will construct Physicals for new columns required
        /// </summary>
        /// <param name="x">The GqlNode or GqlEdge if any to apply this to</param>
        /// <param name="ll">The properties from an inline document, or default values</param>
        /// <param name="md">A list of TConnectors for EdgeType</param>
        /// <returns>The new node type: we promise a new PNodeType for this</returns>
        /// <exception cref="DBException"></exception>
        internal virtual NodeType Build(Context cx, GqlNode? x, long _ap, string nm, CTree<string,QlValue> dc,
            Qlx q=Qlx.NO, NodeType? nt=null,CList<TypedValue>? md=null)
        {
            var ut = this;
            var e = x as GqlEdge;
            if (defpos < 0)
                return this;
            var ls = x?.docValue??CTree<string, QlValue>.Empty;
            ls += dc;
            if (name is not string tn || tn=="")
                throw new DBException("42000", "Node name");
            var st = (name!="")?ut.super:CTree<Domain,bool>.Empty;
            // The new Type may not yet have a Physical record, so fix that
            if (cx.parse == ExecuteStatus.Obey)
                if (HaveNodeOrEdgeType(cx) is NodeType nd)
                    ut = nd;
                else
                {
                    PNodeType? pt = null;
                    if (nt is not null)
                        st += (nt, true);
                    if (ut is EdgeType te)
                    {
                        if (!cx.role.edgeTypes.Contains(tn))
                        {
                            var pe = new PEdgeType(tn, te, st, -1L, cx.db.nextPos, cx);// backwards compatibility
                            pt = pe;
                        }
                    }
                    else
                        pt = new PNodeType(tn, ut, st, -1L, cx.db.nextPos, cx);
                    if (pt != null)
                        ut = (NodeType)(cx.Add(pt) ?? throw new DBException("42105").Add(Qlx.INSERT_STATEMENT));
                }
            var rt = ut.rowType;
            var rs = ut.representation;
            var ui = ut?.infos[cx.role.defpos] ?? throw new DBException("42105").Add(Qlx.TYPE);
            var uds = ut.infos[cx.role.defpos]?.names??Names.Empty;
            var sn = BTree<string, long?>.Empty; // properties we are adding
            for (var b = ls.First(); b != null; b = b.Next())
            {
                var n = b.key();
                if (ut.HierarchyCols(cx).Contains(n))
                    continue;
                var d = cx._Dom(b.value().defpos) ?? Content;
                var pc = new PColumn3(ut, n, -1, d, "", TNull.Value, "", CTree<UpdateAssignment, bool>.Empty,
                false, GenerationRule.None, TNull.Value, cx.db.nextPos, cx);
                ut = (NodeType)(cx.Add(pc) ?? throw new DBException("42105").Add(Qlx.COLUMN));
                rt += pc.ppos;
                rs += (pc.ppos, d);
                var cn = new Ident(n, pc.ppos);
                uds += (cn.ident, (cn.lp, pc.ppos));
                cx.Add(ut.name, _ap, this);
                sn += (n, pc.ppos);
            }
            if (ut is not EdgeType et)
                return ut;
            cx.db += ut;
            for (var b = md?.First(); b != null; b = b.Next())
                if (b.value() is TConnector tc)
                    ut = et.BuildNodeTypeConnector(cx, tc).Item1;
            cx.db += ut;
            // update defs for inherited properties
            for (var b = ut.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.db.objects[p] is TableColumn uc
                    && uc.infos[uc.definer] is ObInfo ci
                        && ci.name is string sc && p != ut.idCol
                        && !rs.Contains(p))
                {
                    rt += p;
                    rs += (p, ut.representation[p] ?? Domain.Char);
                    uds += (sc, (_ap,p));
                }
            cx.Add(tn, _ap, this);
            ut += (RowType, rt);
            ut += (Representation, rs);
            for (var b = ut.super.First(); b != null; b = b.Next())
                if (b.key().infos[cx.role.defpos] is ObInfo si)
                    uds += si.names;
            var ri = BTree<long, int?>.Empty;
            for (var b = rt.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    ri += (p, b.key());
            for (var b = sn.First(); b != null; b = b.Next())
                if (b.value() is long qq && ri[qq] is int i)
                    uds += (b.key(), (_ap,qq));
            ut += (Infos, new BTree<long, ObInfo>(cx.role.defpos,
                new ObInfo(ut.name, Grant.AllPrivileges)
                + (ObInfo._Names, uds)));
            cx.Add(ut);
            var ro = cx.role + (Role.DBObjects, cx.role.dbobjects + (ut.name, ut.defpos));
            cx.db = cx.db + ut + ro;
            if (cx.db is Transaction ta && ta.physicals[ut.defpos] is PNodeType pn)
            {
                pn.dataType = ut;
                cx.db = ta + (Transaction.Physicals, ta.physicals + (pn.ppos, pn));
            }
            return ut;
        }
        internal virtual NodeType Check(Context cx, GqlNode e, long ap, bool allowExtras = true)
        {
            var r = this;
            if (cx._Ob(defpos) is not NodeType nt || nt.infos[definer] is not ObInfo ni)
                throw new DBException("PE42133", name);
            for (var b = e.docValue.First(); b != null; b = b.Next())
                if (!ni.names.Contains(b.key()) && allowExtras)
                {
                    var nc = new PColumn3(r, b.key(), -1, b.value().domain,TNull.Value, cx.db.nextPos, cx, true);
                    r = (NodeType?)cx.Add(nc)
                        ?? throw new DBException("42105");
                    ni += (ObInfo._Names, ni.names + (b.key(), (ap,nc.defpos)));
                    r += (Infos, r.infos+(cx.role.defpos,ni));
                    r += (ObInfo._Names, ni.names);
                    cx.Add(r);
                    cx.db += r;
                }
            return r;
        }
        internal NodeType FixNodeType(Context cx, Ident typename)
        {
            if (((Transaction)cx.db).physicals[typename.uid] is not PType pt)
                throw new PEException("PE50501");
            if (pt is not PNodeType)
            {
                pt = new PNodeType(typename.ident, pt, this, cx);
                cx.Add(pt);
            }
            FixColumns(cx, 1);
            pt.under = cx.FixTDb(super);
            pt.dataType = this;
            return (NodeType)(cx.Add(pt) ?? throw new DBException("42105").Add(Qlx.INSERT_STATEMENT));
        }
        internal void FixColumns(Context cx, int off)
        {
            for (var b = ((Transaction)cx.db).physicals.First(); b != null; b = b.Next())
                if (b.value() is PColumn pc && pc.table?.defpos == defpos)
                {
                    if (pc.seq >= 0)
                        pc.seq += off;
                    pc.table = this;
                }
        }
        internal class NodeInfo
        {
            internal readonly int type;
            internal readonly TypedValue id;
            internal float x, y;
            internal readonly long lv, ar;
            internal string[] props;
            internal NodeInfo(int t, TypedValue i, double u, double v, long l, long a, string[] ps)
            {
                type = t; id = i; x = (float)u; y = (float)v; lv = l; ar = a;
                props = ps;
            }
            public override string ToString()
            {
                var sb = new StringBuilder();
                sb.Append(type);
                sb.Append(',');
                if (id is TChar)
                { sb.Append('\''); sb.Append(id); sb.Append('\''); }
                else
                    sb.Append(id);
                sb.Append(',');
                sb.Append(x);
                sb.Append(',');
                sb.Append(y);
                sb.Append(',');
                sb.Append(lv);
                sb.Append(',');
                sb.Append(ar);
                sb.Append(",\"<p>");
                var cm = "";
                for (var i = 0; i < props.Length; i++)
                {
                    sb.Append(cm); cm = "<br/>"; sb.Append(props[i]);
                }
                sb.Append("</p>\"");
                return sb.ToString();
            }
        }
        /// <summary>
        /// My current idea is that given a  single node n, Pyrrho should be able to compute a list of nearby nodes 
        /// with x,y coordinates relative to n, in the following format
        /// (nodetype, id, x, y, lv, ar)
        /// Line 0 of this list should be the given node n, 
        /// lv and ar are the line numbers of the leaving and arriving nodes for edges.
        /// Nearby means that -500<x<500 and -500<y<500.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="start"></param>
        /// <returns></returns>
        internal static (BList<NodeInfo>, CTree<NodeType, int>) NodeTable(Context cx, TNode start)
        {
            var types = new CTree<NodeType, int>((NodeType)start.dataType, 0);
            var ntable = new BList<NodeInfo>(new NodeInfo(0, start.id, 0F, 0F, -1L, -1L, start.Summary(cx)));
            var nodes = new CTree<TNode, int>(start, 0);  // nodes only, no edges
            var edges = CTree<TEdge, int>.Empty;
            var todo = new BList<TNode>(start); // nodes only, no edges
            ran = new Random(0);
            while (todo.Count > 0)
            {
                var tn = todo[0]; todo -= 0;
            }
            return (ntable, types);
        }
        static void AddType(ref CTree<NodeType, int> ts, NodeType t)
        {
            if (!ts.Contains(t))
                ts += (t, (int)ts.Count);
        }
        static int? Have(CTree<TNode, int> no, TableRow tr)
        {
            for (var b = no.First(); b != null; b = b.Next())
                if (tr.Equals(b.key().tableRow))
                    return b.value();
            return null;
        }
        static bool Have(CTree<TEdge, int> ed, TableRow tr)
        {
            for (var b = ed.First(); b != null; b = b.Next())
                if (tr.Equals(b.key().tableRow))
                    return true;
            return false;
        }
        static Random ran = new (0);
        /// <summary>
        /// Return coordinates for an adjacent edge icon
        /// </summary>
        /// <param name="nt"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        /// <exception cref="PEException"></exception>
        static (double, double) GetSpace(BList<NodeInfo> nt, NodeInfo n)
        {
            var np = 6;
            var df = Math.PI / 3;
            // search for a space on circle of radious size d centered at n
            for (var d = 80.0; d < 500.0; d += 40.0, np *= 2, df /= 2)
            {
                var ang = ran.Next(1, 7) * df;
                for (var j = 0; j < np; j++, ang += df)
                {
                    var (x, y) = (n.x + d * 0.75 * Math.Cos(ang), n.y + d * 0.75 * Math.Sin(ang));
                    for (var b = nt.First(); b != null; b = b.Next())
                        if (b.value() is NodeInfo ni
                            && (dist(ni.x, ni.y, x, y) < 40.0 // the edge icon
                            || dist(ni.x, ni.y, 2 * x - n.x, 2 * y - n.y) < 40.0)) // the new node icon
                            goto skip;
                    return (x, y);
                skip:;
                }
            }
            throw new PEException("PE31800");
        }
        static NodeInfo? HasSpace(BList<NodeInfo> nt, NodeInfo e, double d = 40.0, NodeInfo? f = null)
        {
            for (var b = nt.First(); b != null; b = b.Next())
                if (b.value() is NodeInfo ni && ni != f
                    && dist(ni.x, ni.y, e.x, e.y) < d)
                    return ni;
            return null;
        }
        static NodeInfo TryAdjust(BList<NodeInfo> nt, NodeInfo e, NodeInfo a, NodeInfo b)
        {
            var d = dist(a.x, a.y, b.x, b.y);
            var dx = b.x - a.x;
            var dy = b.y - a.y;
            var cx = 20 * dx / d;
            var cy = 20 * dy / d;
            var e1 = new NodeInfo(e.type, e.id, e.x + cx, e.y + cy, e.lv, e.ar, e.props);
            if (HasSpace(nt, e1, 10.0, e) == null)
                return e1;
            e1 = new NodeInfo(e.type, e.id, e.x - cx, e.y - cy, e.lv, e.ar, e.props);
            if (HasSpace(nt, e1) == null)
                return e1;
            return e;
        }
        static double dist(double x, double y, double p, double q)
        {
            return Math.Sqrt((x - p) * (x - p) + (y - q) * (y - q));
        }
        /// <summary>
        /// Generate a row for the Role$Class table: includes a C# class definition,
        /// and computes navigation properties
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RoleClassValue(Context cx, RowSet from,
            ABookmark<long, object> _enu)
        {
            if (cx.role is not Role ro || infos[ro.defpos] is not ObInfo mi)
                throw new DBException("42105").Add(Qlx.ROLE);
            var nm = NameFor(cx);
            var ne = (this is EdgeType) ? "EdgeType" : "NodeType";
            var key = BuildKey(cx, out Domain keys);
            var fields = CTree<string, bool>.Empty;
            var sb = new StringBuilder("\r\nusing Pyrrho;\r\nusing Pyrrho.Common;\r\n");
            sb.Append("\r\n/// <summary>\r\n");
            sb.Append("/// " + ne + " " + nm + " from Database " + cx.db.name
                + ", Role " + ro.name + "\r\n");
            if (mi.description != "")
                sb.Append("/// " + mi.description + "\r\n");
            for (var b = indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx._Ob(c.key()) is Level3.Index x)
                        x.Note(cx, sb);
            for (var b = tableChecks.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is Check ck)
                    ck.Note(cx, sb);
            sb.Append("/// </summary>\r\n");
            sb.Append("[" + ne + "("); sb.Append(defpos); sb.Append(','); sb.Append(lastChange); sb.Append(")]\r\n");
            var su = new StringBuilder();
            var cm = "";
            for (var b = super.First(); b != null; b = b.Next())
                if (b.key().name != "")
                {
                    su.Append(cm); cm = ","; su.Append(b.key().name);
                }
            sb.Append("public class " + nm + " : " + (su.ToString() ?? "Versioned") + " {\r\n");
            for (var b = representation.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is TableColumn tc && tc.infos[cx.role.defpos] is ObInfo fi && fi.name != null)
                {
                    fields += (fi.name, true);
                    var dt = b.value();
                    var tn = ((dt is Table) ? dt.name : dt.SystemType.Name) + "?"; // all fields nullable
                    tc.Note(cx, sb);
                    if ((keys.rowType.Last()?.value() ?? -1L) == tc.defpos && dt.kind == Qlx.INTEGER)
                        sb.Append("  [AutoKey]\r\n");
                    sb.Append("  public " + tn + " " + tc.NameFor(cx) + ";\r\n");
                }
            for (var b = indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx._Ob(c.key()) is Level3.Index x &&
                            x.flags.HasFlag(PIndex.ConstraintType.ForeignKey) &&
                            cx.db.objects[x.refindexdefpos] is Level3.Index rx &&
                            cx._Ob(rx.tabledefpos) is Table tb && tb.infos[ro.defpos] is ObInfo rt &&
                            rt.name != null)
                    {
                        // many-one relationship
                        var sa = new StringBuilder();
                        var sc = new StringBuilder();
                        cm = "";
                        for (var d = b.key().First(); d != null; d = d.Next())
                            if (d.value() is long p)
                            {
                                sa.Append(cm); cm = ",";
                                sa.Append(cx.NameFor(p));
                                sc.Append(cx.NameFor(p));
                            }
                        if (tb is not UDType && !(rt.metadata.Contains(Qlx.ENTITY) || tb is NodeType))
                            continue;
                        var rn = ToCamel(rt.name);
                        for (var i = 0; fields.Contains(rn); i++)
                            rn = ToCamel(rt.name) + i;
                        var fn = cx.NameFor(rx.keys[0] ?? -1L)??"";
                        fields += (rn, true);
                        sb.Append("  public " + rt.name + "? " + sc.ToString()
                            + "is => conn?.FindOne<" + rt.name + ">((\"" + fn.ToString() + "\"," + sa.ToString() + "));\r\n");
                    }
            for (var b = rindexes.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is Table tb && tb.infos[ro.defpos] is ObInfo rt && rt.name != null)
                {
                    if (tb is UDType || rt.metadata.Contains(Qlx.ENTITY))
                        for (var c = b.value().First(); c != null; c = c.Next())
                        {
                            var sa = new StringBuilder();
                            cm = "(\"";
                            var rn = ToCamel(rt.name);
                            for (var i = 0; fields.Contains(rn); i++)
                                rn = ToCamel(rt.name) + i;
                            fields += (rn, true);
                            var x = tb.FindIndex(cx.db, c.key())?[0];
                            if (x != null)
                            // one-one relationship
                            {
                                cm = "";
                                for (var bb = c.value().First(); bb != null; bb = bb.Next())
                                    if (bb.value() is long p && c.value().representation[p] is DBObject ob &&
                                            ob.infos[ro.defpos] is ObInfo vi && vi.name is not null)
                                    {
                                        sa.Append(cm); cm = ",";
                                        sa.Append(vi.name);
                                    }
                                sb.Append("  public " + rt.name + "? " + rn
                                    + "s => conn?.FindOne<" + rt.name + ">((\"" + sa.ToString() + "\"," + sa.ToString() + "));\r\n");
                                continue;
                            }
                            // one-many relationship
                            var rb = c.value().First();
                            var sc = new StringBuilder();
                            for (var xb = c.key().First(); xb != null && rb != null; xb = xb.Next(), rb = rb.Next())
                                if (xb.value() is long xp && rb.value() is long rp)
                                {
                                    sa.Append(cm); cm = "),(\"";
                                    sa.Append(cx.NameFor(xp)); sa.Append("\",");
                                    sa.Append(cx.NameFor(rp));
                                    sc.Append(cx.NameFor(xp));
                                }
                            sa.Append(')');
                            sb.Append("  public " + rt.name + "[]? " + "of" + sc.ToString()
                                + "s => conn?.FindWith<" + rt.name + ">(" + sa.ToString() + ");\r\n");
                        }
                    else //  e.g. this is Brand
                    {
                        // tb is auxiliary table e.g. BrandSupplier
                        for (var d = tb.indexes.First(); d != null; d = d.Next())
                            for (var e = d.value().First(); e != null; e = e.Next())
                                if (cx.db.objects[e.key()] is Level3.Index px && px.reftabledefpos != defpos
                                            && cx.db.objects[px.reftabledefpos] is Table ts// e.g. Supplier
                                            && ts.infos[ro.definer] is ObInfo ti &&
                                            (ts is UDType || ti.metadata.Contains(Qlx.ENTITY)) &&
                                            ts.FindPrimaryIndex(cx) is Level3.Index tx)
                                {
                                    var sk = new StringBuilder(); // e.g. Supplier primary key
                                    cm = "\\\"";
                                    for (var c = tx.keys.First(); c != null; c = c.Next())
                                        if (c.value() is long p && representation[p] is DBObject ob
                                            && ob.infos[ro.defpos] is ObInfo ci &&
                                                        ci.name != null)
                                        {
                                            sk.Append(cm); cm = "\\\",\\\"";
                                            sk.Append(ci.name);
                                        }
                                    sk.Append("\\\"");
                                    var sa = new StringBuilder(); // e.g. BrandSupplier.Brand = Brand
                                    cm = "\\\"";
                                    var rb = px.keys.First();
                                    for (var xb = keys?.First(); xb != null && rb != null;
                                        xb = xb.Next(), rb = rb.Next())
                                        if (xb.value() is long xp && rb.value() is long rp)
                                        {
                                            sa.Append(cm); cm = "\\\" and \\\"";
                                            sa.Append(cx.NameFor(xp)); sa.Append("\\\"=\\\"");
                                            sa.Append(cx.NameFor(rp));
                                        }
                                    sa.Append("\\\"");
                                    var rn = ToCamel(rt.name);
                                    for (var i = 0; fields.Contains(rn); i++)
                                        rn = ToCamel(rt.name) + i;
                                    fields += (rn, true);
                                    sb.Append("  public " + ti.name + "[]? " + rn
                                        + "s => conn?.FindIn<" + ti.name + ">(\"select "
                                        + sk.ToString() + " from \\\"" + rt.name + "\\\" where "
                                        + sa.ToString() + "\");\r\n");
                                }
                    }
                }
            sb.Append("}\r\n");
            return new TRow(from, new TChar(name), new TChar(key),
                new TChar(sb.ToString()));
        }
        /// <summary>
        /// Generate a row for the Role$Python table: includes a Python class definition
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RolePythonValue(Context cx, RowSet from, ABookmark<long, object> _enu)
        {
            if (cx.role is not Role ro || infos[ro.defpos] is not ObInfo mi
                || kind == Qlx.Null || from.kind == Qlx.Null)
                throw new DBException("42105").Add(Qlx.ROLE);
            var versioned = true;
            var sb = new StringBuilder();
            var nm = NameFor(cx);
            sb.Append("# "); sb.Append(nm);
            sb.Append(" from Database " + cx.db.name + ", Role " + ro.name + "\r\n");
            var key = BuildKey(cx, out Domain keys);
            var fields = CTree<string, bool>.Empty;
            if (mi.description != "")
                sb.Append("# " + mi.description + "\r\n");
            for (var b = indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx._Ob(c.key()) is Level3.Index x)
                        x.Note(cx, sb, "# ");
            for (var b = tableChecks.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is Check ck)
                    ck.Note(cx, sb, "# ");
            var su = new StringBuilder();
            var cm = "";
            for (var b = super.First(); b != null; b = b.Next())
                if (b.key().name != "")
                {
                    su.Append(cm); cm = ","; su.Append(b.key().name);
                }
            if (this is UDType ty && ty.super is not null)
                sb.Append("public class " + nm + "(" + su.ToString() + "):\r\n");
            else
                sb.Append("class " + nm + (versioned ? "(Versioned)" : "") + ":\r\n");
            sb.Append(" def __init__(self):\r\n");
            if (versioned)
                sb.Append("  super().__init__('','')\r\n");
            for (var b = representation.First(); b is not null; b = b.Next())
                if (cx._Ob(b.key()) is TableColumn tc && tc.infos[cx.role.defpos] is ObInfo fi && fi.name != null)
                {
                    fields += (fi.name, true);
                    var dt = b.value();
                    tc.Note(cx, sb, "# " + ((dt is Table) ? dt.name : dt.SystemType.Name));
                    if ((keys.rowType.Last()?.value() ?? -1L) == tc.defpos && dt.kind == Qlx.INTEGER)
                        sb.Append("  AutoKey\r\n");
                    sb.Append("  self." + cx.NameFor(b.key()) + " = " + b.value().defaultValue);
                    sb.Append("\r\n");
                }
            sb.Append("  self._schemakey = "); sb.Append(from.lastChange); sb.Append("\r\n");
            if (keys != Null)
            {
                var comma = "";
                sb.Append("  self._key = [");
                for (var i = 0; i < keys.Length; i++)
                {
                    sb.Append(comma); comma = ",";
                    sb.Append('\''); sb.Append(keys[i]); sb.Append('\'');
                }
                sb.Append("]\r\n");
            }
            for (var b = indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx._Ob(c.key()) is Level3.Index x &&
                            x.flags.HasFlag(PIndex.ConstraintType.ForeignKey) &&
                            cx.db.objects[x.refindexdefpos] is Level3.Index rx &&
                            cx._Ob(rx.tabledefpos) is Table tb && tb.infos[ro.defpos] is ObInfo rt &&
                            rt.name != null)
                    {
                        // many-one relationship
                        var sa = new StringBuilder();
                        var sc = new StringBuilder();
                        cm = "";
                        for (var d = b.key().First(); d != null; d = d.Next())
                            if (d.value() is long p)
                            {
                                sa.Append(cm); cm = ",";
                                sa.Append(cx.NameFor(p));
                                sc.Append(cx.NameFor(p));
                            }
                        if (tb is not UDType && !(rt.metadata.Contains(Qlx.ENTITY) || tb is NodeType))
                            continue;
                        var rn = ToCamel(rt.name);
                        for (var i = 0; fields.Contains(rn); i++)
                            rn = ToCamel(rt.name) + i;
                        var fn = cx.NameFor(rx.keys[0] ?? -1L)??"";
                        fields += (rn, true);
                        sb.Append(" def " + sc.ToString() + "is(): \r\n");
                        sb.Append("  return conn.FindOne(" + rt.name + ",\"" + fn.ToString() + "\"=" + sa.ToString() + ")\r\n");
                    }
            for (var b = rindexes.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is Table tb && tb.infos[ro.defpos] is ObInfo rt && rt.name != null)
                {
                    if (tb is UDType || rt.metadata.Contains(Qlx.ENTITY))
                        for (var c = b.value().First(); c != null; c = c.Next())
                        {
                            var sa = new StringBuilder();
                            cm = "\"";
                            var rn = ToCamel(rt.name);
                            for (var i = 0; fields.Contains(rn); i++)
                                rn = ToCamel(rt.name) + i;
                            fields += (rn, true);
                            var x = tb.FindIndex(cx.db, c.key())?[0];
                            if (x != null)
                            // one-one relationship
                            {
                                cm = "";
                                for (var bb = c.value().First(); bb != null; bb = bb.Next())
                                    if (bb.value() is long p && c.value().representation[p] is DBObject ob &&
                                            ob.infos[ro.defpos] is ObInfo vi && vi.name is not null)
                                    {
                                        sa.Append(cm); cm = ",";
                                        sa.Append(vi.name);
                                    }
                                sb.Append(" def " + rn + "s():\r\n");
                                sb.Append("  return conn.FindOne(" + rt.name + ",\"" + sa.ToString() + "\"=" + sa.ToString() + ")\r\n");
                                continue;
                            }
                            // one-many relationship
                            var rb = c.value().First();
                            var sc = new StringBuilder();
                            for (var xb = c.key().First(); xb != null && rb != null; xb = xb.Next(), rb = rb.Next())
                                if (xb.value() is long xp && rb.value() is long rp)
                                {
                                    sa.Append(cm); cm = ",\"";
                                    sa.Append(cx.NameFor(xp)); sa.Append("\"=");
                                    sa.Append(cx.NameFor(rp));
                                    sc.Append(cx.NameFor(xp));
                                }
                            sb.Append(" def of" + sc.ToString() + "s():\r\n");
                            sb.Append("  return conn.FindWith(" + rt.name + "," + sa.ToString() + ")\r\n");
                        }
                }
            return new TRow(from, new TChar(name), new TChar(key),
                new TChar(sb.ToString()));
        }
        public override int CompareTo(object? obj)
        {
            if (obj is NodeType that)
            {
                var c = labels.CompareTo(that.labels);
                if (c != 0)
                    return c;
            }
            return base.CompareTo(obj);
        }
        public virtual string Describe(Context cx)
        {
            var sb = new StringBuilder();
            sb.Append("NODE "); sb.Append(name);
            var cm = " {";
            for (var b = First(); b != null; b = b.Next())
                if (b.value() is long p && representation[p] is Domain d)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(cx.NameFor(p));
                    sb.Append(' ');
                    sb.Append(d.ToString());
                }
            if (cm == ",")
                sb.Append('}');
            return sb.ToString();
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (idIx != -1L) sb.Append(" IdIx=" + Uid(idIx));
            if (idCol != -1L) sb.Append(" IdCol=" + Uid(idCol));
            return sb.ToString();
        }
    }
    /// <summary>
    /// For interpretation of labels: because of type renaming the valueType is role-dependent!
    /// left and right can be NodeType defpos (if name is "")
    /// Domain is a node/edge type
    /// op can be Qlx.EXCLAMATION,Qlx.VBAR,Qlx.AMPERSAND,Qlx.IMPLIES,Qlx.COLON
    /// OnInsert gives a tree of Node/Edgetypes 
    /// For is used in Match to give a list of TNodes that match a given GqlNode/Edge/Path
    /// OnExtra method gives a single node/edgetype if available for allowExtras 
    /// </summary>
    internal class GqlLabel : Domain
    {
        internal static GqlLabel Empty = new();
        internal long left => (long)(mem[QlValue.Left] ?? -1L);
        internal long right => (long)(mem[QlValue.Right] ?? -1L);
        public GqlLabel(long dp, Context? cx, long lf, long rg, BTree<long, object>? m = null)
            : base(dp, _Mem(cx, m, lf, rg)) { }
        internal GqlLabel(Ident id, Context cx,NodeType? lt = null, NodeType? at = null, BTree<long,object>? m = null)
            : this(id.lp, id.uid, id.ident, cx, lt?.defpos, at?.defpos, m) { }
        internal GqlLabel(long lp, long dp, string nm,  Context cx, long? lt = null, long? at = null, 
            BTree<long,object>? m=null)
            : this(dp, _Mem(cx, nm, lp, lt, at, m))
        {
            cx.dnames += (nm, (lp,dp));
        }
        GqlLabel() : this(-1L, null, -1L, -1L) { }
        internal GqlLabel(long dp, BTree<long, object> m) : base(dp, m)
        { }
        static BTree<long,object> _Mem(Context? cx,BTree<long,object>? m,long lf, long rg)
        {
            m ??= BTree<long, object>.Empty;
            if (cx?.obs[lf] is Domain lo && cx?.obs[rg] is Domain ro)
            {
                var rs = lo.representation + ro.representation;
                var ln = lo.infos[cx.role.defpos]?.names ?? lo.names;
                var rn = ro.infos[cx.role.defpos]?.names ?? ro.names;
                var rt = ro.rowType;
                for (var b = ln.First(); b != null; b = b.Next())
                    if (ln[b.key()].Item2 is long lp)
                    {
                        var ld = lo.representation[lp];
                        var rd = ro.representation[rn[b.key()].Item2];
                        if (ld is not null && rd is not null && ld.CompareTo(rd) != 0)
                            throw new DBException("22G12", ln);
                        if (!ro.representation.Contains(lp))
                            rt += lp;
                    }
                m += (RowType,rt);
                m += (Representation, rs);
                m += (ObInfo._Names, ln + rn);
            }
            return m + (QlValue.Left, lf) + (QlValue.Right, rg);
        }
        static BTree<long, object> _Mem(Context cx, string id, long ap, long? lt, long? at, BTree<long,object>?m)
        {
            m ??= BTree<long, object>.Empty;
            m += (ObInfo.Name, id);
            Domain dt = NodeType;
            var da = ((Qlx)(m[Kind] ?? Qlx.NO)) == Qlx.DOUBLEARROW;
            if (!m.Contains(Kind))
                m += (Kind, (lt is null || at is null) ? Qlx.NODETYPE : Qlx.EDGETYPE);
            else if (da && cx.db.objects[lt ?? -1L] is NodeType un)
            {
                m += (Under, new CTree<Domain,bool>(un,true));
                dt = un;
            }
            else if (((Qlx)(m[Kind] ?? Qlx.NO)) == Qlx.EDGETYPE)
                dt = EdgeType;
            var sd = (lt is null || at is null) ?
                (cx.db.objects[cx.role.nodeTypes[id] ?? -1L] as Domain)
                : (cx.db.objects[cx.role.edgeTypes[id]?? -1L] as Domain);
            if (sd is EdgeType se)
                sd = null;
            if (sd?.kind == Qlx.UNION)
                for (var c = sd.unionOf.First(); sd is null && c != null; c = c.Next())
                    if (cx.db.objects[c.key().defpos] is EdgeType sf)
                        sd = sf;
            sd ??= cx.db.objects[cx.role.dbobjects[id] ?? -1L] as Domain;
            if (sd is not null)
            {
                m = m + (_Domain, sd) + (Kind, sd.kind);
                cx.AddDefs(ap,sd);
            }
            else
                m += (_Domain, dt);
            return m;
        }
        public static GqlLabel operator +(GqlLabel sl, (long, object) x)
        {
            return (GqlLabel)sl.New(sl.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new GqlLabel(defpos, m);
        }
        internal override BTree<long, TableRow> For(Context cx, MatchStatement ms, GqlNode xn, BTree<long, TableRow>? ds)
        {
            xn += (Kind, kind);
            var lt = cx._Ob(left) as GqlLabel ?? Empty;
            var rt = cx._Ob(right) as GqlLabel ?? Empty;
            cx.Add(xn);
            switch (kind)
            {
                case Qlx.VBAR:
                    return lt.For(cx, ms, xn, ds) + rt.For(cx, ms, xn, ds);
                case Qlx.COLON: // because we don't know the priority semantics
                case Qlx.AMPERSAND:
                    {
                        var es = lt.For(cx, ms, xn, ds);
                        var fs = rt.For(cx, ms, xn, ds);
                        var eb = es.First();
                        ds = BTree<long, TableRow>.Empty;
                        for (var fb = fs.First(); eb != null && fb != null;)
                        {
                            var ep = eb.key();
                            var fp = fb.key();
                            if (ep == fp)
                            {
                                ds += (ep, eb.value());
                                eb = eb.Next();
                                fb = fb.Next();
                            }
                            else if (ep < fp)
                                eb = eb.Next();
                            else
                                fb = fb.Next();
                        }
                        return ds;
                    }
                case Qlx.EXCLAMATION:
                    {
                        var ns = lt.For(cx, ms, xn, ds);
                        var xs = xn.label.For(cx, ms, xn, null);
                        for (var b = xs.First(); xs != null && b != null; b = b.Next())
                            if (ns.Contains(b.key()))
                                xs -= b.key();
                        ds = xs;
                        break;
                    }
                case Qlx.NODETYPE:
                case Qlx.DOUBLEARROW:
                case Qlx.NO:
                    {
                        ds = xn.domain.For(cx, ms, xn, null);
                        break;
                    }
            }
            return ds ?? BTree<long, TableRow>.Empty;
        }
        internal override TypedValue Coerce(Context cx, TypedValue v)
        {
            return v;
        }
        internal override CTree<Domain, bool> OnInsert(Context cx, long ap, BTree<long, object>? m = null,
            CTree<TypedValue,bool>? cs = null)
        {
           var r = CTree<Domain, bool>.Empty;
            var lf = cx.obs[left] as Domain ?? Empty;
            var rg = cx.obs[right] as Domain ?? Empty;
            cs ??= CTree<TypedValue, bool>.Empty;
            m ??= BTree<long, object>.Empty;
            var lm = m;
            var rm = m;
            if (kind == Qlx.COLON)
            {
                if (left > right)
                {
                    lm += (Under, lf.super + (rg, true));
                    lm += (ObInfo._Names, lf.names + rg.names);
                    cx.Add(lf=(Domain)lf.New(lf.defpos,lm));
                }
                else
                {
                    rm += (Under, rg.super + (lf, true));
                    rm += (ObInfo._Names, lf.names + rg.names);
                    cx.Add(rg=(Domain)rg.New(rg.defpos, rm));
                }
            }
            if (cx.parse != ExecuteStatus.Obey)
                return new(Content, true);
            var dc = (CTree<string, QlValue>)(m[GqlNode.DocValue] ?? CTree<string, QlValue>.Empty);
            var k = kind;
            if (cx.parse != ExecuteStatus.Obey)
                return new(Content, true);
            switch(k)
            {
                case Qlx.AMPERSAND:
                case Qlx.COLON:
                    return lf.OnInsert(cx, ap, lm) + rg.OnInsert(cx, ap, rm);
                case Qlx.DOUBLEARROW:
                    rg.OnInsert(cx, ap, rm);
                    return lf.OnInsert(cx, ap, lm + (Under, new CTree<Domain, bool>(rg, true)));
                case Qlx.NODETYPE:
                    if (name is string n)
                        r += (cx.FindNodeType(n, dc)?.Build(cx, null, ap, "", dc) ?? NodeTypeFor(n, m, cx), true);
                    return r;
                case Qlx.EDGETYPE:
                    return r + (cx.FindEdgeType(name ?? "", dc)?.Build(cx, null, ap, "", dc)
                ?? EdgeTypeFor(name ?? "", m, cx, cs), true);
                case Qlx.NO:
                    if (cx.db.objects[cx.role.dbobjects[name] ?? -1L] is NodeType d)
                        r += (d, true);
                    return r;
                default:
                    return r;
            }
        }
        internal override bool Match(Context cx, CTree<Domain, bool> ts, Qlx tk = Qlx.Null)
        {
            var lf = cx.obs[left] as Domain ?? Empty;
            var rg = cx.obs[right] as Domain ?? Empty;
            return kind switch
            {
                Qlx.AMPERSAND or Qlx.COLON or Qlx.DOUBLEARROW => ts.Contains(domain),
                Qlx.NODETYPE or Qlx.EDGETYPE => tk == kind || ts.Contains(domain),
                Qlx.VBAR => lf.Match(cx, ts, tk) || rg.Match(cx, ts, tk),
                Qlx.EXCLAMATION => !lf.Match(cx, ts, tk),
                Qlx.NO => true,
                _ => false
            };
        }
        static NodeType NodeTypeFor(string nm, BTree<long,object> m, Context cx)
        {
            var un = (CTree<Domain, bool>)(m[Under] ?? CTree<Domain, bool>.Empty);
            var nu = CTree<Domain, bool>.Empty;
            for (var b = un.First(); b != null; b = b.Next())
                nu += ((b.key() is GqlLabel gl) ? (cx.db.objects[cx.role.dbobjects[gl.name ?? ""] ?? -1L]
                   as Domain)?? throw new DBException("42107", gl.name ?? "??") : b.key(), true);
            var dc = (CTree<string, QlValue>)(m[GqlNode.DocValue]??CTree<string,QlValue>.Empty);
            var nt = cx.FindNodeType(nm, dc);
            if (nt is null || nt.defpos<0)
            {
                if (cx.ParsingMatch)
                    return NodeType;
                var pt = new PNodeType(nm, NodeType, nu, -1L, cx.db.nextPos, cx);
                nt = (NodeType)(cx.Add(pt) ?? throw new DBException("42105"));
                for (var b = dc.First(); b != null; b = b.Next())
                    if (!nt.HierarchyCols(cx).Contains(b.key()))
                    {
                        var pc = new PColumn3(nt, b.key(), -1, b.value().domain, TNull.Value, cx.db.nextPos, cx, true);
                        nt = (NodeType)(cx.Add(pc)??throw new DBException("42105"));
                    }
                nt = nt.Build(cx, null, 0L, nm, dc);
            }
            return nt ?? throw new DBException("42105");
        }
        internal static EdgeType EdgeTypeFor(string nm, BTree<long, object> m, Context cx, CTree<TypedValue,bool>? cs=null)
        {
            if (cx.ParsingMatch)
                return EdgeType;
            var un = (CTree<Domain, bool>)(m[Under] ?? CTree<Domain, bool>.Empty);
            var nu = CTree<Domain, bool>.Empty;
            for (var b = un.First(); b != null; b = b.Next())
                nu += ((b.key() is GqlLabel gl) ? (cx.db.objects[cx.role.dbobjects[gl.name ?? ""] ?? -1L]
                    as Domain) ?? throw new DBException("42107", gl.name ?? "??") : b.key(), true);
            var dc = (CTree<string, QlValue>)(m[GqlNode.DocValue] ?? CTree<string, QlValue>.Empty);
            if (cx.db.objects[cx.role.edgeTypes[nm] ?? -1L] is not EdgeType et)
            {
                var pt = new PEdgeType(nm, EdgeType, nu, -1L, cx.db.nextPos, cx, true);
                et = (EdgeType)(cx.Add(pt) ?? throw new DBException("42105"));
            }
            for (var b = cs?.First(); b != null; b = b.Next())
                if (b.key() is TConnector tc)
                    (et,tc) = et.BuildNodeTypeConnector(cx, tc);
            var ro = cx.role;
            var e = (EdgeType?)cx.obs[et.defpos] ?? throw new DBException("42105");
            for (var b = dc.First(); b != null; b = b.Next())
                if (!e.HierarchyCols(cx).Contains(b.key()))
                {
                    var pc = new PColumn3(e, b.key(), -1, b.value().domain, TNull.Value, cx.db.nextPos, cx, true);
                    e = (EdgeType)(cx.Add(pc) ?? throw new DBException("42105"));
                }
            e = (EdgeType)e.Build(cx, null, 0L, nm, dc);
            return e;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (name is string nm && nm != kind.ToString())
            {
                sb.Append(' '); sb.Append(nm);
            }
            if (defpos>0)
            {
                sb.Append(' '); sb.Append(kind);
                if (left > 0)
                {
                    sb.Append(' '); sb.Append(Uid(left));
                }
                if (right > 0)
                {
                    sb.Append(' '); sb.Append(Uid(right));
                }
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// Structural information about edge connections is copied to subtypes.
    /// </summary>
    internal class EdgeType : NodeType
    {
        internal const long
            Connects = -467; // CTree<TypedValue,bool> TConnector
        public CTree<TypedValue,bool> connects =>
            (CTree<TypedValue,bool>)(mem[Connects] ?? CTree<TypedValue,bool>.Empty);
        internal EdgeType(long lp,long dp, string nm, UDType dt, BTree<long,object> m, Context cx)
            : base(lp, dp, nm, dt, m, cx)
        { }
        internal EdgeType(Qlx t) : base(t)
        { }
        public EdgeType(long dp, BTree<long, object> m) : base(dp, m)
        { }
        public static EdgeType operator+(EdgeType left, (long,object)x)
        {
            return (EdgeType)left.New(left.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new EdgeType(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new EdgeType(dp, m + (Kind, Qlx.EDGETYPE));
        }
        internal override UDType New(Ident pn, CTree<Domain,bool> un, long dp, Context cx)
        {
            var nd = (EdgeType)EdgeType.Relocate(dp);
            if (nd.defpos!=dp)
                nd.Fix(cx);
            return (UDType)(cx.Add(new PEdgeType(pn.ident, nd, un, -1L, dp, cx))
                ?? throw new DBException("42105").Add(Qlx.EDGETYPE));
        } 
        internal override NodeType Check(Context cx, GqlNode n, long ap, bool allowExtras = true)
        {
            var et = base.Check(cx, n, ap, allowExtras);
            // TBD
            return et;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var nc = cx.FixTVb(connects);
            if (nc != connects)
                m += (Connects, nc);
            return base._Fix(cx, m);
        }
        internal override void AddNodeOrEdgeType(Context cx)
        {
            var ro = cx.role;
            var nm = name ?? label.name;
            if (nm != "")
            {
                var ep = ro.edgeTypes[nm];
                var ed = cx._Ob(ep ?? -1L) as Domain;
                if (ep is null || ed?.kind == Qlx.NODETYPE) // second term here is for Metadata.EdgeType
                {
                    ro += (Role.EdgeTypes, ro.edgeTypes + (nm, defpos));
                    cx.db += ro;
                }
                else if (ed is not null && ed.defpos != defpos)
                {
                    if (ed.kind == Qlx.EDGETYPE)
                        ed = (Domain)cx.Add(UnionType(ed.defpos + 1L, ed, this));
                    else if (ed.kind == Qlx.UNION)
                        ed = (Domain)cx.Add(new Domain(ed.defpos, Qlx.UNION, ed.unionOf + (this, true)));
                    else throw new PEException("PE20901");
                    ro += (Role.EdgeTypes, ro.edgeTypes + (nm, ed.defpos));
                    ro += (Role.DBObjects, ro.dbobjects + (nm, ed.defpos));
                    cx.db += ed;
                    cx.db += ro;
                }
            }
            else
            {
                var ps = CTree<long, bool>.Empty;
                var pn = CTree<string, bool>.Empty;
                for (var b = representation.First(); b != null; b = b.Next())
                    if (cx.NameFor(b.key()) is string n)
                    {
                        ps += (b.key(), true);
                        pn += (n, true);
                    }
                if (cx.db.objects[ro.unlabelledEdgeTypesInfo[pn] ?? -1L] is not Domain ud)
                {
                    ro += (Role.UnlabelledNodeTypesInfo, ro.unlabelledNodeTypesInfo + (pn, defpos));
                    cx.db += (Database.UnlabelledNodeTypes, cx.db.unlabelledNodeTypes + (ps, defpos));
                    cx.db += ro;
                }
                else
                {
                    if (ud.kind == Qlx.EDGETYPE)
                        ud = (Domain)cx.Add(UnionType(cx.GetUid(), ud, this));
                    else if (ud.kind == Qlx.UNION)
                        ud = (Domain)cx.Add(new Domain(cx.GetUid(), Qlx.UNION, ud.unionOf + (this, true)));
                    else throw new PEException("PE20902");
                    ro += (Role.UnlabelledNodeTypesInfo, ro.unlabelledNodeTypesInfo + (pn, ud.defpos));
                    cx.db += (Database.UnlabelledNodeTypes, cx.db.unlabelledNodeTypes + (ps, ud.defpos));
                    cx.db += ro;
                }
            }
            cx.db += (Database.Role, cx.db.objects[cx.role.defpos] ?? throw new DBException("42105"));
            cx.db += this;
        }
        internal EdgeType Connect(Context cx, GqlNode? b, GqlNode? a, TypedValue cc,
            CTree<string, QlValue> ls, bool allowChange = false)
        {
            if (cc is not TConnector ec)
                return this;
            var found = false;
            for (var c = connects.First(); c != null; c = c.Next())
                if (c.key() is TConnector tc)
                {
                    long bt = (b?.domain ?? NodeType).defpos;
                    long at = (a?.domain ?? NodeType).defpos;
                    var cn = (ec.cn == "") ? ec.cn : tc.cn;
                    TypedValue qv = tc.q switch
                    {
                        Qlx.TO => ec.q switch
                        {
                            Qlx.ARROW => new TConnector(tc.q, at, tc.cn, tc.cd),
                            Qlx.RARROW => new TConnector(tc.q, bt, tc.cn, tc.cd),
                            _ => TNull.Value
                        },
                        Qlx.FROM => ec.q switch
                        {
                            Qlx.ARROWBASE => new TConnector(tc.q, bt, tc.cn, tc.cd),
                            Qlx.RARROWBASE => new TConnector(tc.q, at, tc.cn, tc.cd),
                            _ => TNull.Value
                        },
                        Qlx.WITH => ec.q switch
                        {
                            Qlx.ARROWBASETILDE => new TConnector(tc.q, at, tc.cn, tc.cd),
                            Qlx.RBRACKTILDE => new TConnector(tc.q, bt, tc.cn, tc.cd),
                            Qlx.TILDE => new TConnector(tc.q, at, tc.cn, tc.cd) ??
                                        new TConnector(tc.q, bt, tc.cn, tc.cd),
                            _ => TNull.Value
                        },
                        _ => TNull.Value
                    };
                    if (qv!=TNull.Value)
                    {
                        found = true;
                    }
                }
            var r = this;
            if (!found)
            {
                long nn = -1L;
                Qlx q = Qlx.Null;
                long bt = (b?.domain ?? NodeType).defpos;
                long at = (a?.domain ?? NodeType).defpos;
                if (b != null) switch (ec.q)
                    {
                        case Qlx.ARROWBASE: q = Qlx.FROM; nn = bt; break;
                        case Qlx.ARROW: q = Qlx.TO; nn = at; break;
                        case Qlx.RARROW: q = Qlx.TO; nn = bt; break;
                        case Qlx.RARROWBASE: q = Qlx.FROM; nn = at; break;
                        case Qlx.ARROWBASETILDE: q = Qlx.WITH; nn = bt; break;
                        case Qlx.RBRACKTILDE: q = Qlx.WITH; nn = at; break;
                    }
                if (nn>0)
                    (r, _) = BuildNodeTypeConnector(cx, 
                        new TConnector(q, nn, ec.cn, Position));
            }
            return r;
        }
        internal (EdgeType, CTree<string, QlValue>) Connect(Context cx, TNode? b, TNode a, GqlEdge ed, TypedValue cc,
CTree<string, QlValue> ls, bool allowChange = false)
        {
            if (cc is not TConnector ec)
                return (this, ls);
            var found = false;
            for (var c = connects.First(); c != null; c = c.Next())
                if (c.key() is TConnector tc)
                {
                    TypedValue qv = tc.q switch
                    {
                        Qlx.TO => ec.q switch
                        {
                            Qlx.ARROW or Qlx.ARROWR => Connect(cx, a, ec, tc, ed), // ]-> ->
                            Qlx.RARROW or Qlx.ARROWL => Connect(cx, b, ec, tc, ed), // <-[ <-
                            _ => TNull.Value
                        },
                        Qlx.FROM => ec.q switch
                        {
                            Qlx.ARROWBASE or Qlx.ARROWR => Connect(cx, b, ec, tc, ed), // -[ ->
                            Qlx.RARROWBASE or Qlx.ARROWL => Connect(cx, a, ec, tc, ed), // ]- <-
                            _ => TNull.Value
                        },
                        Qlx.WITH => ec.q switch
                        {
                            Qlx.ARROWLTILDE or Qlx.RARROWTILDE or Qlx.ARROWBASETILDE // <~ <~[ ~[
                                => Connect(cx, b, ec, tc, ed),
                            Qlx.RBRACKTILDE or Qlx.ARROWTILDE or Qlx.ARROWRTILDE // ]~ ]~> ~>
                                => Connect(cx, a, ec, tc, ed),
                            Qlx.TILDE => Connect(cx, a, ec, tc, ed) ?? Connect(cx, b, ec, tc, ed), // ~
                            _ => TNull.Value
                        },
                        _ => TNull.Value
                    };
                    if (qv != TNull.Value)
                    {
                        ls += (cx.NameFor(tc.cp) ?? tc.cn, new SqlLiteral(cx.GetUid(), qv));
                        found = true;
                    }
                }
            var r = this;
            if (!found)
            {
                TNode? nn = null;
                Qlx q = Qlx.Null;
                long bt = (b == null) ? -1L : b.dataType.defpos;
                long at = a.dataType.defpos;
                if (b != null) switch (ec.q)
                    {
                        case Qlx.ARROWBASE: q = Qlx.FROM; nn = b; break;
                        case Qlx.ARROW: q = Qlx.TO; nn = a; break;
                        case Qlx.RARROW: q = Qlx.FROM; nn = a; break;
                        case Qlx.RARROWBASE: q = Qlx.TO; nn = b; break;
                        case Qlx.ARROWBASETILDE: q = Qlx.WITH; nn = b; break;
                        case Qlx.RBRACKTILDE: q = Qlx.WITH; nn = a; break;
                    }
                if (nn != null)
                {
                    (r, var rc) = BuildNodeTypeConnector(cx,
                        new TConnector(q, nn.dataType.defpos, ec.cn, Position));
                    ls += (cx.NameFor(rc.cp) ?? rc.cn,
                        (SqlLiteral)cx.Add(new SqlLiteral(cx.GetUid(), new TPosition(nn.defpos))));
                }
            }
            return (r, ls);
        }

        static TypedValue Connect(Context cx, TNode? n, TConnector c, TConnector tc, GqlEdge ed)
        {
            if (n == null || (c.cn != "" && tc.cn.ToUpper() != c.cn.ToUpper()))
                return TNull.Value;
            if (cx.db.objects[n.tableRow.tabledefpos] is Domain nt && cx.db.objects[tc.ct] is Domain ct
                && !nt.EqualOrStrongSubtypeOf(ct))
                return TNull.Value;
            if (n != null)
            {
                if (tc.cd.kind == Qlx.POSITION)
                    return new TPosition(n.defpos);
                if (tc.cd.kind == Qlx.SET && tc.cd.elType is Domain de)
                    return (de.kind == Qlx.POSITION) ? new TPosition(n.defpos) : n;
                if (cx.db.objects[tc.ct] is Domain d && n.dataType.EqualOrStrongSubtypeOf(d))
                    return n;
            }
            throw new DBException("22G0V");
        }

        internal (EdgeType,TConnector) BuildNodeTypeConnector(Context cx, TConnector tc)
        {
            var d = cx._Ob(tc.ct) as Domain ?? throw new PEException("PE90151");
            if (d is NodeType nt)
                return BuildNodeTypeConnector(cx, tc, nt);
            else
                for (var c = d.unionOf.First(); c != null; c = c.Next())
                    if (c.key() is NodeType ct)
                        return BuildNodeTypeConnector(cx, tc, ct);
            throw new PEException("PE40721");
        }
        (EdgeType,TConnector) BuildNodeTypeConnector(Context cx, TConnector tc, NodeType nt)
        {
            var ut = cx.db.objects[cx.role.edgeTypes[name] ?? -1L] as EdgeType ?? this;
            var cs = CTree<(Qlx, Domain), CTree<TypedValue,bool>>.Empty;
            var ns = CTree<string, TConnector>.Empty;
            for (var b = ut.connects.First(); b != null; b = b.Next())
                if (b.key() is TConnector c && cx._Ob(c.ct) is NodeType n)
                {
                    var cl = cs[(c.q, n)] ?? CTree<TypedValue,bool>.Empty; 
                    cs += ((c.q, n), cl+(c,true));
                    var nn = (c.cn=="") ? c.q.ToString() : c.cn;
                    if (ns.Contains(nn))
                        nn += b.key();
                    ns += (nn, c);
                }
            var dn = cx._Ob(tc.ct) as NodeType ?? throw new PEException("PE90152");
            var tt = cs[(tc.q, dn)];
            var cn = (tc.cn == "")? tc.q.ToString() : tc.cn;
            if (ut.names.Contains(cn))
                cn += cs.Count;
            var cc = (tt?.Count == 1) ? tt?.First()?.key() ?? ns[cn] : null;
            if (cc is TConnector x && x.cp>0L)
                return (ut,x);
            var tn = new TConnector(tc.q, tc.ct, tc.cn, tc.cd, cx.db.nextPos, tc.cs, tc.cm);
            var pc = new PColumn3(ut, cn, Length, Position,tn, cx.db.nextPos, cx, 
                !cx.parse.HasFlag(ExecuteStatus.Compile));
            ut = (EdgeType)(cx.Add(pc) ?? throw new DBException("42105").Add(Qlx.COLUMN));
            var nc = (TableColumn)(cx._Ob(pc.ppos) ?? throw new DBException("42105").Add(Qlx.COLUMN));
            if (tn.cm is TMetadata md)
                cx.Add(new PMetadata(cn, -1, nc, tn.cs, md, cx.db.nextPos));
            var di = new Domain(-1L, cx, Qlx.ROW, new BList<DBObject>(nc), 1);
            ut.AddNodeOrEdgeType(cx);
            return ((EdgeType)cx.Add(ut),tn);
        }
        internal override Table? Delete(Context cx, Delete del)
        {
            if (tableRows[del.delpos] is TableRow tr)
                for (var b = connects.First(); b != null; b = b.Next())
                    if (b.key() is TConnector co && cx._Ob(co.ct) is NodeType nt
                            && tr.vals[co.cp] is TInt li && li.ToLong() is long lp
                            && nt.sindexes[lp] is CTree<long, CTree<long, bool>> Ll
                            && Ll[lp] is CTree<long, bool> Lll)
                        cx.db += nt + (SysRefIndexes, nt.sindexes + (lp, Ll + (co.cp, Lll - del.delpos)));
            return base.Delete(cx, del);
        }
        internal override void Update(Context cx, TableRow prev, CTree<long, TypedValue> fields)
        {
            for (var b = connects.First(); b != null; b = b.Next())
                if (b.key() is TConnector co && cx._Ob(co.ct) is NodeType nt
                        && prev.vals[co.cp] is TInt li
                        && fields[co.cp] is TInt lu && li.CompareTo(lu) != 0
                        && li.ToLong() is long lp
                        && nt.sindexes[lp] is CTree<long, CTree<long, bool>> Ll
                        && Ll[co.cp] is CTree<long, bool> Lll)
                    cx.db += nt + (SysRefIndexes, nt.sindexes + (lp, Ll + (co.cp, Lll - prev.defpos)));
        }
        internal override Domain? HaveNodeOrEdgeType(Context cx)
        {
            if (name!="")
            {
                // this should check if we have the same TConnectors, possibly in a different order
                // and that the target nodetypes match
                if (cx.role.edgeTypes[name] is long ep && cx.db.objects[ep] is Domain d)
                {
                    if (d is EdgeType ed)
                        return EdgeSubTypeOf(cx,ed);
                    if (d.kind == Qlx.UNION)
                    {
                        for (var c = d.unionOf.First(); c != null; c = c.Next())
                            if (cx.db.objects[c.key().defpos] is EdgeType ee
                                && EdgeSubTypeOf(cx, ee) is null)
                                return null;
                        return d;
                    }
                }
                return null;
                // but we need to organise something if they don't
            }
            var pn = CTree<string, bool>.Empty;
            for (var b = representation.First(); b != null; b = b.Next())
                if (cx.NameFor(b.key()) is string n)
                pn += (n, true);
            return cx.role.unlabelledEdgeTypesInfo.Contains(pn)?this:null;
        }
        EdgeType? EdgeSubTypeOf(Context cx,EdgeType d)
        {
            var cs = CTree<TypedValue, bool>.Empty;
            if (d.connects.Count != connects.Count)
                return null;
            for (var b = connects.First(); b != null; b = b.Next())
            {
                if (b.key() is TConnector c)
                {
                    for (var db = d.connects.First(); db != null; db = db.Next())
                        if (db.key() is TConnector dc && cx._Ob(c.ct) is Domain td
                            && dc.q == c.q && !td.OkForConnector(cx,dc)
                            && (c.cn != "" && c.cn != dc.cn)) goto skip;
                    return null;
                skip:;
                }
            }
            return d;
        }
        internal EdgeType FixEdgeType(Context cx, Ident typename, CTree<TypedValue,bool>?tm = null)
        {
            if (((Transaction)cx.db).physicals[typename.uid] is not PType pt)
                throw new PEException("PE50501");
            if (pt is not PEdgeType)
            {
                pt = new PEdgeType(typename.ident, pt, this, cx);
                cx.Add(pt);
            }
            FixColumns(cx, 1);
            pt.under = cx.FixTDb(super);
            var r = this;
            for (var b = tm?.First(); b != null; b = b.Next())
                if (b.key() is TConnector tc)
                    r = r.BuildNodeTypeConnector(cx, tc).Item1;
            return r;
        }
        internal override DBObject Add(Context cx, PMetadata pm)
        {
            var ro = cx.role;
            if (pm.detail.Contains(Qlx.EDGETYPE) && infos[ro.defpos] is ObInfo oi
                && oi.name is not null)
            {
                if (oi.name!="")
                {
                    if (cx.role.edgeTypes[oi.name] is long ep && cx.db.objects[ep] is Domain ed)
                    {
                        if (ed.kind==Qlx.UNION)
                        {
                            var ev = cx.Add(new Domain(ep, Qlx.UNION, ed.unionOf + (this, true)));
                            ro += (Role.EdgeTypes, ro.edgeTypes + (oi.name, ev.defpos));
                        }    
                    } 
                    else
                        ro += (Role.EdgeTypes, ro.edgeTypes + (oi.name, defpos));
                    cx.db += ro; 
                }
           }
            return base.Add(cx, pm);
        }
        internal override TNode Node(Context cx, TableRow r)
        {
            return new TEdge(cx, r);
        }
        internal override Table _PathDomain(Context cx)
        {
            var rt = rowType;
            var rs = representation;
            var ii = infos;
            var gi = rs.Contains(idCol); 
            for (var tb = super.First(); tb != null; tb = tb.Next())
                if (cx._Ob(tb.key().defpos) is Table pd && pd.defpos>0)
                {
                    for (var b = infos.First(); b != null; b = b.Next())
                        if (b.value() is ObInfo ti)
                        {
                            if (pd.infos[cx.role.defpos] is ObInfo si)
                                ti += si;
                            else throw new DBException("42105").Add(Qlx.UNDER);
                            ii += (b.key(), ti);
                        }
                    if (pd is NodeType pn && (!gi) && (!rs.Contains(pn.idCol)) && pn.idCol>=0)
                    {
                        gi = true;
                        rt += pn.idCol;
                        rs += (pn.idCol, pn.idColDomain);
                    }
                    for (var b = pd?.rowType.First(); b != null; b = b.Next())
                        if (b.value() is long p && pd?.representation[p] is Domain cd && !rs.Contains(p))
                        {
                            rt += p;
                            rs += (p, cd);
                        }
                    for (var b = rowType.First(); b != null; b = b.Next())
                        if (b.value() is long p && representation[p] is Domain cd && !rs.Contains(p))
                        {
                            rt += p;
                            rs += (p, cd);
                        }
                }
            return new Table(cx, rs, rt, ii);
        }
        internal override Basis Fix(Context cx)
        {
            var r = New(cx.Fix(defpos), _Fix(cx, mem));
            var ro = cx.role;
      //      cx.db += ro + (Role.EdgeTypes, ro.edgeTypes + (name, cx.Fix(defpos)));
            if (cx.db.objects.Contains(defpos))
                cx.db += this;
            if (defpos != -1L)
                cx.Add(r);
            return r;
        }
        public override Domain For()
        {
            return EdgeType;
        }
        public override int Compare(TypedValue a, TypedValue b)
        {
            if (a == b) return 0;
            var c = a.dataType.defpos.CompareTo(b.dataType.defpos);
            if (c != 0) return c;
            if (a.dataType.kind==Qlx.ARRAY)
                return ((TArray)a).CompareTo((TArray)b);
            // if we get to here they both have this as dataType
            return ((TEdge)a).CompareTo((TEdge)b);
        }
        internal override CTree<Domain, bool> OnInsert(Context cx, long ap, BTree<long, object>? m = null,
            CTree<TypedValue,bool>? cs = null)
        {
            var et = cx.db.objects[cx.role.edgeTypes[name]??-1L] as EdgeType
                    ?? (EdgeType)(cx.Add(new PEdgeType(name, this, super, -1L, cx.db.nextPos,cx, true))
                    ?? throw new DBException("42105"));
            cx.obs += (defpos, et);
            var dc = (CTree<string, QlValue>?)m?[GqlNode.DocValue];
            for (var b = dc?.First(); b != null; b = b.Next())
                if (b.value() is QlValue sv && !et.HierarchyCols(cx).Contains(b.key()))
                {
                    var pc = new PColumn3(et, b.key(), -1, sv.domain, TNull.Value, cx.db.nextPos, cx, true);
                    et = (EdgeType)(cx.Add(pc) ?? throw new DBException("42105"));
                }
            var ec = CTree<(Qlx, NodeType, string), bool>.Empty;
            for (var b = et.connects.First(); b != null; b = b.Next())
            {
                if (b.key() is TConnector tc)
                {
                    var d = cx._Ob(tc.ct) as Domain ?? throw new PEException("PE90153");
                    if (d is NodeType nt)
                        ec += ((tc.q, nt, tc.cn), true);
                    else for (var c = d.unionOf.First(); c != null; c = c.Next())
                            if (c.key() is NodeType tn)
                                ec += ((tc.q, tn, tc.cn), true);
                }
            }
            for (var b = cs?.First(); b != null; b = b.Next())
                if (b.key() is TConnector tc)
                    et = et.BuildNodeTypeConnector(cx, tc).Item1;
            return new CTree<Domain, bool>(et, true);
        }
        internal override Domain MakeUnder(Context cx, DBObject so)
        {
            return (so is EdgeType sn) ? ((EdgeType)New(defpos,mem + (Under, super+(sn,true)))) : this;
        }
        public override string Describe(Context cx)
        {
            var sb = new StringBuilder();
            sb.Append("DIRECTED ");
            sb.Append("EDGE "); sb.Append(name);
            var cm = " {";
            for (var b = First(); b != null; b = b.Next())
                if (b.value() is long p && representation[p] is Domain d
                    && d.kind!=Qlx.POSITION)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(cx.NameFor(p));
                    sb.Append(' ');
                    sb.Append(d.ToString());
                }
            if (cm == ",")
                sb.Append('}');
            return sb.ToString();
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (connects.Count != 0L)
            {
                sb.Append(' '); sb.Append(connects);
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// This type is created as a side effect of Record4.Install: 
    /// Records in this table are posted into and indexed by the separate nodeTypes.
    /// </summary>
    internal class JoinedNodeType : NodeType
    {
        internal JoinedNodeType(long lp, long dp, string nm, UDType dt, BTree<long,object> m, Context cx) 
            : base(lp, dp, nm, dt, m, cx)
        {
            var oi = new ObInfo(nm, Grant.AllPrivileges);
            var ns = Names.Empty;
            for (var b = nodeTypes.First(); b != null; b = b.Next())
                if (b.key().infos[cx.role.defpos] is ObInfo fi)
                    ns += fi.names;
            cx.Add(this + (Infos,new BTree<long,ObInfo>(cx.role.defpos,oi+(ObInfo._Names,ns))));
        }
        public JoinedNodeType(long dp, BTree<long, object> m) : base(dp, m)
        { }
        public static JoinedNodeType operator+(JoinedNodeType nt,(long,object)x)
        {
            return (JoinedNodeType)nt.New(nt.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new JoinedNodeType(defpos,m);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            return base._Replace(cx, so, sv);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return base.New(dp, m);
        }
        internal override NodeType Build(Context cx, GqlNode? x, long ap, string nm, 
            CTree<string,QlValue>?ls=null,Qlx q=Qlx.NO, NodeType? nn=null, CList<TypedValue>? md=null)
        {
            var ids = CTree<long, TypedValue>.Empty;
            var it = Null;
            ls ??= CTree<string,QlValue>.Empty;
            // examine any provided id values
            for (var b = nodeTypes.First(); b != null; b = b.Next())
                if (b.key() is NodeType nt && cx._Ob(nt.idCol) is TableColumn tc
                        && tc.infos[cx.role.defpos] is ObInfo ci && ci.name is string cn
                        && ls?[cn] is QlValue sv)
                {
                    var tv = sv.Eval(cx);
                    for (var c = ids.First(); c != null; c = c.Next())
                        if (c.value() is TypedValue cv && cv.CompareTo(tv) != 0)
                            throw new DBException("42000", "Conflicting id values");
                    if (it.defpos == Null.defpos)
                        it = tv.dataType;
                    else if (it.CompareTo(tv.dataType) != 0)
                        throw new DBException("42000", "Conflicting id types");
                }
            if (cx.parse != ExecuteStatus.Obey)
                return this;
            var fl = CTree<long, TypedValue>.Empty;
            var dp = cx.db.nextPos;
            var tbs = CTree<long, bool>.Empty;
            for (var b = nodeTypes.First(); b != null; b = b.Next())
                if (b.key() is NodeType nt && x is not null)
                {
                    var np = cx.GetUid();
                    var m = new BTree<long, object>(GqlNode._Label,
                        cx.Add(new GqlLabel(ap,cx.GetUid(),nt.name,cx)));
                    var nd = new GqlNode(new Ident(Uid(np), np), CList<Ident>.Empty, cx,
                        -1L, x.docValue, x.state, nt, m);
                    nd.Create(cx, nt, ap, x.docValue,false);
                    // locate the Record that has just been constructed in et
                    tbs += (nt.defpos, true);
                    var tr = (Transaction)cx.db; // must be inside this loop
                    for (var c = tr.physicals.Last(); c != null; c = c.Previous())
                        if (c.value() is Record rc 
                            && (rc.tabledefpos==nt.defpos 
                            || (rc as Record4)?.extraTables.Contains(nt.defpos)==true))
                        { 
                            fl += rc.fields;
                            break;
                        }
                    // and make sure the next Record uses the same position!
                    cx.db += (Database.NextPos, dp);
                }
            var nr = new Record4(tbs, fl, -1L, Level.D, cx.db.nextPos, cx);
            if (x is not null)
                cx.values += (x.defpos, new TNode(cx, new TableRow(nr,cx)));
            cx.Add(nr);
            return this;
        }
        internal override Table _PathDomain(Context cx)
        {
            return base._PathDomain(cx);
        }
        internal override CTree<Domain, bool> _NodeTypes(Context cx)
        {
            return nodeTypes;
        }
        internal override TNode Node(Context cx, TableRow r)
        {
            if (cx.db.joinedNodes.Contains(r.defpos))
                return new TJoinedNode(cx, r);
            return base.Node(cx, r);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("JoinedNodeType");
            sb.Append(base.ToString());
            return sb.ToString();
        } 
    }
    /// <summary>
    /// See GQL 4.13: it is a set of node types and edge types that are defined as constraints on a Graph
    /// </summary>
    internal class GraphType : Domain
    {
        internal GraphType(PGraphType pg, Context cx, long ap)
            : this(pg.ppos, _Mem(pg,cx, ap))
        { }
        public GraphType(long dp, BTree<long, object> m) : base(dp, m)
        { }
        public GraphType(long pp, long dp, BTree<long, object>? m = null) : base(pp, dp, m)
        {  }
        static BTree<long,object> _Mem(PGraphType pg,Context cx,long ap)
        {
            var r = BTree<long, object>.Empty;
            r += (Graph.Iri, pg.iri);
            r += (Graph.GraphTypes, pg.types);
            var ix = pg.iri.LastIndexOf('/');
            var nm = pg.iri[ix..];
            var oi = new ObInfo(nm, Grant.AllPrivileges);
            var ns = Names.Empty;
            for (var b = pg.types.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is UDType ut)
                    ns += (ut.name, (ap,b.key()));
            oi += (ObInfo._Names, ns);
            r += (Infos, new BTree<long, ObInfo>(cx.role.defpos, oi));
            return r;
        }
        public static GraphType operator +(GraphType et, (long, object) x)
        {
            return (GraphType)et.New(et.defpos, et.mem + x);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new GraphType(dp,m);
        }
    }
    // The Graph view of graph data

    // The database is considered to contain a (possibly empty) set of TGraphs.
    // Every Node in the database belongs to exactly one graph in this set. 

    /// <summary>
    /// A Graph is a DBObject containing a tree of TNodes.
    /// </summary>
    internal class Graph : DBObject,IComparable
    {
        internal const long
            GraphTypes = -122, // CTree<long,bool> GraphType
            Iri = -147, // string
            Nodes = -499; // CTree<long,TNode> // and edges
        internal CTree<long,TNode> nodes =>
            (CTree<long, TNode>) (mem[Nodes]??CTree<long,TNode>.Empty);
        internal CTree<long,bool> graphTypes => 
            (CTree<long,bool>)(mem[GraphTypes] ?? CTree<long, bool>.Empty);
        internal string iri => (string)(mem[Iri]??"");
        internal Graph(PGraph pg,Context cx,long ap)
            : base(pg.ppos,_Mem(cx,pg,ap))
        { }
        public Graph(long dp, BTree<long, object> m) : base(dp, m)
        { }
        static BTree<long,object> _Mem(Context cx,PGraph ps,long ap)
        {
            var r = BTree<long, object>.Empty
                + (Nodes, ps.records) + (Iri, ps.iri) + (GraphTypes, ps.types ?? CTree<long, bool>.Empty);
            var nm = ps.iri;
            var ix = ps.iri.LastIndexOf('/');
            if (ix >= 0)
            {
                nm = ps.iri[(ix + 1)..];
                ps.iri = ps.iri[0..ix];
            }
            var oi = new ObInfo(nm, Grant.AllPrivileges);
            var ns = Names.Empty;
            for (var b = ps.types?.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is UDType ut)
                    ns += (ut.name, (ap,b.key()));
            oi += (ObInfo._Names, ns);
            r += (Infos, new BTree<long, ObInfo>(cx.role.defpos, oi));
            return r;
        }
        public static Graph operator +(Graph et, (long, object) x)
        {
            return (Graph)et.New(et.defpos, et.mem + x);
        }
        public static Graph operator+(Graph g,TNode r)
        {
            return new Graph(g.defpos,g.mem + (Nodes,g.nodes+(r.tableRow.defpos,r)));
        }
        public int CompareTo(object? obj)
        {
            if (obj is not Graph tg)
                return 1;
            var c = iri.CompareTo(tg.iri);
            if (c != 0) return c;
            c = nodes.CompareTo(tg.nodes);
            if (c!=0) return c;
            return graphTypes.CompareTo(tg.graphTypes);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("TGraph (");
            var cm = "[";
            for (var b=nodes.First();b is not null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.value());
            }
            if (cm==",")
                sb.Append(']');
            return sb.ToString();
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new Graph(dp,m);
        }
    }
    internal class Schema : DBObject
    {
        internal const long
            _Graphs = -362,    // CTree<long,bool> Graph
            GraphTypes = -186; // CTree<long,bool> Graph
        internal CTree<long,bool> graphs =>
            (CTree<long, bool>)(mem[_Graphs] ?? CTree<long, bool>.Empty);
        internal CTree<long, bool> graphTypes =>
            (CTree<long, bool>)(mem[GraphTypes] ?? CTree<long, bool>.Empty);
        internal string directoryPath => 
            (string)(mem[Graph.Iri] ?? "");
        internal static Schema Empty = new(); 
        Schema() : base(--_uid,new BTree<long,object>(Infos,
            new BTree<long,ObInfo>(-502,new ObInfo(".",Grant.AllPrivileges))))
        { }
        public Schema (PSchema ps,Context cx)
            :base(ps.ppos,_Mem(cx,ps))
        { }
        public Schema(long dp, BTree<long, object> m) : base(dp, m)
        {  }
        public Schema(long pp, long dp, BTree<long, object>? m = null) : base(pp, dp, m)
        {  }
        static BTree<long,object> _Mem(Context cx,PSchema ps)
        {
            var r = BTree<long, object>.Empty;
            r += (Graph.Iri, ps.directoryPath);
            var oi = new ObInfo(ps.directoryPath, Grant.AllPrivileges);
            r += (Infos, new BTree<long, ObInfo>(cx.role.defpos, oi));
            return r;
        }
        public static Schema operator +(Schema et, (long, object) x)
        {
            return (Schema)et.New(et.defpos, et.mem + x);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new Schema(dp, m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" DirectoryPath: ");sb.Append(directoryPath);
            return sb.ToString();
        }
    }
    internal class TNode : TypedValue
    {
        public readonly TableRow tableRow;
        public long defpos => tableRow.defpos;
        public TypedValue id => tableRow.vals[(dataType as NodeType)?.idCol??-1L]??new TInt(defpos);
        internal TNode(Context cx, TableRow tr)
            : base(_Type(cx,tr))
        {
            tableRow = tr;
        }
        static NodeType _Type(Context cx,TableRow tr)
        {
            var nm = cx.db.objects[tr.tabledefpos] as NodeType ?? throw new PEException("PE50402");
            return nm.Specific(cx, tr);
        }
        internal override TypedValue this[long p] => tableRow.vals[p]??TNull.Value;
        internal virtual bool CheckProps(Context cx,TNode n)
        {
            return dataType.defpos == n.dataType.defpos && id == n.id;
        }
        internal override TypedValue Fix(Context cx)
        {
            return new TNode(cx,tableRow.Fix(cx));
        }
        internal override string ToString(Context cx)
        {
            if (cx.db.objects[dataType.defpos] is not NodeType nt ||
                nt.infos[cx.role.defpos] is not ObInfo ni)
                return "??";
            var sb = new StringBuilder();
            sb.Append(ni.name);
            var cm = '(';
            var tb = nt._PathDomain(cx);
            for (var b = tb.First(); b != null; b = b.Next())
                if (b.value() is long cp && tb.representation[cp] is not null
                    && tb.representation[cp]?.kind!=Qlx.POSITION
                    && (cx.db.objects[cp] as TableColumn)?.infos[cx.role.defpos]?.name is string nm)
                {
                    sb.Append(cm); cm = ',';
                    sb.Append(nm); sb.Append('=');
                    sb.Append(tableRow.vals[cp]);
                }
            if (cm==',')
                sb.Append(')');
            return sb.ToString();
        }
        internal string[] Summary(Context cx)
        {
            if (cx.db.objects[dataType.defpos] is not NodeType nt ||
                nt.infos[cx.role.defpos] is not ObInfo ni)
                return [];
            var ss = new string[Math.Max(nt.Length,5)+1];
            ss[0] = ni.name ?? "";
            for (var b = nt.First(); b != null && b.key() < 5; b = b.Next())
                if (b.value() is long cp && cx.db.objects[cp] is TableColumn tc 
                    && tc.infos[cx.role.defpos] is ObInfo ci)
                {
                    var u = ci.name;
                    var tv = tableRow.vals[cp];
                    var v = tv?.ToString() ?? "??";
                    if (v.Length > 50)
                        v = v[..50];
                    ss[b.key() + 1] = u+" = "+v + Link(cx,tc,tv);
                }
            return ss;
        }
        internal static string Link(Context cx,TableColumn tc,TypedValue? tv)
        {
            if (tc.tc is not TConnector  || cx.db.objects[tc.tabledefpos] is not NodeType nt)
                return "";
            var et = nt as EdgeType;
            if (nt.infos[cx.role.defpos] is not ObInfo li
                || cx.db.objects[nt.idCol] is not TableColumn il 
                || il.infos[cx.role.defpos] is not ObInfo ii)
                return "";
            var sb = new StringBuilder(" [");
            sb.Append(li.name); sb.Append('/');
            sb.Append(ii?.name ?? "??"); sb.Append('='); 
            if (tv is TChar id)
            { sb.Append('\''); sb.Append(id); sb.Append('\''); }
            else
                sb.Append(tv?.ToString()??"??");
            sb.Append(']');
            return sb.ToString();
        }
        public override int CompareTo(object? obj)
        {
            if (obj is not TNode that) return 1;
            return tableRow.defpos.CompareTo(that.tableRow.defpos);
        }
        internal virtual Names _Names(Context cx)
        {
            return (dataType.names!=Names.Empty)?dataType.names:dataType.infos[cx.role.defpos]?.names ?? Names.Empty;
        }
        internal TNode Cast(Context cx,Domain dt)
        {
            if (dt.defpos == dataType.defpos)
                return this;
            if (dataType is NodeType nt && nt.tableRows[defpos] is TableRow tr)
            {
                if (dt.defpos < 0) // calculate specific type
                    return new TNode(cx, new TableRow(defpos, tr.prev, Specific(cx,nt), tr.vals));
                return new TNode(cx, new TableRow(defpos, tr.prev, dt.defpos, tr.vals));
            }
            return this;
        }
        long Specific(Context cx,NodeType nt)
        {
            for (var b = nt.subtypes.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is NodeType tb && tb.tableRows.Contains(defpos))
                    return Specific(cx, tb);
            return nt.defpos;
        }
        public override string ToString()
        {
            return "TNode "+DBObject.Uid(defpos)+"["+ DBObject.Uid(dataType.defpos)+"]";
        }
    }
    internal class TEdge : TNode
    {
        internal TEdge(Context cx, TableRow tr) : base(cx, tr)
        { }
        public override string ToString()
        {
            return "TEdge " + DBObject.Uid(defpos) + "[" + DBObject.Uid(dataType.defpos) + "]";
        }
    }
    internal class TConnector : TypedValue
    {
        public readonly Qlx q;
        public readonly long ct; // A node type or a union of node types
        public readonly string cn;
        public readonly long cp;   // PColumn3 - constructed if needed
        public readonly Domain cd; // Possibly a SET type (Domain.EdgeEnds), most often POSITION
        public string cs;   // string version of metadata
        public readonly TMetadata? cm;
        internal TConnector(Qlx a,long x,string s,Domain d,long p= -1L,string ss="",TMetadata? tm=null) 
            : base(Domain.Connector)
        {
            q = a; ct = x; cn = s; cd = d; cp = p; cs = ss;  cm = tm;
        }
        public override int CompareTo(object? obj)
        {
            if (obj is not TConnector that)
                return 1;
            var c = q.CompareTo(that.q);
            if (c != 0)
                return c;
            c = ct.CompareTo(that.ct);
            if (c != 0)
                return c;
            c = cn.CompareTo(that.cn);
            if (c!=0)
                return c;
//            c = cp.CompareTo(that.cp);
//            if (c != 0)
//                return c;
            return cd.CompareTo(that.cd);
        }
        internal override TypedValue Fix(Context cx)
        {
            var r = (TConnector)base.Fix(cx);
            return new TConnector(r.q,cx.Fix(r.ct), r.cn, (Domain)r.cd.Fix(cx),cx.Fix(r.cp));
        }
        internal override TypedValue Replaced(Context cx)
        {
            return new TConnector(q, cx.Replaced(ct), cn, cd, cx.Replaced(cp),
                cs, (TMetadata)(cm?.Replaced(cx) ?? TMetadata.Empty));
        }
        public override string ToString()
        {
            var sb = new StringBuilder("TConnector ");
            sb.Append(q); sb.Append(' '); sb.Append(cn);
            sb.Append(' '); sb.Append(DBObject.Uid(ct));
            sb.Append(' '); sb.Append(DBObject.Uid(cp));
            if (cm!=TMetadata.Empty)
            { sb.Append(' '); sb.Append(cm); }
            return sb.ToString();
        }
    }
    internal class TJoinedNode : TNode
    {
        public CTree<Domain, bool>? nodeTypes => (dataType as JoinedNodeType)?.nodeTypes; 
        internal TJoinedNode(Context cx,TableRow tr) : base(cx, tr) { }
        internal override Names _Names(Context cx)
        {
            var r = Names.Empty;
            for (var b = cx.db.joinedNodes[defpos]?.First(); b != null; b = b.Next())
                if (b.key() is NodeType nt && nt.infos[cx.role.defpos] is ObInfo oi)
                    r += oi.names;
            return r;
        }
        public override string ToString()
        {
            return "TJoinedNode " + DBObject.Uid(defpos) + "[" + dataType + "]";
        }
    }
    /// <summary>
    /// A class for an unbound identifier (A variable in Francis's paper)
    /// </summary>
    internal class TGParam(long dp, string i, Domain d, TGParam.Type t, long f) : TypedValue(d)
    {
        [Flags]
        internal enum Type { None=0,Node=1,Edge=2,Path=4,Group=8,Maybe=16,Type=32,Field=64,Value=128 };
        internal readonly long uid = dp;
        internal readonly long from = f;
        internal readonly Type type = t; 
        internal readonly string value = i;

        public override int CompareTo(object? obj)
        {
            return (obj is TGParam tp && tp.uid == uid) ? 0 : -1;
        }
        internal DBObject? IsBound(Context cx)
        {
            if (type.HasFlag(Type.Node) && cx.db.objects[cx.role.nodeTypes[value] ?? -1L] is NodeType nt)
                return nt;
            if (type.HasFlag(Type.Edge) && cx.db.objects[cx.role.edgeTypes[value]??-1L] is Domain d)
                return d as EdgeType??Domain.EdgeType;
            if (type.HasFlag(Type.Type) && cx.db.objects[cx.role.dbobjects[value] ?? -1L] is NodeType n)
                return n;
            return null;
        }
        public override string ToString() 
        {
            var sb = new StringBuilder();
            if (uid > 0)
                sb.Append(DBObject.Uid(uid));
            else
                sb.Append((Qlx)(int)-uid);
            sb.Append(' ');
            sb.Append(value);
            return sb.ToString();
        }
    }
}
