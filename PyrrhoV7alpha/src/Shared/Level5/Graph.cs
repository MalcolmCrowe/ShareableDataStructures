using Pyrrho.Level3;
using Pyrrho.Common;
using Pyrrho.Level4;
using System.Text;
using Pyrrho.Level2;
using System.Data.SqlTypes;

namespace Pyrrho.Level5
{
    // Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
    // (c) Malcolm Crowe, University of the West of Scotland 2004-2024
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
            IdIx = -436;    // long Index (defining position used if not specified)
        internal Domain idColDomain => (Domain)(mem[IdColDomain] ?? Position);
        internal Domain label => 
            (Domain)(mem[GqlNode._Label] ?? GqlLabel.Empty);
        internal TRow? singleton => (TRow?)mem[TrivialRowSet.Singleton];
        internal NodeType(Qlx t) : base(t)
        { }
        public NodeType(long dp, BTree<long, object> m) : base(dp, m)
        { }
        internal NodeType(long dp, string nm, UDType dt, BTree<long,object> m, Context cx)
            : base(dp, _Mem(nm, dt, m, cx))
        { }
        static BTree<long, object> _Mem(string nm, UDType dt, BTree<long,object>m, Context cx)
        {
            var r = m + dt.mem + (Kind, Qlx.NODETYPE) -EdgeType.LeaveIx - EdgeType.ArriveIx;
            r += (ObInfo.Name, nm);
            var oi = new ObInfo(nm, Grant.AllPrivileges);
            oi += (ObInfo.Name, nm);
            r += (Definer, cx.role.defpos);
            var rt = BList<long?>.Empty;
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
                                ns += (cn,p);
                                nn += (cn, p);
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
        internal virtual bool HaveNodeOrEdgeType(Context cx)
        {
            if (name != "")
                return cx.role.nodeTypes[name] is long p && p < Transaction.Analysing;
            var pn = CTree<string, bool>.Empty;
            for (var b = representation.First(); b != null; b = b.Next())
                if (cx.NameFor(b.key()) is string n)
                    pn += (n, true);
            return cx.role.unlabelledNodeTypesInfo[pn] is long q && q < Transaction.Analysing;
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
  /*      internal override BList<long?> Add(BList<long?> a, int k, long v, long p)
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
            if (infos[cx.role.defpos] is ObInfo oi && cx._Ob(oi.names[ic.ident]) is DBObject ob)
            {
                if (n is Ident ni)
                    switch (ni.ident)
                    {
                        case "ID":
                            break;
                        default:
                            return ob._Lookup(n.uid, cx, n, n.sub, null);
                    }
                return (new QlInstance(cx.GetUid(), cx, ic.ident, lp, ob), null);
            }
            return base._Lookup(lp, cx, ic, n, r);
        }
        internal override BTree<long, TableRow> For(Context cx, MatchStatement ms, GqlNode xn, BTree<long, TableRow>? ds)
        {
            var th = (NodeType)(cx.db.objects[defpos]??throw new PEException("PE50001"));
            if (th.label != GqlLabel.Empty)
                return th.label.For(cx, ms, xn, ds);
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
            } else  // schema flag
                ds += (defpos, th.Schema(cx));
            return ds;
        }
        /// <summary>
        /// Construct a fake TableRow for a nodetype schema
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal TableRow Schema(Context cx)
        {
            var vals = CTree<long, TypedValue>.Empty;
            for (var b = rowType.First(); b != null; b = b.Next())
                if (cx.db.objects[b.value() ?? -1L] is TableColumn tc)
                    vals += (tc.defpos, new TTypeSpec(tc.domain));
            return new TableRow(defpos, -1L, defpos, vals);
        }
        public override Domain For()
        {
            return NodeType;
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
        internal override CTree<Domain, bool> OnInsert(Context cx,long ap,BTree<long,object>?m=null)
        {
            if (defpos < 0)
                return CTree<Domain, bool>.Empty;
            var nt = this;
            if (!cx.role.dbobjects.Contains(name))
            {
                var pn = new PNodeType(name, this, super, -1L, cx.db.nextPos, cx);
                nt = (NodeType)(cx.Add(pn)??throw new DBException("42105"));
                var dc = (CTree<string, QlValue>?)m?[GqlNode.DocValue];
                for (var b = dc?.First(); b != null; b = b.Next())
                    if (b.value() is QlValue sv && !nt.HierarchyCols(cx).Contains(b.key()))
                    {
                        var pc = new PColumn3(nt, b.key(), -1, sv.domain, PColumn.GraphFlags.None,
                            -1L, -1L, cx.db.nextPos, cx);
                        nt = (NodeType)(cx.Add(pc)??throw new DBException("42105"));
                    }
            }
            nt = (NodeType)(cx._Ob(nt.defpos) ?? nt);
            return new CTree<Domain,bool>(nt,true);
        }
        internal override CTree<Domain, bool> OnInsert(Context cx, BTree<long,long?>? d,
    long lt = -1L, long at = -1L)
        {
            var nt = this;
            if (!cx.role.dbobjects.Contains(name))
            {
                var pn = new PNodeType(name, this, super, -1L, cx.db.nextPos, cx);
                nt = (NodeType)(cx.Add(pn) ?? throw new DBException("42105"));
                cx.obs += (defpos,nt);
                for (var b = d?.First(); b != null; b = b.Next())
                    if (cx.obs[b.key()] is QlValue sc && sc.name is string s
                        && cx.obs[b.value() ?? -1L] is QlValue sv
                        && !nt.HierarchyCols(cx).Contains(s))
                    {
                        var pc = new PColumn3(nt, s, -1, sv.domain, PColumn.GraphFlags.None,
                            -1L, -1L, cx.db.nextPos, cx);
                        if (cx.Add(pc) is NodeType nn)
                        {
                            cx.obs += (defpos, nn);
                            nt = nn;
                        }
                    }
            }
            return new CTree<Domain, bool>(nt, true);
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
        /// <param name="md">A metadata-like set of associations (a la ParseMetadata)</param>
        /// <returns>The new node type: we promise a new PNodeType for this</returns>
        /// <exception cref="DBException"></exception>
        internal virtual NodeType Build(Context cx, GqlNode? x, long _ap, BTree<long,object>? m=null, TMetadata? md=null)
        {
            var ut = this;
            var e = x as GqlEdge;
            if (defpos < 0)
                return this;
            var ls = x?.docValue??CTree<string, QlValue>.Empty;
            var ll = (CTree<string, QlValue>)(m?[GqlNode.DocValue]??CTree<string,QlValue>.Empty);
            ls += ll;
            if (name is not string tn || tn=="")
                throw new DBException("42000", "Node name");
            long? lt = (ut as EdgeType)?.leavingType, at = (ut as EdgeType)?.arrivingType;
            var st = (name!="")?ut.super:CTree<Domain,bool>.Empty;
            // The new Type may not yet have a Physical record, so fix that
            if (!HaveNodeOrEdgeType(cx))
            {
                PNodeType pt;
                for (var b = (m?[NodeTypes] as CTree<Domain,long>)?.First(); b != null; b = b.Next())
                    if (b.key() is UDType ud)
                    {
                        if (ud.infos[cx.role.defpos] is ObInfo u0 && u0.name != tn)
                            ut = (NodeType)ut.New(ut.mem - RowType - Representation + (ObInfo.Name, tn)
                                + (Infos, ut.infos + (cx.role.defpos, u0 + (ObInfo.Name, tn))));
                        else if (ud.name!=name)
                            st += (ud, true);
                    }
                if (this is EdgeType et)
                {
                    if (md?[Qlx.RARROW] is TChar lv && cx.role.dbobjects[lv.value] is long lp)
                        lt = lp;
                    else if ((cx.binding[e?.leavingValue ?? -1L]??cx.values[e?.leavingValue??-1L]) is TNode nl)
                        lt = nl.tableRow.tabledefpos;
                    if (md?[Qlx.ARROW] is TChar av && cx.role.dbobjects[av.value] is long ap)
                        at = ap;
                    else if ((cx.binding[e?.arrivingValue ?? -1L] ?? cx.values[e?.arrivingValue ?? -1L]) is TNode na)
                        at = na.tableRow.tabledefpos;
                    if (lt is null || at is null)
                        throw new DBException("42000").Add(Qlx.INSERT_STATEMENT);
                    var pe = new PEdgeType(tn, et, st, -1L, lt.Value, at.Value, cx.db.nextPos, cx);
                    pt = pe;
                }
                else
                    pt = new PNodeType(tn, ut, st, -1L, cx.db.nextPos, cx);
                ut = (NodeType)(cx.Add(pt) ?? throw new DBException("42105").Add(Qlx.INSERT_STATEMENT));
            }
            // for the metadata tokens used for these identifiers, see the ParseMetadata routine
            var id = ls.Contains("ID")?"ID":(md?[Qlx.NODE] as TChar)?.value ?? (md?[Qlx.EDGE] as TChar)?.value;
            var sl = (md?[Qlx.LPAREN] as TChar)?.value ?? "LEAVING";
            var sa = (md?[Qlx.RPAREN] as TChar)?.value ?? "ARRIVING";
            var le = (md?[Qlx.ARROWBASE] as TBool)?.value ?? false;
            var ae = (md?[Qlx.RARROWBASE] as TBool)?.value ?? false;
            var rt = ut.rowType;
            var rs = ut.representation;
            var sn = BTree<string, long?>.Empty; // properties we are adding
                                                 // check contents of ls
                                                 // ls comes from inline properties in graph create or from ParseRowTypeSpec default value
            var io = names.Contains("ID")?cx.obs[infos[cx.role.defpos]?.names["ID"]??-1L]
                ?? ls["ID"] ?? ((id is not null) ? new SqlLiteral(cx.GetUid(), id, TChar.Empty, Char) : null):null;
            var ii = io?.defpos ?? -1L;
            if (io is not null)
            {
                cx.Add(io);
                (id, rt, rs, sn, ut) = GetColAndIx(cx, ut, ut.FindPrimaryIndex(cx), ii, id,
                    IdIx, IdCol, -1L, false, rt, rs, sn);
            }
            if (ut is EdgeType)
            {
                if ((cx.role.dbobjects[(md?[Qlx.RARROW] as TChar)?.value ?? ""]
                    ??ut.leavingType) is long pL)
                {
                    var rl = (cx.db.objects[lt ?? -1L] as Table)?.FindPrimaryIndex(cx);
                    var lc = rl?.keys?.First()?.value() ?? pL;
                    if (lc < 0 && cx.obs[e?.leavingValue??-1L] is GqlNode g)
                        lc = g.domain.defpos;
                    if (lc < 0)
                        lc = (long)(m?[EdgeType.LeaveCol] ?? -1L);
                    (sl, rt, rs, sn, ut) = GetColAndIx(cx, ut, rl, lc, sl, EdgeType.LeaveIx,
                        EdgeType.LeaveCol, EdgeType.LeavingType, le, rt, rs, sn);
                    cx.Add(ut);
                }
                if ((cx.role.dbobjects[(md?[Qlx.ARROW] as TChar)?.value ?? ""]??ut.arrivingType) is long pA)
                {
                    var al = (cx.db.objects[at ?? -1L] as Table)?.FindPrimaryIndex(cx);
                    var ac = al?.keys?.First()?.value() ?? pA;
                    if (ac < 0 && cx.obs[e?.arrivingValue??-1L] is GqlNode g)
                        ac = g.domain.defpos;
                    if (ac < 0)
                        ac = (long)(m?[EdgeType.ArriveCol] ?? -1L);
                    (sa, rt, rs, sn, ut) = GetColAndIx(cx, ut, al, ac, sa, EdgeType.ArriveIx,
                        EdgeType.ArriveCol, EdgeType.ArrivingType, ae, rt, rs, sn);
                    cx.Add(ut);
                }
                cx.db += ut;
            }
            else if (ut is not null)
            {
                cx.Add(ut);
                cx.db += ut;
            }
            var ui = ut?.infos[cx.role.defpos] ?? throw new DBException("42105").Add(Qlx.TYPE);
            var uds = ut.infos[cx.role.defpos]?.names??Names.Empty;
            for (var b = ls.First(); b != null; b = b.Next())
            {
                var n = b.key();
                if (n != id && n != sl && n != sa && ui?.names.Contains(n) != true)
                {
                    var d = cx._Dom(b.value().defpos) ?? Content;
                    var pc = new PColumn3(ut, n, -1, d, "", TNull.Value, "", CTree<UpdateAssignment, bool>.Empty,
                    false, GenerationRule.None, PColumn.GraphFlags.None, -1L, -1L, cx.db.nextPos, cx);
                    ut = (NodeType)(cx.Add(pc) ?? throw new DBException("42105").Add(Qlx.COLUMN));
                    rt += pc.ppos;
                    rs += (pc.ppos, d);
                    var cn = new Ident(n, pc.ppos);
                    uds += (cn.ident, pc.ppos);
                    cx.Add(ut.name, this);
                    sn += (n, pc.ppos);
                }
            }
            cx.Add(ut);
            cx.db += ut;
            if (id is not null)
                uds += (id, ut.idCol);
            // update defs for inherited properties
            for (var b = ut.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.db.objects[p] is TableColumn uc
                    && uc.infos[uc.definer] is ObInfo ci
                        && ci.name is string sc && p != ut.idCol
                        && !rs.Contains(p))
                {
                    rt += p;
                    rs += (p, ut.representation[p] ?? Domain.Char);
                    uds += (sc, p);
                }
            cx.Add(tn, this);
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
                if (b.value() is long q && ri[q] is int i)
                    uds += (b.key(), q);
            ut += (Infos, new BTree<long, ObInfo>(cx.role.defpos,
                new ObInfo(ut.name, Grant.AllPrivileges)
                + (ObInfo._Names, uds)));
            cx.Add(ut);
            if (ut is EdgeType ee && cx.db.objects[lt ?? -1L] is NodeType ln
                && cx.db.objects[at ?? -1L] is NodeType an)
            {
                ee = ee + (EdgeType.LeavingType, ln.defpos)
                    + (EdgeType.ArrivingType, an.defpos);
                ut = (EdgeType)cx.Add(ee);
            }
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
                    r = (NodeType?)cx.Add(new PColumn3(r, b.key(), -1, b.value().domain,
                    PColumn.GraphFlags.None, -1L, -1L, cx.db.nextPos, cx))
                        ??throw new DBException("42105");
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
        /// <summary>
        /// New indexes are provided in all edgetype cases: if there is no available index for reference we use
        /// a sysrefindex. Edgetype indexes are always treated as a constraint.
        /// For the IdCol case, we construct a primary index if id is specified or "ID" is a column: 
        /// it is not treated as a constraint unless it has been specified as such. (This will then also be
        /// available for referencing edges.)
        /// </summary>
        /// <param name="ut">The node type/edge type</param>
        /// <param name="rx">The primary index or referenced index if already defined</param>
        /// <param name="kc">The key column (may be  GqlNode just now)</param>
        /// <param name="id">The key column name</param>
        /// <param name="xp">IdIx/LeaveIx/ArriveIx</param>
        /// <param name="cp">IdCol/LeaveCol/ArriveCol</param>
        /// <param name="tp">LeaveType/ArriveType</param>
        /// <param name="se">Whether the index Domain is to be a Set</param>
        /// <param name="rt">The node type rowType so far</param>
        /// <param name="rs">The representation of the node type so far</param>
        /// <param name="sn">The names of the columns in the node type so far</param>
        /// <returns>The name of the special column (which may have changed),
        /// the modified domain bits and names for ut, and ut with poissible changes to indexes</returns>
        /// <exception cref="DBException"></exception>
        internal static (string?, BList<long?>, CTree<long, Domain>, BTree<string, long?>, NodeType)
            GetColAndIx(Context cx, NodeType ut, Level3.Index? rx, long kc, string? id, long xp, long cp, long tp,
                bool se, BList<long?> rt, CTree<long, Domain> rs, BTree<string, long?> sn)
        {
            TableColumn? tc = null; // the specified column: it might be an existing one but will maybe get a new index
            PColumn3? pc = null; // the new column if required
            Table? tr; // referenced node type
            Domain? di; // new index key if required
            PIndex? px = null; // primary index if referenced
            DBObject? so = cx._Ob(kc);
            var sd = (so as TableColumn)?.domain ?? (so as QlValue)?.domain ?? Position;
            // the PColumn, if new, needs to record in the transaction log what is going on here
            // using its fields for flags, toType and index information
            PColumn.GraphFlags gf = cp switch
            {
                IdCol => PColumn.GraphFlags.IdCol,
                EdgeType.LeaveCol => PColumn.GraphFlags.LeaveCol,
                EdgeType.ArriveCol => PColumn.GraphFlags.ArriveCol,
                _ => PColumn.GraphFlags.None
            };
            var ns = ut.infos[cx.role.defpos]?.names;
            var cd = se ? new TSet(sd).dataType : sd;
            if (id is null && cp == IdCol && ns?["ID"] is long p
                && cx._Ob(p) is TableColumn)
            {
                id = "ID";
                ut += (IdCol, p);
            }
            if (id is not null)
            {
                if (ns?.Contains(id) == true && ns?[id] is long pp)
                {
                    pc = (PColumn3?)((Transaction)cx.db).physicals[pp];
                    tc = (TableColumn?)(cx._Ob(pp));
                }
                else
                {
                    pc = new PColumn3(ut, id, -1, cd, gf, rx?.defpos ?? -1L, kc,
                        cx.db.nextPos, cx);
                    // see note above
                    ut = (NodeType)(cx.Add(pc) ?? throw new DBException("42105").Add(Qlx.ID));
                    ut += (cp, pc.ppos);
                    sn += (id, pc.ppos);
                    //    rt = Remove(rt, kc); FIX
                    rs -= kc;
                    var ot = rt;
                    rt = new BList<long?>(pc.ppos);
                    for (var c = ot.First(); c != null; c = c.Next())
                        if (c.value() is long op)
                            rt += op;
                    rs += (pc.ppos, cd);
                    tc = (TableColumn)(cx._Ob(pc.ppos) ?? throw new DBException("42105").Add(Qlx.COLUMN));
                    if (rx is null && tc is not null && (cp == EdgeType.LeaveCol || cp == EdgeType.ArriveCol))
                    {
                        tr = cx._Od(tc.toType) as Table ?? throw new DBException("42105").Add(Qlx.CONNECTING);
                        if (pc is not null)
                        {
                            pc.dataType = Position;
                            pc.domdefpos = Position.defpos;
                            pc.toType = tc.toType;
                            var dt = (Transaction)cx.db;
                            dt += (Transaction.Physicals, dt.physicals + (pc.defpos, pc));
                            cx.db = dt;
                        }
                        cx.db += tr;
                        cx.Add(tr);
                    }
                    if (tc is not null)
                    {
                        di = new Domain(-1L, cx, Qlx.ROW, new BList<DBObject>(tc), 1);
                        px = new PIndex(id, ut, di,
                            (cp == IdCol) ? PIndex.ConstraintType.PrimaryKey
                            : (PIndex.ConstraintType.ForeignKey | PIndex.ConstraintType.CascadeUpdate),
                            rx?.defpos ?? -1L, cx.db.nextPos);
                        tc += (Level3.Index.RefIndex, px.ppos);
                    }
                }
            }
            if (pc is not null)
            {
                pc.flags = gf;
                pc.toType = tc?.toType ?? -1L; // -1L for idCol case
                pc.index = rx?.defpos ?? -1L; // ditto
                var ta = (Transaction)cx.db;
                cx.db = ta + (Transaction.Physicals, ta.physicals + (pc.ppos, pc));
            }
            if (tc is not null)
            {
                if (pc is not null)
                    tc += (_Domain, pc.dataType);
                if (tc.flags != gf)
                {
                    tc += (TableColumn.GraphFlag, gf);
                    cx.Add(tc);
                    cx.db += (tc.defpos, tc);
                }
                if (rx != null)
                    tc = tc + (Level3.Index.RefTable, rx.tabledefpos) + (Level3.Index.RefIndex, rx.defpos);
                cx.Add(tc);
                cx.db += (tc.defpos, tc);
                if (ut is EdgeType)
                    ut += (tp, tc.toType);
            }
            if (px is not null)
            {
                ut = (NodeType)(cx.Add(px) ?? throw new DBException("42105").Add(Qlx.PRIMARY));
                if (xp != -1L)
                    ut += (xp, px.ppos);
            }
            if (tc is not null)
                ut += (cp, tc.defpos);
            return (id, rt, rs, sn, ut);
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
                if (tn is not null && tn.dataType is NodeType nt && ntable[nodes[tn]] is NodeInfo ti)
                    for (var b = nt.rindexes.First(); b != null; b = b.Next())
                        if (cx.db.objects[b.key()] is EdgeType rt
                            && cx.db.objects[rt.leavingType] is NodeType lt
                            && cx.db.objects[rt.arrivingType] is NodeType at
                        && nt.defpos >= 0)
                            for (var g = b.value().First(); g != null; g = g.Next())
                            {
                                if (g.key()[0] == rt.leaveCol && cx.db.objects[rt.leaveIx] is Pyrrho.Level3.Index rx
                                && rx.rows?.impl?[tn.id] is TPartial tp)
                                    for (var c = tp.value.First(); c != null; c = c.Next())
                                        if (rt.tableRows[c.key()] is TableRow tr
                                            && (!Have(edges, tr))
                                            && at.Get(cx, tr.vals[rt.arriveCol]) is TableRow ar)
                                        {
                                            if (Have(nodes, ar) is int i && ntable[i] is NodeInfo ai)
                                            {
                                                var te = new TEdge(cx, tr);
                                                edges += (te, (int)ntable.Count);
                                                AddType(ref types, rt);
                                                var ei = new NodeInfo(types[rt], te.id, (ti.x + ai.x) / 2, (ti.y + ai.y) / 2,
                                                    nodes[tn], i, te.Summary(cx));
                                                if (HasSpace(ntable, ei) != null)
                                                    ei = TryAdjust(ntable, ei, ti, ai);
                                                ntable += ei;
                                            }
                                            else
                                            {
                                                var (x, y) = GetSpace(ntable, ti);
                                                if (Math.Abs(x) > 1000 || Math.Abs(y) > 1000)
                                                    continue;
                                                var te = new TEdge(cx, tr);
                                                AddType(ref types, rt);
                                                edges += (te, (int)ntable.Count);
                                                AddType(ref types, at);
                                                ntable += new NodeInfo(types[rt], te.id, x, y, nodes[tn],
                                                    (int)ntable.Count + 1, te.Summary(cx));
                                                var ta = new TNode(cx, ar);
                                                nodes += (ta, (int)ntable.Count);
                                                ntable += new NodeInfo(types[at], ta.id, 2 * x - ti.x, 2 * y - ti.y, -1L, -1L,
                                                    ta.Summary(cx));
                                                todo += ta;
                                            }
                                        }
                                if (g.key()[0] == rt.arriveCol && cx.db.objects[rt.arriveIx] is Pyrrho.Level3.Index ax
                                        && ax.rows?.impl?[tn.id] is TPartial ap)
                                    for (var c = ap.value.First(); c != null; c = c.Next())
                                        if (rt.tableRows[c.key()] is TableRow tr
                                            && (!Have(edges, tr))
                                            && lt.Get(cx, tr.vals[rt.leaveCol]) is TableRow lr)
                                        {
                                            if (Have(nodes, lr) is int i && ntable[i] is NodeInfo li)
                                            {
                                                var te = new TEdge(cx, tr);
                                                edges += (te, (int)ntable.Count);
                                                AddType(ref types, rt);
                                                var ei = new NodeInfo(types[rt], te.id, (ti.x + li.x) / 2, (ti.y + li.y) / 2,
                                                    i, nodes[tn], te.Summary(cx));
                                                if (HasSpace(ntable, ei) != null)
                                                    ei = TryAdjust(ntable, ei, li, ti);
                                                ntable += ei;
                                            }
                                            else
                                            {
                                                var (x, y) = GetSpace(ntable, ti);
                                                if (Math.Abs(x) > 1000 || Math.Abs(y) > 1000)
                                                    continue;
                                                var te = new TEdge(cx, tr);
                                                edges += (te, (int)ntable.Count);
                                                AddType(ref types, rt);
                                                ntable += new NodeInfo(types[rt], te.id, x, y, (int)ntable.Count + 1, nodes[tn],
                                                    te.Summary(cx));
                                                var tl = new TNode(cx, lr);
                                                nodes += (tl, (int)ntable.Count);
                                                AddType(ref types, lt);
                                                ntable += new NodeInfo(types[lt], tl.id, 2 * x - ti.x, 2 * y - ti.y, -1L, -1L,
                                                    tl.Summary(cx));
                                                todo += tl;
                                            }
                                        }
                            }
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
                var c = label.CompareTo(that.label);
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
    /// For interpretation of labels: because of type renaming the result is role-dependent!
    /// left and right can be NodeType defpos (if name is "")
    /// Domain is a node/edge type
    /// op can be Qlx.EXCLAMATION,Qlx.VBAR,Qlx.AMPERSAND,Qlx.IMPLIES,Qlx.COLON
    /// OnInsert gives an unordered list of Node/Edgetype defpos (a CTree)
    /// For is used in Match to give a list of TNodes that match a given GqlNode/Edge/Path
    /// OnExtra method gives a single node/edgetype if available for allowExtras 
    /// </summary>
    internal class GqlLabel : Domain
    {
        internal static GqlLabel Empty = new();
        internal long left => (long)(mem[QlValue.Left] ?? -1L);
        internal long right => (long)(mem[QlValue.Right] ?? -1L);
        public GqlLabel(long dp, long lf, long rg, BTree<long, object>? m = null)
            : base(dp, (m ?? BTree<long, object>.Empty)
                  + (QlValue.Left, lf) + (QlValue.Right, rg))
        { }
        internal GqlLabel(Ident id, Context cx,NodeType? lt = null, NodeType? at = null, BTree<long,object>? m = null)
            : this(id.uid, id.ident, cx, lt?.defpos, at?.defpos, m) { }
        internal GqlLabel(long dp, string nm,  Context cx, long? lt = null, long? at = null, 
            BTree<long,object>? m=null)
            : this(dp, _Mem(cx, nm, lt, at, m))
        { }
        GqlLabel() : this(-1L, -1L, -1L) { }
        internal GqlLabel(long dp, BTree<long, object> m) : base(dp, m)
        { }
        static BTree<long, object> _Mem(Context cx, string id, long? lt, long? at, BTree<long,object>?m)
        {
            m ??= BTree<long, object>.Empty;
            m = m + (ObInfo.Name, id);
            Domain dt = NodeType;
            if (!m.Contains(Kind))
                m += (Kind, (lt is null || at is null) ? Qlx.NODETYPE : Qlx.EDGETYPE);
            else if (((Qlx)(m[Kind]??Qlx.NO)) == Qlx.EDGETYPE)
                dt = EdgeType;
            if (lt is not null && lt>=0 && at is not null && at>=0)
            {
                m = m + (GqlEdge.LeavingValue, lt.Value) + (GqlEdge.ArrivingValue, at.Value);
                dt = EdgeType;
            }
            var sd = (lt is null || at is null) ?
                (cx.db.objects[cx.role.nodeTypes[id] ?? -1L] as Domain)
                : (cx.db.objects[cx.role.edgeTypes[id]?? -1L] as Domain);
            if (sd is EdgeType se && (se.leavingType != lt || se.arrivingType != at))
                sd = null;
            if (sd?.kind == Qlx.UNION)
                for (var c = sd.unionOf.First(); sd is null && c != null; c = c.Next())
                    if (cx.db.objects[c.key().defpos] is EdgeType sf
                        && sf.leavingType == lt && sf.arrivingType == at)
                        sd = sf;
            sd ??= cx.db.objects[cx.role.dbobjects[id] ?? -1L] as Domain;
            if (sd is not null)
            {
                m = m + (_Domain, sd) + (Kind, sd.kind);
                cx.AddDefs(sd);
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
        internal override CTree<Domain, bool> OnInsert(Context cx, long ap, BTree<long, object>? m = null)
        {
           var r = CTree<Domain, bool>.Empty;
            var lf = cx.obs[left] as Domain ?? Empty;
            var rg = cx.obs[right] as Domain ?? Empty;
            m ??= BTree<long, object>.Empty;
            var lm = m;
            var rm = m;
            if (kind == Qlx.COLON)
            {
                if (left > right)
                    lm += (Under, lf.super + (rg, true));
                else
                    rm += (Under, rg.super + (lf, true));
            }
            var lt = ((long?)m[EdgeType.LeavingType]) ?? cx.obs[(long)(m[GqlEdge.LeavingValue] ?? -1L)]?.domain.defpos ?? -1L;
            var at = ((long?)m[EdgeType.ArrivingType]) ?? cx.obs[(long)(m[GqlEdge.ArrivingValue] ?? -1L)]?.domain.defpos ?? -1L;
            var k = kind;
            if (lt >= 0 && at >= 0)
            { // lt and at may be GqlNodes for now: will be fixed in Build
                m = m + (EdgeType.LeaveCol, lt) + (EdgeType.ArriveCol, at);
                if (k == Qlx.NODETYPE)
                {
                    cx.Add(this + (Kind, Qlx.EDGETYPE));
                    k = Qlx.EDGETYPE;
                }
            }
            else if (k == Qlx.EDGETYPE)
                k = Qlx.COLON;
            var dc = (CTree<string, QlValue>)(m[GqlNode.DocValue] ?? CTree<string, QlValue>.Empty);
            return k switch
            {
                Qlx.AMPERSAND or Qlx.COLON => lf.OnInsert(cx, ap, lm) + rg.OnInsert(cx, ap, rm),
                Qlx.NODETYPE => (name is string n) ?
                    r + (cx.FindNodeType(n, dc)?.Build(cx, null, ap, m) ?? NodeTypeFor(n, m, cx), true)
                    : r,
                Qlx.EDGETYPE => (cx.FindEdgeType(name, lt, at, dc, m, TMetadata.Empty) is CTree<Domain, bool> rr
                    && rr.Count > 0) ? rr : new CTree<Domain, bool>(EdgeTypeFor(name, m, cx), true),
                Qlx.NO => (cx.db.objects[cx.role.dbobjects[name] ?? -1L] is NodeType d) ?
                    r + (d, true) : r,
                Qlx.DOUBLEARROW => (lf is null || rg is null) ? r
                : (cx.db.objects[Math.Min(lf.defpos, rg.defpos)] is NodeType tt) ? (r + (tt, true)) : r,
                _ => r
            };
        }
        internal override bool Match(Context cx, CTree<long, bool> ts, Qlx tk = Qlx.Null)
        {
            var lf = cx.obs[left] as Domain ?? Empty;
            var rg = cx.obs[right] as Domain ?? Empty;
            return kind switch
            {
                Qlx.AMPERSAND or Qlx.COLON => ts.Contains(cx.role.dbobjects[domain.name] ?? -1L),
                Qlx.NODETYPE or Qlx.EDGETYPE => tk == kind || ts.Contains(domain.defpos),
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
                var pt = new PNodeType(nm, NodeType, nu, -1L, cx.db.nextPos, cx);
                nt = (NodeType)(cx.Add(pt) ?? throw new DBException("42105"));
                for (var b = dc?.First(); b != null; b = b.Next())
                    if (!nt.HierarchyCols(cx).Contains(b.key()))
                    {
                        var pc = new PColumn3(nt, b.key(), -1, b.value().domain, PColumn.GraphFlags.None,
                            -1L, -1L, cx.db.nextPos, cx);
                        nt = (NodeType)(cx.Add(pc)??throw new DBException("42105"));
                    }
                nt = nt.Build(cx, null, 0L, m);
            }
            return nt ?? throw new DBException("42105");
        }
        internal static EdgeType EdgeTypeFor(string nm, BTree<long, object> m, Context cx)
        {
            var un = (CTree<Domain, bool>)(m[Under] ?? CTree<Domain, bool>.Empty);
            var nu = CTree<Domain, bool>.Empty;
            for (var b = un.First(); b != null; b = b.Next())
                nu += ((b.key() is GqlLabel gl) ? (cx.db.objects[cx.role.dbobjects[gl.name ?? ""] ?? -1L]
                    as Domain) ?? throw new DBException("42107", gl.name ?? "??") : b.key(), true);
            var lt = (long)(m[EdgeType.LeavingType] ?? -1L);
            var at = (long)(m[EdgeType.ArrivingType] ?? -1L);
            var dc = (CTree<string, QlValue>)(m[GqlNode.DocValue] ?? CTree<string, QlValue>.Empty);
            var pt = new PEdgeType(nm, EdgeType, nu, -1L, lt, at, cx.db.nextPos, cx);
            var ro = cx.role;
            var e = (EdgeType?)cx.Add(pt) ?? throw new DBException("42105");
            for (var b = dc?.First(); b != null; b = b.Next())
                if (!e.HierarchyCols(cx).Contains(b.key()))
                {
                    var pc = new PColumn3(e, b.key(), -1, b.value().domain, PColumn.GraphFlags.None,
                        -1L, -1L, cx.db.nextPos, cx);
                    e = (EdgeType)(cx.Add(pc) ?? throw new DBException("42105"));
                }
            e = (EdgeType)e.Build(cx, null, 0L, m);
            return e;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            if (name is string nm && nm != kind.ToString())
            {
                sb.Append(' '); sb.Append(nm);
            }
            if (kind == Qlx.NO)
                sb.Append(base.ToString());
            else if (defpos>0)
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
    /// structural information about leaving and arriving is copied to subtypes.
    /// For an undirected edge LeavingEnds is true, and there is no ArrivingType etc,
    /// and the default identifier CONNECTING is used instead of LEAVING.
    /// </summary>
    internal class EdgeType : NodeType
    {
        internal const long
            ArriveCol = -474, // long TableColumn  NameFor is md[RPAREN]
            ArriveColDomain = -495, // Domain (usually INT or CHAR)
            ArriveIx = -435, // long Index
            ArrivingEnds = -371, // bool
            ArrivingType = -467, // long NodeType  NameFor is md[ARROW]
            LeaveCol = -473, // long TableColumn   NameFor is md[LPAREN]
            LeaveColDomain = -494, // Domain (usually INT or CHAR)
            LeaveIx = -470, // long Index
            LeavingEnds = -377, // bool
            LeavingType = -464; // long NodeType   NameFor is md[RARROW]
        public bool leavingEnds => (bool)(mem[LeavingEnds] ?? false);
        public Domain leaveColDomain => (Domain)(mem[LeaveColDomain] ?? Int);
        public bool arrivingEnds => (bool)(mem[ArrivingEnds] ?? false);
        public Domain arriveColDomain => (Domain)(mem[ArriveColDomain] ?? Int);
        public bool undirected => (bool)(mem[QuantifiedPredicate.Between] ?? false);
        internal EdgeType(long dp, string nm, UDType dt, BTree<long,object> m, Context cx, 
            TMetadata? md = null)
            : base(dp, nm, dt, _Mem(m, md, cx), cx)
        { }
        internal EdgeType(Qlx t) : base(t)
        { }
        public EdgeType(long dp, BTree<long, object> m) : base(dp, m)
        { }
        static BTree<long, object> _Mem(BTree<long,object> m, TMetadata? md, Context cx)
        {
            var lv = (long?)m[GqlEdge.LeavingValue];
            var av = (long?)m[GqlEdge.ArrivingValue];
            var lt = (long?)m[LeavingType]??cx.obs[lv??-1L]?.domain?.defpos;
            var at = (long?)m[ArrivingType] ?? cx.obs[av??-1L]?.domain?.defpos;
            var sl = (bool?)m[LeavingEnds];
            var sa = (bool?)m[ArrivingEnds];
            if (md != null)
            {
                TChar? ln = null, an = null;
                if (md[Qlx.RARROWBASE] is TChar aa) { an = aa; sa = true; }
                if (md[Qlx.ARROW] is TChar ab) an = ab;
                if (md[Qlx.ARROWBASE] is TChar la) { ln = la; sl = true; }
                if (md[Qlx.RARROW] is TChar lb) ln = lb;
                m += (LeavingType, lt??cx.role.dbobjects[ln?.value??"?"]??NodeType.defpos);
                m += (ArrivingType, at??cx.role.dbobjects[an?.value??"?"]??NodeType.defpos);
                if (sl is not null) m += (LeavingEnds, sl);
                if (sa is not null) m += (ArrivingEnds, true);
            }
            // Now see NodeType._Mem
            return m;
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
            return (UDType)(cx.Add(new PEdgeType(pn.ident, nd, un, -1L, nd.leavingType, nd.arrivingType, dp, cx))
                ?? throw new DBException("42105").Add(Qlx.EDGETYPE));
        } 
        internal override NodeType Check(Context cx, GqlNode n, long ap, bool allowExtras = true)
        {
            var et = base.Check(cx, n, ap, allowExtras);
            var e = (GqlEdge)n;
            if (cx.obs[e.leavingValue] is not GqlNode nl || cx.obs[e.arrivingValue] is not GqlNode na)
                throw new PEException("PE60904");
            if (cx._Ob(et.leavingType) is NodeType el && cx._Ob(et.arrivingType) is NodeType ea)
            {
                var ll = nl.label.OnInsert(cx, ap, nl.mem) + nl.domain.FindType(cx,nl.docValue);
                if (nl is GqlReference hr && cx.db.objects[hr.refersTo] is NodeType hl)
                    ll += (hl, true);
                else if (nl is GqlNode sl && cx.binding[sl.defpos] is TNode ln)
                {
                    if (ln.dataType.nodeTypes.Count == 0)
                        ll += (ln.dataType, true);
                    else
                        for (var b = ln.dataType.nodeTypes.First(); b != null; b = b.Next())
                            if (b.key() is NodeType nt)
                                ll += (nt, true);
                }
                var al = na.label.OnInsert(cx,ap, na.mem) + na.domain.FindType(cx, na.docValue);
                if (na is GqlReference hs && cx.db.objects[hs.refersTo] is NodeType ha)
                    al += (ha, true);
                else if (na is GqlNode sa && cx.binding[sa.defpos] is TNode an)
                    for (var b = cx.NodeTypes(an.tableRow.tabledefpos).First(); b != null; b = b.Next())
                        if (b.key() is NodeType nt)
                            al += (nt, true);
                if (!ll.Contains(el) || !al.Contains(ea))
                {
                    if (nl.label.kind==Qlx.AMPERSAND || na.label.kind == Qlx.AMPERSAND)
                        return et; // will be sorted out by ChooseEnds
                    var md = TMetadata.Empty + (Qlx.ARROW, new TChar(na.domain.name))
                        + (Qlx.RARROW, new TChar(nl.domain.name));
                    var ne = new EdgeType(cx.GetUid(), name, EdgeType, n.mem, cx,md);
                    et = ne.Build(cx, n, ap, null, md);
                }
                var ls = n.docValue;
                if (el.infos[cx.role.defpos] is ObInfo li
                    && li.name is string en && ls[en] is QlValue vl && vl.Eval(cx) is TChar lv
                    && cx.db.objects[NodeTypeFor(cx, lv)] is NodeType lt
                    && !lt.EqualOrStrongSubtypeOf(el))
                {
                    var pp = cx.db.nextPos;
                    var pi = new Ident(pp.ToString(), pp);
                    var xt = (Domain)cx.Add(et + (LeavingType,
                        new NodeType(cx.GetUid(), pi.ident, TypeSpec, el.mem, cx).defpos));
                    cx.Add(xt);
                }
                if (ea.infos[cx.role.defpos] is ObInfo ai
                    && ai.name is string aa && ls[aa] is QlValue va && va.Eval(cx) is TChar av
                    && cx.db.objects[NodeTypeFor(cx, av)] is NodeType at
                    && !at.EqualOrStrongSubtypeOf(ea))
                {
                    var pp = cx.db.nextPos;
                    var pi = new Ident(pp.ToString(), pp);
                    var xt = (Domain)cx.Add(et + (ArrivingType,
                        new NodeType(cx.GetUid(), pi.ident, TypeSpec, ea.mem, cx).defpos));
                    cx.Add(xt);
                }
            }
            return et;
        }
        internal override void AddNodeOrEdgeType(Context cx)
        {
            var ro = cx.role;
            var nm = name??label.name;
            if (nm != "")
            {
                var ep = ro.edgeTypes[nm];
                var ed = cx._Ob(ep ?? -1L) as Domain;
                if (ep is null || ed?.kind==Qlx.NODETYPE) // second term here is for Metadata.EdgeType
                {
                    ro += (Role.EdgeTypes, ro.edgeTypes+(nm, defpos));
                    cx.db += ro;
                }
                else if (ed is not null && ed.defpos!=defpos)
                {
                    if (ed.kind == Qlx.EDGETYPE)
                        ed = (Domain)cx.Add(UnionType(ed.defpos+1L, ed, this));
                    else if (ed.kind == Qlx.UNION)
                        ed = (Domain)cx.Add(new Domain(ed.defpos, Qlx.UNION, ed.unionOf + (this, true)));
                    else throw new PEException("PE20901");
                    ro += (Role.EdgeTypes, ro.edgeTypes+(nm, ed.defpos));
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
                var ud = cx.db.objects[ro.unlabelledEdgeTypesInfo[pn] ?? -1L] as Domain;
                if (ud is null)
                {
                    ro += (Role.UnlabelledNodeTypesInfo, ro.unlabelledNodeTypesInfo + (pn, defpos));
                    cx.db += (Database.UnlabelledNodeTypes, cx.db.unlabelledNodeTypes + (ps, defpos));
                    cx.db += ro;
                } else
                {
                    if (ud.kind == Qlx.EDGETYPE)
                        ud = (Domain)cx.Add(UnionType(cx.GetUid(), ud, this));
                    else if (ud.kind == Qlx.UNION)
                        ud = (Domain)cx.Add(new Domain(cx.GetUid(), Qlx.UNION, ud.unionOf + (this, true)));
                    else throw new PEException("PE20901");
                    ro += (Role.UnlabelledNodeTypesInfo, ro.unlabelledNodeTypesInfo + (pn, ud.defpos));
                    cx.db += (Database.UnlabelledNodeTypes, cx.db.unlabelledNodeTypes + (ps, ud.defpos));
                    cx.db += ro;
                }
            }
            cx.db += (Database.Role, cx.db.objects[cx.role.defpos]??throw new DBException("42105"));
            cx.db += this;
        }
        internal override bool HaveNodeOrEdgeType(Context cx)
        {
            if (name != "")
            {
                if (cx.role.edgeTypes[name] is long ep && cx.db.objects[ep] is Domain d)
                {
                    if (d is EdgeType et && et.leavingType == leavingType && et.arrivingType == arrivingType)
                        return true;
                    if (d.kind == Qlx.UNION)
                        for (var c = d.unionOf.First(); c != null; c = c.Next())
                            if (cx.db.objects[c.key().defpos] is EdgeType ee
                                && ee.leavingType == leavingType && ee.arrivingType == arrivingType)
                                return true;
                }
                return false;
            }
            var pn = CTree<string, bool>.Empty;
            for (var b = representation.First(); b != null; b = b.Next())
                if (cx.NameFor(b.key()) is string n)
                pn += (n, true);
            return cx.role.unlabelledEdgeTypesInfo.Contains(pn) == true;
        }
        internal EdgeType FixEdgeType(Context cx, PType pt)
        {
            FixColumns(cx, 1);
            pt.under = cx.FixTDb(super);
            pt.dataType = this;
            return (EdgeType)(cx.Add(pt) ?? throw new DBException("42105").Add(Qlx.EDGETYPE));
        }
        static long NodeTypeFor(Context cx, TypedValue? v)
        {
            if (v is TTypeSpec ts && ts._dataType is NodeType nt)
                return nt.defpos;
            if (v is TChar s)
                return cx.role.dbobjects[s.value] ?? -1L;
            return -1L;
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
                        if (ed is EdgeType ee && (ee.leavingType!=leavingType || ee.arrivingType!=arrivingType))
                        {
                            var eu = cx.Add(UnionType(cx.GetUid(), ee, this));
                            ro += (Role.EdgeTypes, ro.edgeTypes + (oi.name, eu.defpos));
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
                    if (pd is EdgeType pe)
                    { // leaveCol, arriveCol are never <0 if the edgeType has been fully defined
                      // we want to avoid pollution of the rown type meantime
                        if (!rs.Contains(pe.leaveCol) && pe.leaveCol>=0)
                        {
                            rt += pe.leaveCol;
                            rs += (pe.leaveCol, pe.leaveColDomain);
                        }
                        if (!rs.Contains(pe.arriveCol) && pe.arriveCol>=0)
                        {
                            rt += pe.arriveCol;
                            rs += (pe.arriveCol, pe.arriveColDomain);
                        }
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
            if (leaveCol >= 0)
                to += (LeaveCol, leaveCol);
            if (arriveCol >= 0)
                to += (ArriveCol, arriveCol);
            if (leavingType >= 0)
                to += (LeavingType, leavingType);
            if (arrivingType >= 0)
                to += (ArrivingType, arrivingType);
            if (leaveIx >=0)
                to += (LeaveIx, leaveIx);
            if (arriveIx >= 0)
                to += (ArriveIx, arriveIx);
            return base.Inherit(to);
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
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx, m);
            var lt = cx.Fix(leavingType);
            if (lt != leavingType)
                r += (LeavingType, lt);
            var at = cx.Fix(arrivingType);
            if (at != arrivingType)
                r += (ArrivingType, at);
            var lc = cx.Fix(leaveCol);
            if (lc != leaveCol)
                r += (LeaveCol, lc);
            var lx = cx.Fix(leaveIx);
            if (lx != leaveIx)
                r += (LeaveIx, lx);
            var ac = cx.Fix(arriveCol);
            if (ac != arriveCol)
                r += (ArriveCol, ac);
            var ax = cx.Fix(arriveIx);
            if (ax != arriveIx)
                r += (ArriveIx, ax);
            return r;
        }
        internal override Basis ShallowReplace(Context cx, long was, long now)
        {
            var r = (EdgeType)base.ShallowReplace(cx, was, now);
            var ch = false;
            if (leavingType==was)
            {
                r += (LeavingType, now); ch = true;
            }
            if (leaveCol == was)
            {
                r += (LeaveCol, now); ch = true;
            }
            if (leaveIx == was)
            {
                r += (LeaveIx, now); ch = true;
            }
            if (arrivingType == was)
            {
                r += (ArrivingType, now); ch = true;
            }
            if (arriveCol == was)
            {
                r += (ArriveCol, now); ch = true;
            }
            if (arriveIx == was)
            {
                r += (ArriveIx, now); ch = true;
            }
            if (ch)
                cx.Add(r);
            return r;
        }
        public static EdgeType operator +(EdgeType et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (dp == LeavingType && et.leavingType>=0 && ob is long v && v<0 )
                return et;
            if (dp == ArrivingType && et.arrivingType>=0 && ob is long v1 && v1<0)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (EdgeType)et.New(m + x);
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
        internal override CTree<Domain, bool> OnInsert(Context cx, BTree<long, long?>? d,
    long lt = -1, long at = -1)
        {
            if (lt == leavingType && at == arrivingType)
                return base.OnInsert(cx, d, lt, at);
            var pe = new PEdgeType(name, this, super, -1L, lt, at,cx.db.nextPos,cx); 
            var et = (EdgeType)(cx.Add(pe) ?? throw new DBException("42105"));
            cx.obs += (defpos, et);
            for (var b = d?.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is QlValue sc && sc.name is string s
                    && cx.obs[b.value() ?? -1L] is QlValue sv
                    && !et.HierarchyCols(cx).Contains(s))
                {
                    var pc = new PColumn3(et, s, -1, sv.domain, PColumn.GraphFlags.None,
                        -1L, -1L, cx.db.nextPos, cx);
                    cx.Add(pc);
                }
            return new CTree<Domain, bool>(et, true);
        }
        internal override Domain MakeUnder(Context cx, DBObject so)
        {
            return (so is EdgeType sn) ? ((EdgeType)New(defpos,mem + (Under, super+(sn,true)))) : this;
        }
        internal override DBObject Relocate(long dp, Context cx)
        {
            var r = (EdgeType)base.Relocate(dp, cx)
                + (LeavingType, cx.Fix(leavingType))
                + (ArrivingType, cx.Fix(arrivingType));
            return (EdgeType)cx.Add(r);
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
            if (cx.db.objects[leavingType] is NodeType ln && cx.db.objects[arrivingType] is NodeType an)
            {
                sb.Append(" CONNECTING (");
                sb.Append(ln.name); sb.Append("->");
                sb.Append(an.name); sb.Append(')');
            }
            return sb.ToString();
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (leavingType != -1L)
            {
                sb.Append(" Leaving "); sb.Append(Uid(leavingType));
                sb.Append("[" + Uid(leaveIx) + "]");
                sb.Append(" LeaveCol="); sb.Append(Uid(leaveCol));
            }
            if (arrivingType != -1L)
            {
                sb.Append(" Arriving "); sb.Append(Uid(arrivingType));
                sb.Append("[" + Uid(arriveIx) + "]");
                sb.Append(" ArriveCol="); sb.Append(Uid(arriveCol));
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
        internal JoinedNodeType(long dp, string nm, UDType dt, BTree<long,object> m, Context cx) 
            : base(dp, nm, dt, m, cx)
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
        internal override NodeType Build(Context cx, GqlNode? x, long ap, BTree<long,object>? mm = null, TMetadata? md=null)
        {
            var ids = CTree<long, TypedValue>.Empty;
            var it = Null;
            var ls = (CTree<string,QlValue>?)mm?[GqlNode.DocValue];
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
            var fl = CTree<long, TypedValue>.Empty;
            var dp = cx.db.nextPos;
            var tbs = CTree<long, bool>.Empty;
            for (var b = nodeTypes.First(); b != null; b = b.Next())
                if (b.key() is NodeType nt && x is not null)
                {
                    var np = cx.GetUid();
                    var m = new BTree<long, object>(GqlNode._Label,
                        cx.Add(new GqlLabel(cx.GetUid(),nt.name,cx)));
                    var nd = new GqlNode(new Ident(Uid(np), np), CList<Ident>.Empty, cx,
                        -1L, x.docValue, x.state, nt, m);
                    nd.Create(cx, nt, ap, false);
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
        internal GraphType(PGraphType pg, Context cx)
            : this(pg.ppos, _Mem(pg,cx))
        { }
        public GraphType(long dp, BTree<long, object> m) : base(dp, m)
        { }
        public GraphType(long pp, long dp, BTree<long, object>? m = null) : base(pp, dp, m)
        {  }
        static BTree<long,object> _Mem(PGraphType pg,Context cx)
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
                    ns += (ut.name, b.key());
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
        internal Graph(PGraph pg,Context cx)
            : base(pg.ppos,_Mem(cx,pg))
        { }
        public Graph(long dp, BTree<long, object> m) : base(dp, m)
        { }
        static BTree<long,object> _Mem(Context cx,PGraph ps)
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
                    ns += (ut.name, b.key());
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
            : base(cx.db.objects[tr.tabledefpos] as NodeType ?? throw new PEException("PE50402"))
        {
            tableRow = tr;
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
            for (var b = nt.First(); b != null; b = b.Next())
                if (b.value() is long cp && nt.representation[cp] is not null
                    && nt.representation[cp]?.kind!=Qlx.POSITION
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
            if (tc.flags == PColumn.GraphFlags.None
                || cx.db.objects[tc.tabledefpos] is not NodeType nt)
                return "";
            var et = nt as EdgeType;
            var lt = tc.flags switch
            {
                PColumn.GraphFlags.IdCol => nt,
                PColumn.GraphFlags.LeaveCol => cx.db.objects[et?.leavingType??-1L] as NodeType,
                PColumn.GraphFlags.ArriveCol => cx.db.objects[et?.arrivingType??-1L] as NodeType,
                _ => null
            };
            if (lt is null || lt is EdgeType || lt.infos[cx.role.defpos] is not ObInfo li
                || cx.db.objects[lt.idCol] is not TableColumn il 
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
            return dataType.infos[cx.role.defpos]?.names ?? Names.Empty;
        }
        public override string ToString()
        {
            return "TNode "+DBObject.Uid(defpos)+"["+ DBObject.Uid(dataType.defpos)+"]";
        }
    }
    internal class TEdge : TNode
    {
        public TypedValue leaving => tableRow.vals[(dataType as EdgeType)?.leaveCol ?? -1L]??TNull.Value;
        public TypedValue arriving => tableRow.vals[(dataType as EdgeType)?.arriveCol ?? -1L]??TNull.Value;
        internal TEdge(Context cx, TableRow tr) : base(cx, tr)
        { }
        public override string ToString()
        {
            return "TEdge " + DBObject.Uid(defpos) + "[" + DBObject.Uid(dataType.defpos) + "]";
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
