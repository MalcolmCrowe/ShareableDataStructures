﻿using Pyrrho.Level3;
using Pyrrho.Common;
using Pyrrho.Level4;
using System.Text;
using Pyrrho.Level2;
using System.Net.Http.Headers;
using System.Security.Cryptography;

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
            IdColDomain = -493, // Domain (by default is Int)
            IdIx = -436,    // long Index (defining position used if not specified)
            Labels = -482; // CTree<long,bool> Type (existing labels are always graph type names)
        internal Domain idColDomain => (Domain)(mem[IdColDomain] ?? Int);
        internal CTree<long, bool> labels => (CTree<long, bool>)(mem[Labels] ?? CTree<long, bool>.Empty);
        internal NodeType(Sqlx t) : base(t)
        { }
        public NodeType(long dp, BTree<long, object> m) : base(dp, m)
        { }
        internal NodeType(long dp, string nm, UDType dt, CTree<Domain, bool> ut, Context cx)
            : base(dp, _Mem(nm, dt, ut, cx))
        { }
        static BTree<long, object> _Mem(string nm, UDType dt, CTree<Domain, bool> un, Context cx)
        {
            var r = dt.mem + (Kind, Sqlx.NODETYPE);
            r += (ObInfo.Name, nm);
            var oi = new ObInfo(nm, Grant.AllPrivileges);
            oi += (ObInfo.Name, nm);
            r += (Definer, cx.role.defpos);
            r += (Under, un);
            var rt = BList<long?>.Empty;
            var rs = CTree<long, Domain>.Empty;
            var ns = BTree<string, (int, long?)>.Empty;
            var n = 0;
            // At this stage we don't know anything about non-identity columns
            // add everything we find in direct supertypes to create the PathDomain
            for (var tb = un.First(); tb != null; tb = tb.Next())
                if (cx._Ob(tb.key().defpos) is Table pd)
                {
                    var rp = pd.representation;
                    for (var c = pd.rowType.First(); c != null; c = c.Next())
                        if (c.value() is long p && rp[p] is Domain rd && rd.kind != Sqlx.Null
                            && cx._Ob(p) is TableColumn tc && tc.infos[cx.role.defpos] is ObInfo ci
                            && ci.name is string cn && !ns.Contains(cn))
                        {
                            rt += p;
                            rs += (p, rd);
                            ns += (cn, (n++, p));
                        }
                }
            oi += (ObInfo.Names, ns);
            r += (PathDomain, new Domain(cx, rs, rt, new BTree<long, ObInfo>(cx.role.defpos, oi)));
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
                return rt;
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
        internal override Basis New(BTree<long, object> m)
        {
            return new NodeType(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new NodeType(dp, m + (Kind, Sqlx.NODETYPE));
        }
        internal override UDType New(Ident pn, CTree<Domain, bool> un, long dp, Context cx)
        {
            return (UDType)(cx.Add(new PNodeType(pn.ident, (NodeType)NodeType.Relocate(dp),
                un, -1L, dp, cx)) ?? throw new DBException("42105"));
        }
        internal override Table _PathDomain(Context cx)
        {
            var rt = rowType;
            var rs = representation;
            var ii = infos;
            var gi = rs.Contains(idCol) || idCol < 0L;
            for (var tb = super.First(); tb != null; tb = tb.Next())
                if (cx._Ob(tb.key().defpos) is Table pd)
                {
                    for (var b = infos.First(); b != null; b = b.Next())
                        if (b.value() is ObInfo ti)
                        {
                            if (pd.infos[cx.role.defpos] is ObInfo si)
                                ti += si;
                            else throw new DBException("42105");
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
        internal override DBObject Add(Context cx, PMetadata pm, long p)
        {
            var ro = cx.role;
            if (pm.detail.Contains(Sqlx.NODETYPE) && infos[ro.defpos] is ObInfo oi
                && oi.name is not null)
            {
                ro += (Role.NodeTypes, ro.nodeTypes + (oi.name, defpos));
                cx.db += (ro, p);
            }
            return base.Add(cx, pm, p);
        }
        internal override BList<long?> Add(BList<long?> a, int k, long v, long p)
        {
            if (p == idCol)
                k = 0;
            return base.Add(a, k, v, p);
        }
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
        internal override (DBObject?, Ident?) _Lookup(long lp, Context cx, string nm, Ident? n, DBObject? r)
        {
            if (infos[cx.role.defpos] is ObInfo oi && oi.names[nm].Item2 is long p && cx._Ob(p) is DBObject ob)
            {
                if (n is Ident ni)
                    switch (ni.ident)
                    {
                        case "ID":
                            break;
                        default:
                            return ob._Lookup(n.iix.dp, cx, n.ident, n.sub, null);
                    }
                return (new SqlCopy(cx.GetUid(), cx, nm, lp, ob), null);
            }
            return base._Lookup(lp, cx, nm, n, r);
        }
        public override Domain For()
        {
            return NodeType;
        }
        internal override int Typecode()
        {
            return 3;
        }
        internal override RowSet RowSets(Ident id, Context cx, Domain q, long fm,
    Grant.Privilege pr = Grant.Privilege.Select, string? a = null)
        {
            cx.Add(this);
            var m = BTree<long, object>.Empty + (_From, fm) + (_Ident, id);
            if (a != null)
                m += (_Alias, a);
            var rowSet = (RowSet)cx._Add(new TableRowSet(id.iix.dp, cx, defpos, m));
            //#if MANDATORYACCESSCONTROL
            Audit(cx, rowSet);
            //#endif
            return rowSet;
        }
        /// <summary>
        /// Create a new NodeType (or EdgeType) if required.
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="n">The name of the new type</param>
        /// <param name="cse">Use case: U: alter type, V: create type, W: insert graph</param>
        /// <param name="id">IdCol name</param>
        /// <param name="ic">IdCol has Char type</param>
        /// <param name="it">Domain for IdCol</param>
        /// <param name="lt">leavingType (if for EdgeType)</param>
        /// <param name="at">arrivingType ..</param>
        /// <param name="lc">leaveCol name ..</param>
        /// <param name="ar">arriveCol name ..</param>
        /// <param name="sl">leaveCol is a set type ..</param>
        /// <param name="sa">arriveCol is a set type ..</param>
        /// <returns></returns>
        internal virtual NodeType NewNodeType(Context cx, string n, char cse, string id = "ID", bool ic = false,
            NodeType? lt = null, NodeType? at = null, string lc = "LEAVING", string ar = "ARRIVING",
            bool sl = false, bool sa = false)
        {
            NodeType? ot = null;
            NodeType? nt = null;
            var md = CTree<Sqlx, TypedValue>.Empty + (Sqlx.NODETYPE, TNull.Value);
            if (id != "ID") md += (Sqlx.NODE, new TChar(id));
            if (ic) md += (Sqlx.CHAR, new TChar(id));
            // Step 1: How does this fit with what we have? 
            var ob = cx.db.objects[cx.role.dbobjects[n] ?? -1L] as DBObject;
            if (ob == null)
            {
                if (cse == 'U') throw new DBException("42107", n);
                nt = new NodeType(cx.GetUid(), n, NodeType, CTree<Domain, bool>.Empty, cx);
                (nt, _) = nt.Build(cx, null, new CTree<long, bool>(nt.defpos, true), CTree<string, SqlValue>.Empty, md);
            }
            else if (ob is not Level5.NodeType)
            {
                var od = ob as Domain ?? throw new DBException("42104", n);
                nt = (NodeType?)cx.Add(new PMetadata(n, od.Length, od, md, cx.db.nextPos));
            }
            else
                nt = ot = (NodeType)ob;
            // Step 2: Do we have an existing primary key that matches id?
            if (ot != null && cx.db.objects[ot.idCol] is TableColumn tc && tc.infos[cx.role.defpos] is ObInfo ci)
            {
                if (ci.name != id)
                {
                    Level3.Index? ix = null;
                    var xp = -1L;
                    var dm = new Domain(Sqlx.ROW, cx, new BList<DBObject>(tc));
                    for (var b = ot.indexes[dm]?.First(); b != null; b = b.Next())
                        if (cx.db.objects[b.key()] is Level3.Index x &&
                            (x.flags.HasFlag(PIndex.ConstraintType.PrimaryKey) || x.flags.HasFlag(PIndex.ConstraintType.Unique)))
                            ix = x;
                    if (ix == null)
                    {
                        xp = cx.db.nextPos;
                        nt = (NodeType?)cx.Add(new PIndex("", ot, dm, PIndex.ConstraintType.Unique, -1L, cx.db.nextPos));
                    }
                    nt = (NodeType?)cx.Add(new AlterIndex(xp, cx.db.nextPos));
                }
                else
                    nt = ot;
            }
            return nt ?? throw new DBException("42105");
        }
        internal override Table Base(Context cx)
        {
            throw new NotImplementedException(); // Node and Edge type do not have a unique base
        }
        /// <summary>
        /// We have a new node type cs and have been given columns ls
        /// New columns specified are added or inserted.
        /// We will construct Physicals for new columns required
        /// </summary>
        /// <param name="x">The insert graph expression</param>
        /// <param name="ls">The properties from an inline document, or default values</param>
        /// <param name="md">A metadata-like set of associations (a la ParseMetadata)</param>
        /// <returns>The new node type: we promise a new PNodeType for this</returns>
        /// <exception cref="DBException"></exception>
        internal virtual (NodeType, CTree<string, SqlValue>) Build(Context cx, SqlNode? x, CTree<long, bool> dc, CTree<string, SqlValue> ls, CTree<Sqlx, TypedValue> md)
        {
            var ut = this;
            if (defpos < 0)
                return (this, ls);
            if (name is not string tn)
                throw new DBException("42000", "Node name");
            // analyse the label set given
            long? lt = (ut as EdgeType)?.leavingType, at = (ut as EdgeType)?.arrivingType;
            var st = ut.super;
            // The new Type may not yet have a Physical record, so fix that
            if (defpos >= Transaction.Analysing)
            {
                PNodeType pt;
                for (var b = dc.First(); b != null; b = b.Next())
                    if (cx._Ob(b.key()) is UDType ud)
                    {
                        if (ud.infos[cx.role.defpos] is ObInfo u0 && u0.name != tn)
                            ut = (NodeType)ut.New(ut.mem - RowType - Representation + (ObInfo.Name, tn)
                                + (Infos, ut.infos + (cx.role.defpos, u0 + (ObInfo.Name, tn))));
                        else
                            st += (ud, true);
                    }
                if (this is EdgeType et)
                {
                    var pe = new PEdgeType(tn, et, st, -1L, cx.db.nextPos, cx);
                    if (md[Sqlx.RARROW] is TChar lv && cx.role.dbobjects[lv.value] is long lp)
                        lt = pe.leavingType = lp;
                    if (md[Sqlx.ARROW] is TChar av && cx.role.dbobjects[av.value] is long ap)
                        at = pe.arrivingType = ap;
                    pt = pe;
                }
                else
                    pt = new PNodeType(tn, ut, st, -1L, cx.db.nextPos, cx);
                ut = (NodeType)(cx.Add(pt) ?? throw new DBException("42105"));
            }
            // for the metadata tokens used for these identifiers, see the ParseMetadata routine
            var id = ls.Contains("ID")?"ID":(md[Sqlx.NODE] as TChar)?.value ?? (md[Sqlx.EDGE] as TChar)?.value;
            var sl = (md[Sqlx.LPAREN] as TChar)?.value ?? "LEAVING";
            var sa = (md[Sqlx.RPAREN] as TChar)?.value ?? "ARRIVING";
            var le = (md[Sqlx.ARROWBASE] as TBool)?.value ?? false;
            var ae = (md[Sqlx.RARROWBASE] as TBool)?.value ?? false;
            var rt = ut.rowType;
            var rs = ut.representation;
            var sn = BTree<string, long?>.Empty; // properties we are adding
                                                 // check contents of ls
                                                 // ls comes from inline properties in graph create or from ParseRowTypeSpec default value
            var io = cx.obs[infos[cx.role.defpos]?.names["ID"].Item2??-1L]
                ?? ls["ID"] ?? ((id is not null) ? new SqlLiteral(cx.GetUid(), id, TChar.Empty, Char) : null);
            var ii = io?.defpos ?? -1L;
            if (io is not null)
            {
                cx.Add(io);
                (id, rt, rs, sn, ut) = GetColAndIx(cx, ut, ut.FindPrimaryIndex(cx), ii, id,
                    IdIx, IdCol, -1L, false, rt, rs, sn);
            }
            if (ut is EdgeType)
            {
                if (cx.role.dbobjects[(md[Sqlx.RARROW] as TChar)?.value ?? ""] is long pL)
                {
                    var tl = cx.db.objects[lt ?? -1L] as Table ?? throw new DBException("42107", "LEAVING");
                    var rl = tl.FindPrimaryIndex(cx);
                    var lc = rl?.keys?.First()?.value() ?? pL;
                    (sl, rt, rs, sn, ut) = GetColAndIx(cx, ut, rl, lc, sl, EdgeType.LeaveIx,
                        EdgeType.LeaveCol, EdgeType.LeavingType, le, rt, rs, sn);
                    cx.Add(ut);
                }
                if (cx.role.dbobjects[(md[Sqlx.ARROW] as TChar)?.value ?? ""] is long pA)
                {
                    var aa = cx.db.objects[at ?? -1L] as Table ?? throw new DBException("42107", "ARRIVING");
                    var al = aa?.FindPrimaryIndex(cx);
                    var ac = al?.keys?.First()?.value() ?? pA;
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
            var ui = ut?.infos[cx.role.defpos] ?? throw new DBException("42105");
            for (var b = ls.First(); b != null; b = b.Next())
            {
                var n = b.key();
                if (n != id && n != sl && n != sa && ui?.names.Contains(n) != true)
                {
                    var d = cx._Dom(b.value().defpos) ?? Content;
                    var pc = new PColumn3(ut, n, -1, d, "", TNull.Value, "", CTree<UpdateAssignment, bool>.Empty,
                    true, GenerationRule.None, PColumn.GraphFlags.None, -1L, -1L, cx.db.nextPos, cx);
                    ut = (NodeType)(cx.Add(pc) ?? throw new DBException("42105"));
                    rt += pc.ppos;
                    rs += (pc.ppos, d);
                    var cn = new Ident(n, new Iix(pc.ppos));
                    var uds = cx.defs[ut.name]?[cx.sD].Item2 ?? Ident.Idents.Empty;
                    uds += (cn, cx.sD);
                    cx.defs += (ut.name, new Iix(defpos), uds);
                    cx.defs += (cn, cx.sD);
                    sn += (n, pc.ppos);
                }
            }
            cx.Add(ut);
            cx.db += ut;
            var ids = cx.defs[ut.name]?[cx.sD].Item2 ?? Ident.Idents.Empty;
            if (id is not null)
            {
                var i1 = new Ident(id, new Iix(ut.idCol));
                ids += (i1, cx.sD);
            }
            // update defs for inherited properties
            for (var b = ut.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.db.objects[p] is TableColumn uc
                    && uc.infos[uc.definer] is ObInfo ci
                        && ci.name is string sc && p != ut.idCol
                        && !rs.Contains(p))
                {
                    rt += p;
                    rs += (p, ut.representation[p] ?? Domain.Char);
                    var i3 = new Ident(sc, new Iix(p));
                    ids += (i3, cx.sD);
                }
            cx.defs += (tn, new Iix(defpos), ids);
            ut += (RowType, rt);
            ut += (Representation, rs);
            var cns = ut.infos[cx.role.defpos]?.names ?? BTree<string, (int, long?)>.Empty;
            for (var b = ut.super.First(); b != null; b = b.Next())
                if (b.key().infos[cx.role.defpos] is ObInfo si)
                    cns += si.names;
            var ri = BTree<long, int?>.Empty;
            for (var b = rt.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    ri += (p, b.key());
            for (var b = sn.First(); b != null; b = b.Next())
                if (b.value() is long q && ri[q] is int i)
                    cns += (b.key(), (i, q));
            ut += (Infos, new BTree<long, ObInfo>(cx.role.defpos,
                new ObInfo(ut.name, Grant.AllPrivileges)
                + (ObInfo.Names, cns)));
            cx.Add(ut);
            if (ut is EdgeType ee && cx.db.objects[lt ?? -1L] is NodeType ln
                && cx.db.objects[at ?? -1L] is NodeType an)
            {
                ee = ee + (EdgeType.LeavingType, ln.defpos)
                    + (EdgeType.ArrivingType, an.defpos);
                ut = (EdgeType)cx.Add(ee);
            }
            var dl = cx.db.loadpos;
            var ro = cx.role + (Role.DBObjects, cx.role.dbobjects + (ut.name, ut.defpos));
            cx.db = cx.db + (ut, dl) + (ro, dl);
            if (cx.db is Transaction ta && ta.physicals[ut.defpos] is PNodeType pn)
            {
                pn.dataType = ut;
                cx.db = ta + (Transaction.Physicals, ta.physicals + (pn.ppos, pn));
            }
            return (ut, ls);
        }
        internal virtual NodeType Check(Context cx, SqlNode e, CTree<string, SqlValue> ls, bool allowExtras = true)
        {
            if (cx._Ob(defpos) is not NodeType nt || nt.infos[definer] is not ObInfo ni)
                throw new DBException("PE42133", name);
            for (var b = ls.First(); b != null; b = b.Next())
                if (b.key() is string n && !ni.names.Contains(n) && ls[n] is SqlValue v && allowExtras)
                    cx.Add(new PColumn3(this, n, -1, v.domain,
                    PColumn.GraphFlags.None, -1L, -1L, cx.db.nextPos, cx));
            return this;
        }
        internal NodeType FixNodeType(Context cx, Ident typename)
        {
            if (((Transaction)cx.db).physicals[typename.iix.dp] is not PType pt)
                throw new PEException("PE50501");
            if (pt is not PNodeType)
            {
                pt = new PNodeType(typename.ident, pt, this, cx);
                cx.Add(pt);
            }
            FixColumns(cx, 1);
            pt.under = cx.FixTDb(super);
            pt.dataType = this;
            return (NodeType)(cx.Add(pt) ?? throw new DBException("42105"));
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
        /// <param name="kc">The key column</param>
        /// <param name="id">The key column name</param>
        /// <param name="xp">IdIx/LeaveIx/ArriveIx</param>
        /// <param name="cp">IdCol/LeaveCol/ArriveCol</param>
        /// <param name="tp">LeavingType/ArrivingType</param>
        /// <param name="se">Whether the index Domain is to be a Set</param>
        /// <param name="rt">The node type rowType so far</param>
        /// <param name="rs">The representation of the node type so far</param>
        /// <param name="sn">The names of the columns in the node type so far</param>
        /// <returns>The name of the special column (which may have changed),
        /// the modified domain bits and names for ut, and ut with poissible changes to indexes</returns>
        /// <exception cref="DBException"></exception>
        internal (string?, BList<long?>, CTree<long, Domain>, BTree<string, long?>, NodeType)
            GetColAndIx(Context cx, NodeType ut, Level3.Index? rx, long kc, string? id, long xp, long cp, long tp,
                bool se, BList<long?> rt, CTree<long, Domain> rs, BTree<string, long?> sn)
        {
            TableColumn? tc = null; // the specified column: it might be an existing one but will maybe get a new index
            PColumn3? pc = null; // the new column if required
            Table? tr; // referenced node type
            Domain? di; // new index key if required
            PIndex? px = null; // primary index if referenced
            DBObject? so = cx._Ob(kc);
            var sd = (so as TableColumn)?.domain??(so as SqlValue)?.domain??Position;
            // the PColumn, if new, needs to record in the transaction log what is going on here
            // using its fields for flags, toType and index information
            PColumn.GraphFlags gf = cp switch
            {
                IdCol => PColumn.GraphFlags.IdCol,
                EdgeType.LeaveCol => PColumn.GraphFlags.LeaveCol,
                EdgeType.ArriveCol => PColumn.GraphFlags.ArriveCol,
                _ => PColumn.GraphFlags.None
            };
            var ui = ut.infos[cx.role.defpos] ?? throw new DBException("42105");
            var cd = se ? new TSet(sd).dataType : sd;
            if (id is null && cp == IdCol && ui.names["ID"].Item2 is long p 
                && cx._Ob(p) is TableColumn ti)
            {
                id = "ID";
                ut += (IdCol, p);
            }
            if (id is not null)
            {
                if (ui.names.Contains(id) && ui.names[id].Item2 is long pp)
                {
                    pc = (PColumn3?)((Transaction)cx.db).physicals[pp];
                    tc = (TableColumn?)(cx._Ob(pp));
                }
                else
                {
                    pc = new PColumn3(ut, id, -1, cd, gf, rx?.defpos ?? -1L, kc,
                        cx.db.nextPos, cx);
                    // see note above
                    ut = (NodeType)(cx.Add(pc) ?? throw new DBException("42105"));
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
                    tc = (TableColumn)(cx._Ob(pc.ppos) ?? throw new DBException("42105"));
                }
                if (rx is null && (cp == EdgeType.LeaveCol || cp == EdgeType.ArriveCol))
                {
                    tr = cx._Od(tc.toType) as Table ?? throw new DBException("42105");
                    if (pc is not null)
                    {
                        pc.dataType = Position;
                        pc.domdefpos = Position.defpos;
                        pc.toType = tc.toType;
                        var dt = (Transaction)cx.db;
                        dt += (Transaction.Physicals, dt.physicals + (pc.defpos, pc));
                        cx.db = dt;
                        tr += (SysRefIndexes, tr.sindexes + (pc.defpos, CTree<long, CTree<long, bool>>.Empty));
                    }
                    cx.db += tr;
                    cx.Add(tr);
                }
                di = new Domain(-1L, cx, Sqlx.ROW, new BList<DBObject>(tc), 1);
                px = new PIndex(id, ut, di, 
                    (cp==IdCol)?PIndex.ConstraintType.PrimaryKey
                    :(PIndex.ConstraintType.ForeignKey | PIndex.ConstraintType.CascadeUpdate),
                    rx?.defpos ?? -1L, cx.db.nextPos);
                tc += (Level3.Index.RefIndex, px.ppos);
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
            if (px is not null && tc is not null)
            {
                ut = (NodeType)(cx.Add(px) ?? throw new DBException("42105"));
                if (xp != -1L)
                    ut = ut + (cp, tc.defpos) + (xp, px.ppos);
            }
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
                                                var te = new TEdge(rt, tr);
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
                                                var te = new TEdge(rt, tr);
                                                AddType(ref types, rt);
                                                edges += (te, (int)ntable.Count);
                                                AddType(ref types, at);
                                                ntable += new NodeInfo(types[rt], te.id, x, y, nodes[tn],
                                                    (int)ntable.Count + 1, te.Summary(cx));
                                                var ta = new TNode(cx, at, ar);
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
                                                var te = new TEdge(rt, tr);
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
                                                var te = new TEdge(rt, tr);
                                                edges += (te, (int)ntable.Count);
                                                AddType(ref types, rt);
                                                ntable += new NodeInfo(types[rt], te.id, x, y, (int)ntable.Count + 1, nodes[tn],
                                                    te.Summary(cx));
                                                var tl = new TNode(cx, lt, lr);
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
                throw new DBException("42105");
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
                    if ((keys.rowType.Last()?.value() ?? -1L) == tc.defpos && dt.kind == Sqlx.INTEGER)
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
                        if (tb is not UDType && !(rt.metadata.Contains(Sqlx.ENTITY) || tb is NodeType))
                            continue;
                        var rn = ToCamel(rt.name);
                        for (var i = 0; fields.Contains(rn); i++)
                            rn = ToCamel(rt.name) + i;
                        var fn = cx.NameFor(rx.keys[0] ?? -1L);
                        fields += (rn, true);
                        sb.Append("  public " + rt.name + "? " + sc.ToString()
                            + "is => conn?.FindOne<" + rt.name + ">((\"" + fn.ToString() + "\"," + sa.ToString() + "));\r\n");
                    }
            for (var b = rindexes.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is Table tb && tb.infos[ro.defpos] is ObInfo rt && rt.name != null)
                {
                    if (tb is UDType || rt.metadata.Contains(Sqlx.ENTITY))
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
                                            (ts is UDType || ti.metadata.Contains(Sqlx.ENTITY)) &&
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
                || kind == Sqlx.Null || from.kind == Sqlx.Null)
                throw new DBException("42105");
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
                    if ((keys.rowType.Last()?.value() ?? -1L) == tc.defpos && dt.kind == Sqlx.INTEGER)
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
                        if (tb is not UDType && !(rt.metadata.Contains(Sqlx.ENTITY) || tb is NodeType))
                            continue;
                        var rn = ToCamel(rt.name);
                        for (var i = 0; fields.Contains(rn); i++)
                            rn = ToCamel(rt.name) + i;
                        var fn = cx.NameFor(rx.keys[0] ?? -1L);
                        fields += (rn, true);
                        sb.Append(" def " + sc.ToString() + "is(): \r\n");
                        sb.Append("  return conn.FindOne(" + rt.name + ",\"" + fn.ToString() + "\"=" + sa.ToString() + ")\r\n");
                    }
            for (var b = rindexes.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is Table tb && tb.infos[ro.defpos] is ObInfo rt && rt.name != null)
                {
                    if (tb is UDType || rt.metadata.Contains(Sqlx.ENTITY))
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
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (idIx != -1L) sb.Append(" IdIx=" + Uid(idIx));
            if (idCol != -1L) sb.Append(" IdCol=" + Uid(idCol));
            return sb.ToString();
        }
    }
    /// <summary>
    /// structural information about leaving and arriving is copied to subtypes
    /// </summary>
    internal class EdgeType : NodeType
    {
        internal const long
            ArriveCol = -474, // long TableColumn  NameFor is md[RPAREN]
            ArriveColDomain = -495, // Domain
            ArriveIx = -435, // long Index
            ArrivingEnds = -371, // bool
            ArrivingType = -467, // long NodeType  NameFor is md[ARROW]
            LeaveCol = -473, // long TableColumn   NameFor is md[LPAREN]
            LeaveColDomain = -494, // Domain
            LeaveIx = -470, // long Index
            LeavingEnds = -377, // bool
            LeavingType = -464; // long NodeType   NameFor is md[RARROW]
        public bool leavingEnds => (bool)(mem[LeavingEnds] ?? false);
        public Domain leaveColDomain => (Domain)(mem[LeaveColDomain] ?? Int);
        public bool arrivingEnds => (bool)(mem[ArrivingEnds] ?? false);
        public Domain arriveColDomain => (Domain)(mem[ArriveColDomain] ?? Int);
        internal EdgeType(long dp, string nm, UDType dt, CTree<Domain,bool> un, Context cx, 
            CTree<Sqlx, TypedValue>? md = null,long? lt=null,long? at=null)
            : base(dp, _Mem(nm, dt, un, md, lt, at, cx))
        {
            var ro = cx.role;
            var ra = cx.db.edgeTypes[dt.defpos] ?? BTree<long, BTree<long, long?>>.Empty;
            var rb = ra[leavingType] ?? BTree<long, long?>.Empty;
            rb += (arrivingType, dp);
            ra += (leavingType, rb);
            ro += (Role.DBObjects, ro.dbobjects+(nm, dp));
            cx.db = cx.db + (ro,cx.db.loadpos) + (Database.EdgeTypes, cx.db.edgeTypes + (dt.defpos, ra));
        }
        internal EdgeType(Sqlx t) : base(t)
        { }
        public EdgeType(long dp, BTree<long, object> m) : base(dp, m)
        { }
        internal EdgeType(long dp, BTree<long, object> m, PType pt)
            : this(dp, m + (LeavingType, ((PEdgeType)pt).leavingType) + (ArrivingType, ((PEdgeType)pt).arrivingType))
        { }
        static BTree<long, object> _Mem(string nm, UDType dt, CTree<Domain,bool> ut, 
            CTree<Sqlx, TypedValue>? md, long? lt, long? at, Context cx)
        {
            var r = dt.mem + (Kind, Sqlx.EDGETYPE);
            r += (ObInfo.Name, nm);
            var oi = dt.infos[cx.role.defpos] ?? new ObInfo(nm, Grant.AllPrivileges);
            oi += (ObInfo.Name, nm);
            r += (Infos, dt.infos + (cx.role.defpos, oi));
            r += (Definer, cx.role.defpos);
            if (ut != null)
                r += (Under, ut);
            var sl = false;
            var sa = false;
            if (md != null)
            {
                TChar? ln = null, an = null;
                if (md[Sqlx.RARROWBASE] is TChar aa) { an = aa; sa = true; }
                if (md[Sqlx.ARROW] is TChar ab) an = ab;
                if (md[Sqlx.ARROWBASE] is TChar la) { ln = la; sl = true; }
                if (md[Sqlx.RARROW] is TChar lb) ln = lb;
                r += (LeavingType, lt??cx.role.dbobjects[ln?.value??"?"]??Domain.NodeType.defpos);
                r += (ArrivingType, at??cx.role.dbobjects[an?.value??"?"] ?? Domain.NodeType.defpos);
                if (sl) r += (LeavingEnds, true);
                if (sa) r += (ArrivingEnds, true);
            }
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new EdgeType(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new EdgeType(dp, m + (Kind, Sqlx.EDGETYPE));
        }
        internal override UDType New(Ident pn, CTree<Domain,bool> un, long dp, Context cx)
        {
            var nd = (EdgeType)EdgeType.Relocate(dp);
            if (nd.defpos!=dp)
                nd.Fix(cx);
            return (UDType)(cx.Add(new PEdgeType(pn.ident, nd, un, -1L, dp, cx))
                ?? throw new DBException("42105"));
        } 
        internal override NodeType Check(Context cx, SqlNode n, CTree<string, SqlValue> ls, bool allowExtras = true)
        {
            var et = base.Check(cx, n, ls, allowExtras);
            var e = (SqlEdge)n;
            if (cx.obs[e.leavingValue] is not SqlNode nl || cx.obs[e.arrivingValue] is not SqlNode na)
                throw new PEException("PE60904");
            if (cx._Ob(et.leavingType) is UDType el && cx._Ob(et.arrivingType) is UDType ea)
            {
                if (!HasLabel(cx,nl.label,et.leavingType) || !HasLabel(cx,na.label,et.arrivingType))
                {
                    if (!allowExtras)
                        throw new DBException("42000","&");
                    var md = CTree<Sqlx, TypedValue>.Empty + (Sqlx.ARROW, new TChar(na.domain.name))
                        + (Sqlx.RARROW, new TChar(nl.domain.name));
                    var ne = new EdgeType(cx.GetUid(), name, Domain.EdgeType, CTree<Domain, bool>.Empty, cx,
                        md);
                    (et,ls) = ne.Build(cx, n, CTree<long, bool>.Empty, ls, md);
                }
                if (el.infos[cx.role.defpos] is ObInfo li
                    && li.name is string en && ls[en] is SqlValue sl && sl.Eval(cx) is TChar lv
                    && cx.db.objects[NodeTypeFor(cx, lv)] is NodeType lt
                    && !lt.EqualOrStrongSubtypeOf(el))
                {
                    var pp = cx.db.nextPos;
                    var pi = new Ident(pp.ToString(), new Iix(pp));
                    var xt = (Domain)cx.Add(et + (LeavingType,
                        new NodeType(cx.GetUid(), pi.ident, TypeSpec, el.super, cx).defpos));
                    cx.Add(xt);
                }
                if (ea.infos[cx.role.defpos] is ObInfo ai
                    && ai.name is string an && ls[an] is SqlValue sa && sa.Eval(cx) is TChar av
                    && cx.db.objects[NodeTypeFor(cx, av)] is NodeType at
                    && !at.EqualOrStrongSubtypeOf(ea))
                {
                    var pp = cx.db.nextPos;
                    var pi = new Ident(pp.ToString(), new Iix(pp));
                    var xt = (Domain)cx.Add(et + (ArrivingType,
                        new NodeType(cx.GetUid(), pi.ident, TypeSpec, ea.super, cx).defpos));
                    cx.Add(xt);
                }
            }
            return et;
        }
        bool HasLabel(Context cx,CTree<long,bool> tl,long p)
        {
            if (tl.Contains(p))
                return true;
            for (var b = tl.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue sl && sl.Eval(cx) is TypedValue tv)
                {
                    if (tv is TTypeSpec ts && ts._dataType.defpos == p)
                        return true;
                    if (tv is TChar tc 
                        && (cx.role.nodeTypes[tc.ToString()] == p))
            //            || (cx.obs[p] is NodeType et && et.infos[cx.role.defpos]?.name==tc.ToString())))
                        return true;
                }
            return false;
        }
        internal EdgeType FixEdgeType(Context cx, PType pt)
        {
            FixColumns(cx, 1);
            pt.under = cx.FixTDb(super);
            pt.dataType = this;
            return (EdgeType)(cx.Add(pt) ?? throw new DBException("42105"));
        }
        static long NodeTypeFor(Context cx, TypedValue? v)
        {
            if (v is TTypeSpec ts && ts._dataType is NodeType nt)
                return nt.defpos;
            if (v is TChar s)
                return cx.role.dbobjects[s.value] ?? -1L;
            return -1L;
        }
        internal override DBObject Add(Context cx, PMetadata pm, long p)
        {
            var ro = cx.role;
            if (pm.detail.Contains(Sqlx.EDGETYPE) && infos[ro.defpos] is ObInfo oi
                && oi.name is not null)
            {
                ro += (Role.EdgeTypes, ro.edgeTypes + (oi.name, defpos));
                cx.db += (ro, p);
            }
            return base.Add(cx, pm, p);
        }
        /// <summary>
        /// Create a new EdgeType if required.
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="n">The name of the new type (will contain & if label.Count!=1L)</param>
        /// <param name="cse">Use case: U: alter type, V: create type, W: insert graph</param>
        /// <param name="id">IdCol name</param>
        /// <param name="ic">IdCol has Char type</param>
        /// <param name="it">Domain for IdCol</param>
        /// <param name="lt">leavingType (if for EdgeType)</param>
        /// <param name="at">arrivingType ..</param>
        /// <param name="lc">leaveCol name ..</param>
        /// <param name="ar">arriveCol name ..</param>
        /// <param name="sl">leaveCol is a set type ..</param>
        /// <param name="sa">arriveCol is a set type ..</param>
        /// <returns></returns>
        internal override NodeType NewNodeType(Context cx, string n, char cse, string id = "ID", bool ic = false,
            NodeType? lt = null, NodeType? at = null, string lc = "LEAVING", string ar = "ARRIVING",
            bool sl = false, bool sa = false)
        {
            EdgeType? ot = null;
            EdgeType? et;
            var md = CTree<Sqlx, TypedValue>.Empty + (Sqlx.EDGETYPE, TNull.Value);
            if (id != "ID") md += (Sqlx.NODE, new TChar(id));
            if (ic) md += (Sqlx.CHAR, new TChar(id));
            if (lt is not null) md += (Sqlx.ARROW, new TChar(cx.NameFor(lt.defpos)));
            if (at is not null) md += (Sqlx.RARROW, new TChar(cx.NameFor(at.defpos)));
            if (lc != "LEAVING") md += (Sqlx.LPAREN, new TChar(lc));
            if (ar != "ARRIVING") md += (Sqlx.RPAREN, new TChar(ar));
            if (sl) md += (Sqlx.ARROWBASE, TBool.True);
            if (sa) md += (Sqlx.RARROWBASE, TBool.True);
            // Step 1: How does this fit with what we have? 
            var ob = cx.db.objects[cx.role.dbobjects[n] ?? -1L] as DBObject;
            if (ob == null)
            {
                if (cse == 'U') throw new DBException("42107", n);
                et = new EdgeType(cx.GetUid(), n, EdgeType, CTree<Domain,bool>.Empty, cx, md);
                var (e1, _) = et.Build(cx, null, new CTree<long,bool>(et.defpos,true), CTree<string, SqlValue>.Empty, md);
                et = (EdgeType)e1;
            }
            else if (ob is not Level5.NodeType)
            {
                var od = ob as Domain ?? throw new DBException("42104", n);
                et = (EdgeType?)cx.Add(new PMetadata(n, od.Length, od, md, cx.db.nextPos));
            }
            else
                et = ot = (EdgeType)ob;
            // Step 2: Do we have an existing primary key that matches id?
            if (ot != null && cx.db.objects[ot.idCol] is TableColumn tc && tc.infos[cx.role.defpos] is ObInfo ci)
            {
                if (ci.name != id)
                {
                    Level3.Index? ix = null;
                    var xp = -1L;
                    var dm = new Domain(Sqlx.ROW, cx, new BList<DBObject>(tc));
                    for (var b = ot.indexes[dm]?.First(); b != null; b = b.Next())
                        if (cx.db.objects[b.key()] is Level3.Index x &&
                            (x.flags.HasFlag(PIndex.ConstraintType.PrimaryKey) || x.flags.HasFlag(PIndex.ConstraintType.Unique)))
                            ix = x;
                    if (ix == null)
                    {
                        xp = cx.db.nextPos;
                        et = (EdgeType?)cx.Add(new PIndex("", ot, dm, PIndex.ConstraintType.Unique, -1L, cx.db.nextPos));
                    }
                    et = (EdgeType?)cx.Add(new AlterIndex(xp, cx.db.nextPos));
                }
                else
                    et = ot;
                // Step 3: Sort out leaving and arriving keys for et
                var ol = cx.db.objects[ot.leaveCol] as TableColumn ?? throw new PEException("PE50603");
                var olt = cx.db.objects[ot.leavingType] as NodeType ?? throw new PEException("PE50604");
                var oa = cx.db.objects[ot.arriveCol] as TableColumn ?? throw new PEException("PE50605");
                var oat = cx.db.objects[ot.arrivingType] as NodeType ?? throw new PEException("PE50606");
                if (cx.NameFor(ol.defpos) != lc || olt != lt || cx.NameFor(oa.defpos) != ar || oat != at)
                {
                    if (ot.super.Count == 0L)
                    {
                        // make a supertype NodeType
                        var sm = CTree<Sqlx, TypedValue>.Empty + (Sqlx.ID, new TChar(cx.NameFor(ot.idCol)))
                            + (Sqlx.CHAR, TBool.For(tc.domain.kind == Sqlx.CHAR));
                        var (ut, _) = ot.Build(cx, null, new CTree<long, bool>(NodeType.defpos, true),
                            CTree<string, SqlValue>.Empty, sm);
                        ot = (EdgeType?)cx.Add(new EditType(n, ot, ot, new CTree<Domain, bool>(ut, true), cx.db.nextPos, cx))
                            ?? throw new DBException("42105");
                        var (e2, _) = ot.Build(cx, null, new CTree<long, bool>(ut.defpos, true), CTree<string, SqlValue>.Empty, md);
                        et = (EdgeType)e2;
                    }
                }
            }
            return et ?? throw new DBException("42105");
        }
        internal override Table _PathDomain(Context cx)
        {
            var rt = rowType;
            var rs = representation;
            var ii = infos;
            var gi = rs.Contains(idCol); 
            for (var tb = super.First(); tb != null; tb = tb.Next())
                if (cx._Ob(tb.key().defpos) is Table pd)
                {
                    for (var b = infos.First(); b != null; b = b.Next())
                        if (b.value() is ObInfo ti)
                        {
                            if (pd.infos[cx.role.defpos] is ObInfo si)
                                ti += si;
                            else throw new DBException("42105");
                            ii += (b.key(), ti);
                        }
                    if (pd is NodeType pn && (!gi) && (!rs.Contains(pn.idCol)))
                    {
                        gi = true;
                        rt += pn.idCol;
                        rs += (pn.idCol, pn.idColDomain);
                    }
                    if (pd is EdgeType pe)
                    {
                        if (!rs.Contains(pe.leaveCol))
                        {
                            rt += pe.leaveCol;
                            rs += (pe.leaveCol, pe.leaveColDomain);
                        }
                        if (!rs.Contains(pe.arriveCol))
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
            return base.Inherit(to);
        }
        internal override Basis Fix(Context cx)
        {
            var r = New(cx.Fix(defpos), _Fix(cx, mem));
            var ro = cx.role;
            var e = cx.db.edgeTypes[defpos] ?? BTree<long, BTree<long, long?>>.Empty;
            var f = e[leavingType] ?? BTree<long, long?>.Empty;
            f += (arrivingType, r.defpos);
            e += (leavingType, f);
            cx.db += (Database.EdgeTypes, cx.db.edgeTypes + (defpos, e));
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
        public static EdgeType operator +(EdgeType et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (dp == LeavingType && et.leavingType>=0 && ob is long && (long)ob<0 )
                return et;
            if (dp == ArrivingType && et.arrivingType>=0 && ob is long && (long)ob<0)
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
            if (a.dataType.kind==Sqlx.ARRAY)
                return ((TArray)a).CompareTo((TArray)b);
            // if we get to here they both have this as dataType
            return ((TEdge)a).CompareTo((TEdge)b);
        }
        internal override DBObject Relocate(long dp, Context cx)
        {
            var r = (EdgeType)base.Relocate(dp, cx)
                + (LeavingType, cx.Fix(leavingType))
                + (ArrivingType, cx.Fix(arrivingType));
            return (EdgeType)cx.Add(r);
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
    /// We don't commit this type to the database: it merely helps organise the creation of Record4 physical records
    /// </summary>
    internal class JoinedNodeType : NodeType
    {
        internal Sqlx op => (Sqlx)(mem[SqlValueExpr.Op] ?? Sqlx.AMPERSAND);
        internal CTree<Domain, bool> nodeTypeFactors =>
            (CTree<Domain, bool>)(mem[Under] ?? CTree<Domain, bool>.Empty);
        internal JoinedNodeType(long dp, string nm, UDType dt, CTree<Domain, bool> ut, Context cx) 
            : base(dp, nm, dt, ut, cx)
        {
        }
        public JoinedNodeType(long dp, BTree<long, object> m) : base(dp, m)
        {
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            return base._Replace(cx, so, sv);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return base.New(dp, m);
        }
        internal override (NodeType, CTree<string, SqlValue>) Build(Context cx, SqlNode? x,
            CTree<long, bool> dc, CTree<string, SqlValue> ls, CTree<Sqlx, TypedValue> md)
        {
            var ids = CTree<long, TypedValue>.Empty;
            TypedValue id = TNull.Value;
            var it = Null;
            // examine any provided id values
            for (var b = nodeTypeFactors.First(); b != null; b = b.Next())
                if (b.key() is NodeType nt && cx._Ob(nt.idCol) is TableColumn tc
                        && tc.infos[cx.role.defpos] is ObInfo ci && ci.name is string cn
                        && ls[cn] is SqlValue sv)
                {
                    var tv = sv.Eval(cx);
                    for (var c = ids.First(); c != null; c = c.Next())
                        if (c.value() is TypedValue cv && cv.CompareTo(tv) != 0)
                            throw new DBException("42000", "Conflicting id values");
                    if (it.defpos == Null.defpos)
                        it = tv.dataType;
                    else if (it.CompareTo(tv.dataType) != 0)
                        throw new DBException("42000", "Conflicting id types");
                    id = tv;
                }
            var fl = CTree<long, TypedValue>.Empty;
            var rs = CTree<long, bool>.Empty;
            var tbs = CTree<long, bool>.Empty;            
            var lp = -1L;
            var ct = (int)nodeTypeFactors.Count; 
            for (var b = nodeTypeFactors.First(); b != null; b = b.Next())
                if (b.key() is NodeType nt && x is not null)
                {
                    var np = cx.GetUid();
                    var nd = new SqlNode(new Ident(Uid(np), new Iix(np)), CList<Ident>.Empty, cx,
                        -1L, new CTree<long, bool>(nt.defpos, true), x.docValue, x.state, nt);
                    nd.Create(cx, nt, false);
                    // locate the Record that has just been constructed in nt
                    tbs += (nt.defpos, true);
                    var tr = (Transaction)cx.db; // must be inside this loop
                    for (var c=tr.physicals.Last();c!=null;c=c.Previous())
                        if (c.value() is Record rc && rc.tabledefpos.Contains(nt.defpos))
                        {
                            fl += rc.fields;
                            rs += (rc.ppos, true);
                            if (--ct == 0)
                                lp = rc.ppos;
                            else
                            {
                                tr += (Transaction.Physicals, tr.physicals - rc.ppos);
                                cx.db = tr;
                            }
                            break;
                        }    
                }
            var nr = new Record4(tbs, fl, -1L, Level.D, lp, cx);
            cx.Add(nr);// Transaction.Add takes care not to Install to the factors again!
            return (this, ls);
        }
        static TypedValue Inc(TypedValue v)
        {
            if (v is TChar tc)
                return new TChar((int.Parse(tc.value) + 1).ToString());
            return new TInt((v.ToInt() ?? 0) + 1);
        }
        internal override Table _PathDomain(Context cx)
        {
            return base._PathDomain(cx);
        }
        public override string ToString()
        {
            return base.ToString();
        } 
    }
    /// <summary>
    /// Despite its name, a GraphType is not a Type or Domain
    /// See GQL 4.13: it is a set of node types and edge types that are defined as constraints in a Graph
    /// </summary>
    internal class GraphType : DBObject
    {
        internal BTree<string, long?> nodeTypes => 
            (BTree<string, long?>)(mem[Role.NodeTypes] ?? BTree<string, long?>.Empty);
        internal BTree<string, long?> edgeTypes =>
            (BTree<string, long?>)(mem[Role.EdgeTypes] ?? BTree<string, long?>.Empty);
        public GraphType(long dp, BTree<long, object> m) : base(dp, m)
        {
        }

        public GraphType(long pp, long dp, BTree<long, object>? m = null) : base(pp, dp, m)
        {
        }

        internal override DBObject New(long dp, BTree<long, object> m)
        {
            throw new NotImplementedException();
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
            GraphTypes = -86, // CTree<long,bool> GraphType
            Iri = 147, // string
            Nodes = -499; // CTree<long,TNode> // and edges
        internal CTree<long,TNode> nodes =>
            (CTree<long, TNode>) (mem[Nodes]??CTree<long,TNode>.Empty);
        internal CTree<long,bool> graphTypes => 
            (CTree<long,bool>)(mem[GraphTypes] ?? CTree<long, bool>.Empty);
        internal string iri => (string)(mem[Iri]??"");
        internal Graph(long dp,CTree<long, TNode> ns, string ir = "", CTree<long,bool>? ts = null)
            : base(dp,BTree<long,object>.Empty+(Nodes,ns)+(Iri,ir)+(GraphTypes,ts??CTree<long,bool>.Empty))
        { }

        public Graph(long dp, BTree<long, object> m) : base(dp, m)
        { }

        public static Graph operator+(Graph g,TNode r)
        {
            return new Graph(g.defpos,g.nodes + (r.tableRow.defpos,r));
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
            sb.Append(']');
            return sb.ToString();
        }

        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new Graph(dp,m);
        }
    }
    internal class TNode : TypedValue
    {
        public readonly TableRow tableRow;
        public long defpos => tableRow.defpos;
        public TypedValue id => tableRow.vals[(dataType as NodeType)?.idCol??-1L]??new TInt(defpos);
        internal TNode(Context cx, NodeType nt, TableRow tr)
            : this((NodeType)(cx.db.objects[nt.defpos] ?? Domain.NodeType), tr)
        { }
        internal TNode(NodeType nt, TableRow tr) :base(nt)
        {
            tableRow = tr;
        }
        internal override TypedValue this[long p] => tableRow.vals[p]??TNull.Value;
        internal virtual bool CheckProps(Context cx,TNode n)
        {
            return dataType.defpos == n.dataType.defpos && id == n.id;
        }
        internal override string ToString(Context cx)
        {
            if (cx.db.objects[dataType.defpos] is not NodeType nt ||
                nt.infos[cx.role.defpos] is not ObInfo ni)
                return "??";
            var sb = new StringBuilder();
            sb.Append(ni.name);
            var cm = '(';
            for (var b=nt.pathDomain.First();b!=null;b=b.Next())
            if (b.value() is long cp){
                sb.Append(cm); cm = ',';
                sb.Append((cx.db.objects[cp] as TableColumn)?
                           .infos[cx.role.defpos]?.name??"??");
                    sb.Append("=");
                    sb.Append(tableRow.vals[cp]);
            }
            sb.Append(')');
            return sb.ToString();
        }
        internal string[] Summary(Context cx)
        {
            if (cx.db.objects[dataType.defpos] is not NodeType nt ||
                nt.infos[cx.role.defpos] is not ObInfo ni)
                return Array.Empty<string>();
            var ss = new string[Math.Max(nt.pathDomain.Length,5)+1];
            ss[0] = ni.name ?? "";
            for (var b = nt.pathDomain.First(); b != null && b.key() < 5; b = b.Next())
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
            var that = obj as TNode;
            if (that == null) return 1;
            return tableRow.defpos.CompareTo(that.tableRow.defpos);
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
        public long leavingType => (dataType as EdgeType)?.leavingType??-1L;
        public long arrivingType => (dataType as EdgeType)?.arrivingType ?? -1L;
        internal TEdge(Context cx, NodeType nt, TableRow tr)
    : this((EdgeType)(cx.db.objects[nt.defpos] ?? Domain.EdgeType), tr)
        { }
        internal TEdge(EdgeType et, TableRow tr) : base(et, tr)
        { }
        public override string ToString()
        {
            return "TEdge " + DBObject.Uid(defpos) + "[" + DBObject.Uid(dataType.defpos) + "]";
        }
    }
    /// <summary>
    /// A class for an unbound identifier (A variable in Francis's paper)
    /// </summary>
    internal class TGParam : TypedValue
    {
        [Flags]
        internal enum Type { None=0,Node=1,Edge=2,Path=4,Group=8,Maybe=16,Type=32,Field=64,Value=128 };
        internal readonly long uid;
        internal readonly long from;
        internal readonly Type type; // in reverse Polish order
        internal readonly string value;
        public TGParam(long dp,string i,Domain d,Type t,long f) : base(d)
        {
            uid = dp;
            type = t;
            value = i;
            from = f;
        }
        public override int CompareTo(object? obj)
        {
            return (obj is TGParam tp && tp.uid == uid) ? 0 : -1;
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            if (uid > 0)
                sb.Append(DBObject.Uid(uid));
            else
                sb.Append((Sqlx)(int)-uid);
            sb.Append(' ');
            sb.Append(value);
            return sb.ToString();
        }
    }
}
