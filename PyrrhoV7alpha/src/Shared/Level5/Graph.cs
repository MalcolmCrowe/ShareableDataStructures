using Pyrrho.Level3;
using Pyrrho.Common;
using Pyrrho.Level4;
using System.Text;
using Pyrrho.Level2;
using Pyrrho.Level5;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

//The RDBMS view of graph data

// A NodeType (or EdgeType) corresponds a single database object that defines
// both a base Table in the database and a user-defined type for its rows. 

// The UDType is managed by the database engine by default
// but the usual ALTER operations are available for both Table and UDT.
// It has at least one INTEGER column (a database uid) which is a key managed by the system.
// If no other primary key is specified, the uid column will be used as a default node identity and called ID.
// Other columns are provided for any properties that are defined.
// The UDT for a Node type also a set of possible LeavingTypes and ArrivingTypes
// for edges, and the UDT for an Edge type specifies the LeavingType and ArrivingType for nodes.
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

// The set of arrivingNode edges is a generated property, so SqlInsert cannot insert a Node that
// contains arrivingNode edges, or an Edge that refers to heap uids for nodes or edges.
// A statement requiring the construction of a graph fragmenet is automatically converted into a
// sequence of database changes (insert and type operation) to avoids such problems.

// Creating graph data in the RDBMS

// A Neo4j-like syntax can be used to add one or more nodes and zero or more edges.
// using the CREATE Node syntax defined in section 7.2 of the manual:
// Create: CREATE Node {Edge Node} {',' Node { Edge Node }}.
// Node: '(' id[':' label [':' label]] [doc] ')'.
// Edge: '-['[id] ':' label [':' label] [doc] ']->' | '<-['[id] ':' label [':' label] [doc] ']-'.

// Alternatively, a JSON object can define an explicit graph fragment value
// with the help of some auxiliary items. As a minimum such an object should contain
// an item of form "$Type":id to indicate the Node type (a similar item can be used to indicate
// an edge type), with a further optional subtype indication "$Under":id if the type is new.
// Edges can be specified using subdocuments similarly specifying an edge type
// and a target node together with a direction item "$LeavingNode" or "ArrivingNode" as appropriate.

// Both of these methods allow the values of additional properties of nodes and edges using
// JSON object syntax. In all cases, the occurrence of unexpected items will result in 
// the inline creation or modification of nodes, edges, node types and/or edge types,
// using new DBObject uids as required, and on commit will alter the set of graphs defined
// in the database, which will contain a Record for each node and edge that includes a
// uid-valued array for the set of leavingNode edges of a node and uid-values properties for 
// the leavingNode uid and arrivingNode uid of an edge.

// As suggested in the DBKDA paper, an interactive SQL session can provide feedback on any changes
// to the database schema as node values are added, in case these are surprising.
// In a batch session, such changes can be allowed (like CASCADE) or disallowed (like RESTRICT).
// Extra fields will be added to the connection string to enable this behaviour,
// so that the default behaviour for the PyrrhoCmd client will be interactive.

// structural information about ID is copied to subtypes
internal class NodeType : UDType
{
    internal const long
        IdCol = -472,  // long TableColumn
        IdIx = -436;   // long Index
    internal NodeType(Sqlx t) : base(t)
    { }
    public NodeType(long dp, BTree<long, object> m) : base(dp, m)
    { }
    internal NodeType(long dp, string nm, UDType dt, Table? ut, Context cx)
        : base(dp, _Mem(nm, dt, ut, cx))
    { }
    static BTree<long, object> _Mem(string nm, UDType dt, Table? ut, Context cx)
    {
        var r = dt.mem + (Kind, Sqlx.NODETYPE);
        r += (ObInfo.Name, nm);
        var oi = dt.infos[cx.role.defpos] ?? new ObInfo(nm, Grant.AllPrivileges);
        oi += (ObInfo.Name, nm);
        r += (Infos, dt.infos + (cx.role.defpos, oi));
        r += (Definer, cx.role.defpos);
        if (ut != null)
            r += (Under, ut);
        return r;
    }
    internal TableRow? Get(Context cx,TInt? id)
    {
        if (id is null)
            return null;
        var px = FindPrimaryIndex(cx);
        return tableRows[px?.rows?.impl?[id]?.ToLong() ?? -1L];
    }
    internal override Basis New(BTree<long, object> m)
    {
        return new NodeType(defpos, m);
    }
    internal override DBObject New(long dp, BTree<long, object> m)
    {
        return new NodeType(dp, m + (Kind, Sqlx.NODETYPE));
    }
    internal override UDType New(Ident pn, Domain? un, long dp, Context cx)
    {
        return (UDType)(cx.Add(new PNodeType(pn.ident, (NodeType)NodeType.Relocate(dp), un, -1L, dp, cx))
            ?? throw new DBException("42105"));
    }
    internal override Table _PathDomain(Context cx)
    {
        if (super is not UDType su)
            return this;
        var rt = BList<long?>.Empty;
        var rs = CTree<long,Domain>.Empty;
        rt += idCol;
        rs += (idCol, Char);
        for (var b = su.pathDomain.rowType.First(); b != null; b = b.Next())
            if (b.value() is long p && su.pathDomain.representation[p] is Domain cd && !rs.Contains(p))
            {
                rt += p;
                rs += (p, cd);
            }
        for (var b = rowType.First(); b != null; b = b.Next())
            if (b.value() is long p && representation[p] is Domain cd )
            {
                rt += p;
                rs += (p, cd);
            }
        return new Table(-1L, cx, rs, rt, rt.Length);
    }
    internal override UDType Inherit(UDType to)
    {
        if (idCol >= 0)
            to += (IdCol, idCol);
        return to;
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
    public static NodeType operator +(NodeType et, (Context, long, object) x)
    {
        var d = et.depth;
        var m = et.mem;
        var (cx, dp, ob) = x;
        if (et.mem[dp] == ob)
            return et;
        if (ob is long p && cx.obs[p] is DBObject bb)
        {
            d = Math.Max(bb.depth + 1, d);
            if (d > et.depth)
                m += (_Depth, d);
        }
        return (NodeType)et.New(m + (dp, ob));
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
    internal override Table Base(Context cx)
    {
        return (Table)(cx.db.objects[super?.defpos??-1L]??base.Base(cx));
    }
    /// <summary>
    /// We have a new node type cs and have been given columns ls
    /// New columns specified are added or inserted.
    /// We will construct Physicals for new columns required,
    /// </summary>
    /// <param name="ut">The NodeType being built: must be NodeType or EdgeType already but may be empty so far</param>
    /// <param name="ls">The properties from an inline document, or default values</param>
    /// <param name="md">A metadata-like set of assoiactions (a la ParseMetadata)</param>
    /// <returns>The new node type: we promise a new PNodeType for this</returns>
    /// <exception cref="DBException"></exception>
    internal (NodeType,CTree<string,SqlValue>) Build(Context cx, NodeType ut, CTree<string, SqlValue> ls, CTree<Sqlx, TypedValue> md)
    {
        if (defpos < 0)
            return (this, ls);
        if (name is not string tn)
            throw new DBException("42000");
        long? lt = (ut as EdgeType)?.leavingType, at = (ut as EdgeType)?.arrivingType;
        // The new Type may not yet have a Physical record, so fix that
        if (defpos < -1L || defpos>=Transaction.Analysing)
        {
            PNodeType pt;
            var st = super as NodeType;
            if (ut.infos[cx.role.defpos] is ObInfo u0 && u0.name != tn)
            {
                st = ut;
                ut = (NodeType)ut.New(ut.mem - RowType - Representation + (ObInfo.Name,tn)
                    + (Infos, ut.infos+(cx.role.defpos,u0 + (ObInfo.Name, tn))));
            }
            if (ut is EdgeType et)
            {
                var pe = new PEdgeType(tn, et, st, -1L, cx.db.nextPos, cx);
                if (md[Sqlx.RARROW] is TChar lv && cx.role.dbobjects[lv.value] is long lp)
                    lt = pe.leavingType = lp;
                else
                    throw new DBException("42107", md[Sqlx.RARROW]?.ToString() ?? "LEAVING");
                if (md[Sqlx.ARROW] is TChar av && cx.role.dbobjects[av.value] is long ap)
                    at = pe.arrivingType = ap;
        //        else
        //            throw new DBException("42107", md[Sqlx.ARROW]?.ToString() ?? "ARRIVING");
                pt = pe;
            }
            else
                pt = new PNodeType(tn, ut, st, -1L, cx.db.nextPos, cx);
            ut = (NodeType)(cx.Add(pt) ?? throw new DBException("42105"));
        }
        // for the metadata tokens used for these identifiers, see the ParseMetadata routine
        var id = (md[Sqlx.NODE] as TChar)?.value ?? (md[Sqlx.EDGE] as TChar)?.value ?? "ID";
        var sl = (md[Sqlx.LPAREN] as TChar)?.value ?? "LEAVING";
        var sa = (md[Sqlx.RPAREN] as TChar)?.value ?? "ARRIVING";
        var le = (md[Sqlx.ARROWBASE] as TBool)?.value ?? false;
        var ae = (md[Sqlx.RARROWBASE] as TBool)?.value ?? false; 
        var rt = ut.rowType;
        var rs = ut.representation;
        var sn = BTree<string, long?>.Empty; // properties we are adding
                                             // check contents of ls
                                             // ls comes from inline properties in graph create or from ParseRowTypeSpec default values
        var si = ut.super?.infos[cx.role.defpos];
        var ii = new SqlLiteral(cx.GetUid(), id, TChar.Empty, Char);
        (id, rt, rs, sn, ut) = GetColAndIx(cx, ut, ut.FindPrimaryIndex(cx), ii.defpos, id,
            IdIx, IdCol, -1L, false, rt, rs, sn);
        if (ut is EdgeType)
        {
            if (cx.role.dbobjects[(md[Sqlx.RARROW] as TChar)?.value??""] is long pL 
                && (cx.db.objects[lt ?? -1L] as Table)?.FindPrimaryIndex(cx) is Pyrrho.Level3.Index rl)
            (sl, rt, rs, sn, ut) = GetColAndIx(cx, ut, rl, pL, sl, EdgeType.LeaveIx,
                EdgeType.LeaveCol, EdgeType.LeavingType, le, rt, rs, sn);
            if (cx.role.dbobjects[(md[Sqlx.ARROW] as TChar)?.value ?? ""] is long pA
                && (cx.db.objects[at ?? -1L] as Table)?.FindPrimaryIndex(cx) is Pyrrho.Level3.Index ra)
            (sa, rt, rs, sn, ut) = GetColAndIx(cx, ut, ra, pA, sa, EdgeType.ArriveIx,
                EdgeType.ArriveCol, EdgeType.ArrivingType, ae, rt, rs, sn);
        }
        var ui = ut.infos[cx.role.defpos] ?? throw new DBException("42105");
        for (var b = ls.First(); b != null; b = b.Next())
        {
            var n = b.key();
            if (n != id && n != sl && n != sa && ui?.names.Contains(n) != true)
            {
                var d = cx._Dom(b.value().defpos) ?? Content;
                var pc = new PColumn3(ut, n, -1, d, "", TNull.Value, "", CTree<UpdateAssignment, bool>.Empty,
                true, GenerationRule.None, cx.db.nextPos, cx);
                ut = (NodeType)(cx.Add(pc) ?? throw new DBException("42105"));
                rt += pc.ppos;
                rs += (pc.ppos, d);
                cx.defs += (new Ident(n, new Iix(pc.ppos)), cx.sD);
                sn += (n, pc.ppos);
            }
        }
        cx.Add(ut);
        cx.db += ut;
        var ids = cx.defs[ut.name]?[cx.sD].Item2 ?? Ident.Idents.Empty;
        var i1 = new Ident(id, new Iix(ut.idCol));
        ids += (i1, cx.sD);
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
            ee = ee + (EdgeType.LeavingType, LeastSpecific(ln))
                + (EdgeType.ArrivingType, LeastSpecific(an));
            ut = (EdgeType)cx.Add(ee);
        }
        var dl = cx.db.loadpos;
        var ro = cx.role + (Role.DBObjects, cx.role.dbobjects + (ut.name, ut.defpos));
        cx.db = cx.db + (ut, dl) + (ro, dl);
        return (ut,ls);
    }
    internal virtual NodeType Check(Context cx,CTree<string, SqlValue> ls)
    {
        if (infos[definer] is not ObInfo ni)
            throw new DBException("PE42133", name);
        for (var b = ls?.First(); b != null; b = b.Next())
            if (b.key() is string n && !ni.names.Contains(n))
                throw new DBException("42112", n);
        return this;
    }
    static long LeastSpecific(NodeType nt)
    {
        while (nt.super is NodeType st)
            nt = st;
        return nt.defpos;
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
        FixColumns(cx,1);
        pt.under = super;
        pt.dataType = this;
        return (NodeType)(cx.Add(pt) ?? throw new DBException("42105"));
    }
    internal void FixColumns(Context cx,int off)
    {
        for (var b = ((Transaction)cx.db).physicals.First(); b != null; b = b.Next())
            if (b.value() is PColumn pc && pc.table?.defpos == defpos)
            {
                pc.seq += off;
                pc.table = this;
            }
    }
    /// <summary>
    /// This is where we sort out the special columns for a new NodeType or EdgeType
    /// The semantics for the IdCol case are different from the others: if cp==IdCol then
    ///   rx if not null is the primary key for ut and provides the identity column
    ///   (and the given identity name must match it)
    /// For the leaveCol and ArriveCol cases rx is the primary key of the leaveType/arriveType
    ///   and must not be null
    /// </summary>
    /// <param name="ut">The node type/edge type</param>
    /// <param name="rx">The primary index or referenced index if already defined</param>
    /// <param name="kc">The key column</param>
    /// <param name="id">The key column name</param>
    /// <param name="xp">IdIx/LeaveIx/ArriveIx</param>
    /// <param name="cp">IdCol/LeaveCol/ArriveCol</param>
    /// <param name="se">Whether the index Domain is to be a Set</param>
    /// <param name="rt">The node type rowType so far</param>
    /// <param name="rs">The representation of the node type so far</param>
    /// <param name="sn">The names of the columns in the node type so far</param>
    /// <returns>The name of the special column (which may have changed),
    /// the modified domain bits and names for ut, and ut with poissible changes to indexes</returns>
    /// <exception cref="DBException"></exception>
    (string, BList<long?>, CTree<long, Domain>, BTree<string, long?>, NodeType)
        GetColAndIx(Context cx, NodeType ut, Pyrrho.Level3.Index? rx, long kc, string id, long xp, long cp, long tp,
            bool se, BList<long?> rt, CTree<long, Domain> rs, BTree<string, long?> sn)
    {
        TableColumn? tc;
        // the PColumn needs to record in the transaction log what is going on here
        // using its fields for flags, toType and index information
        PColumn.GraphFlags gf = cp switch
        {
            IdCol => PColumn.GraphFlags.IdCol,
            EdgeType.LeaveCol => PColumn.GraphFlags.LeaveCol,
            EdgeType.ArriveCol => PColumn.GraphFlags.ArriveCol,
            _ => PColumn.GraphFlags.None
        };
        PColumn3? pc = null;
        var ui = infos[cx.role.defpos]??throw new DBException("42105");
        var si = super?.infos[cx.role.defpos];
        var cd = se ? new TSet(Int).dataType : Int;
        long? pp = null;
        if (si?.names.Contains(id) == true)
            pp = si.names[id].Item2;
        if (pp == null && ui.names.Contains(id))
            pp = ui.names[id].Item2;
        if (pp == null)
        {
            pc = new PColumn3(ut, id, ut.Seq(gf), cd, cx.db.nextPos, cx)
            {
                flags = gf
            };
            // see note above
            ut = (NodeType)(cx.Add(pc) ?? throw new DBException("42105"));
            ut += (cp, pc.ppos);
            sn += (id, pc.ppos);
            rt = Remove(rt, kc);
            rs -= kc;
            var ot = rt;
            rt = new BList<long?>(pc.ppos);
            for (var b = ot.First(); b != null; b = b.Next())
                if (b.value() is long op)
                    rt += op;
            rs += (pc.ppos, cd);
            tc = (TableColumn)(cx._Ob(pc.ppos) ?? throw new DBException("42105"));
        }
        else if (cx.db.objects[pp ?? -1L] is TableColumn sc)// coming from supertype
            tc = sc;
        else
        {
            pc = (PColumn3)(((Transaction)cx.db).physicals[pp ?? -1L] ?? throw new DBException("42105"));
            tc = (TableColumn)(cx._Ob(ui.names[id].Item2 ?? -1L) ?? throw new DBException("42105"));
        }
        // add a new index in all cases
        var di = new Domain(-1L, cx, Sqlx.ROW, new BList<DBObject>(tc), 1);
        var px = new PIndex(id, ut, di, (cp == NodeType.IdCol) ? PIndex.ConstraintType.PrimaryKey
            : (PIndex.ConstraintType.ForeignKey | PIndex.ConstraintType.CascadeUpdate),
            (cp == IdCol) ? -1L : rx?.defpos ?? -1L, cx.db.nextPos);
        tc += (Pyrrho.Level3.Index.RefIndex, px.ppos);
        if (pc != null)
        {
            pc.flags = gf;
            pc.toType = rx?.reftabledefpos ?? -1L; // -1L for idCol case
            pc.index = rx?.refindexdefpos ?? -1L; // ditto
        }
        tc += (TableColumn.GraphFlag, gf);
        if (rx != null)
            tc = tc + (Pyrrho.Level3.Index.RefTable, rx.tabledefpos) + (Pyrrho.Level3.Index.RefIndex, rx.defpos);
        cx.Add(tc);
        cx.db += (tc.defpos, tc);
        ut = (NodeType)(cx.Add(px) ?? throw new DBException("42105"));
        ut = ut + (cp, tc.defpos) + (xp, px.ppos);
        if (rx != null)
            ut += (tp, px.tabledefpos);
        if (ut is EdgeType)
            ut += (tp, tc.toType);
        return (id, rt, rs, sn, ut);
    }
    static BList<long?> Remove(BList<long?> rt, long v)
    {
        var r = BList<long?>.Empty;
        for (var b = rt.First(); b != null; b = b.Next())
            if (b.value() is long p && p != v)
                r += p;
        return r;
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
        ArriveIx = -435, // long Index
        ArrivingEnds = -371, // bool
        ArrivingType = -467, // long NodeType  NameFor is md[ARROW]
        LeaveCol = -473, // long TableColumn   NameFor is md[LPAREN]
        LeaveIx = -470, // long Index
        LeavingEnds = -377, // bool
        LeavingType = -464; // long NodeType   NameFor is md[RARROW]
    public bool leavingEnds => (bool)(mem[LeavingEnds] ?? false);
    public bool arrivingEnds => (bool)(mem[ArrivingEnds] ?? false);
    internal EdgeType(long dp,string nm, UDType dt, Table? ut, Context cx, CTree<Sqlx, TypedValue>? md = null)
        : base(dp, _Mem(nm, dt, ut, md, cx))
    { }
    internal EdgeType(Sqlx t) : base(t)
    { }
    public EdgeType(long dp, BTree<long, object> m) : base(dp, m)
    { }
    internal EdgeType(long dp, BTree<long, object> m, PType pt)
        : this(dp,m+(LeavingType,((PEdgeType)pt).leavingType)+(ArrivingType,((PEdgeType)pt).arrivingType))
    { }
    static BTree<long, object> _Mem(string nm, UDType dt, Table? ut, CTree<Sqlx, TypedValue>? md, Context cx)
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
            if (ln == null || an == null)
                throw new DBException("42000");
            r += (LeavingType, cx.role.dbobjects[ln.value] ?? throw new DBException("42107", ln));
            r += (ArrivingType, cx.role.dbobjects[an.value] ?? throw new DBException("42107", an));
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
    internal override UDType New(Ident pn, Domain? un, long dp, Context cx)
    {
        return (UDType)(cx.Add(new PEdgeType(pn.ident, (EdgeType)EdgeType.Relocate(dp), un, -1L, dp, cx))
            ?? throw new DBException("42105"));
    }
    internal override NodeType Check(Context cx, CTree<string, SqlValue> ls)
    {
        var et = base.Check(cx,ls);
        if (cx._Ob(et.leavingType) is UDType el && el.infos[cx.role.defpos] is ObInfo li
            && li.name is string en && ls[en] is SqlValue sl && sl.Eval(cx) is TChar lv
            && cx.db.objects[NodeTypeFor(cx, lv)] is NodeType lt
            && !lt.EqualOrStrongSubtypeOf(el))
        {
            var pp = cx.db.nextPos;
            var pi = new Ident(pp.ToString(), new Iix(pp));
            var xt = (Domain)cx.Add(et + (LeavingType,
                new NodeType(cx.GetUid(), pi.ident, TypeSpec, el, cx).defpos));
            cx.Add(xt);
        }
        if (cx._Ob(et.arrivingType) is UDType ea && ea.infos[cx.role.defpos] is ObInfo ai
            && ai.name is string an && ls[an] is SqlValue sa && sa.Eval(cx) is TChar av
            && cx.db.objects[NodeTypeFor(cx, av)] is NodeType at
            && !at.EqualOrStrongSubtypeOf(ea))
        {
            var pp = cx.db.nextPos;
            var pi = new Ident(pp.ToString(), new Iix(pp));
            var xt = (Domain)cx.Add(et + (ArrivingType,
                new NodeType(cx.GetUid(),pi.ident, TypeSpec, ea, cx).defpos));
            cx.Add(xt);
        }
        return et;
    }
    long NodeTypeFor(Context cx, TypedValue? v)
    {
        if (v is TTypeSpec ts && ts._dataType is NodeType nt)
            return nt.defpos;
        if (v is TChar s)
            return cx.role.dbobjects[s.value] ?? -1L;
        return -1L;
    }
    internal EdgeType FixEdgeType(Context cx, Ident typename)
    {
        if(((Transaction)cx.db).physicals[typename.iix.dp] is not PType pt)
            throw new PEException("PE50502");
        if (pt is not PEdgeType)
            pt = new PEdgeType(typename.ident, pt, this, cx);
        pt.dataType = this;
        FixColumns(cx,3);
        pt.under = super;
        return (EdgeType)(cx.Add(pt)??throw new DBException("42105"));
    }
    internal override Table _PathDomain(Context cx)
    {
        if (super is not UDType su)
            return this;
        var rt = BList<long?>.Empty;
        var rs = CTree<long,Domain>.Empty;
        rt += idCol;
        rs += (idCol, Char);
        rt += leaveCol;
        rs += (leaveCol, leavingEnds ? new TSet(Char).dataType : Char);
        rt += arriveCol;
        rs += (arriveCol, arrivingEnds ? new TSet(Char).dataType : Char);
        for (var b = su.pathDomain.rowType.First(); b != null; b = b.Next())
            if (b.value() is long p
                && su.pathDomain.representation[p] is Domain cd && !rs.Contains(p))
            {
                rt += p;
                rs += (p, cd);
            }
        for (var b = rowType.First(); b != null; b = b.Next())
            if (b.value() is long p
                && representation[p] is Domain cd && !rs.Contains(p))
            {
                rt += p;
                rs += (p, cd);
            }
        return new Table(-1L, cx, rs, rt, rt.Length);
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
    internal override BList<long?> Add(BList<long?> a, int k, long v, long p)
    {
        if (p == leaveCol)
            k = 1;
        if (p == arriveCol)
            k = 2;
        return base.Add(a, k, v, p);
    }
    internal override Basis Fix(Context cx)
    {
        var r = New(cx.Fix(defpos), _Fix(cx, mem));
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
        if (ob is DBObject bb && dp != _Depth)
        {
            d = Math.Max(bb.depth + 1, d);
            if (d > et.depth)
                m += (_Depth, d);
        }
        return (EdgeType)et.New(m + x);
    }
    public static EdgeType operator +(EdgeType et, (Context, long, object) x)
    {
        var d = et.depth;
        var m = et.mem;
        var (cx, dp, ob) = x;
        if (et.mem[dp] == ob)
            return et;
        if (ob is long p && et.representation[p] is DBObject bb)
        {
            d = Math.Max(bb.depth + 1, d);
            if (d > et.depth)
                m += (_Depth, d);
        }
        return (EdgeType)et.New(m + (dp, ob));
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

// The Graph view of graph data

// The database is considered to contain a (possibly empty) set of TGraphs.
// Every Node in the database belongs to exactly one graph in this set. 

// The nodes of a graph are totally ordered by the order of insertion in the database
// but this is not the traversal ordering: the first node in a graph is the first in both orderings.
// The traversal ordering starts with this first node but preferentially follows edges
// (first the leavingNode edges ordered by their edge types and edge uids
// followed by arrivingNode edges ordered similarly)
// while not visiting any node or edge more than once.

// The set of graphs is totally ordered by the uids of their first node. 

// Show the data management language, an SqlNode is an SqlRow whose domain is a Node type.
// It may have an ad hoc (heap) uid. Evaluation of the SqlRow part gives a set of properties
// and edges of the node. Evaluation of the SqlNode gives a rowset of TGraph values.
// The datatype of TGraph is a primitive data type Graph that allows assignment of a JSON value as described above.

// A TGraph value may match a subgraph of one of the graphs in the database, in which case
// we say the TGraph is found in the database.

// An SqlMatchExpr is an SqlValue containing FormalParameters of form beginning with _ or ? .
// The result of a match is to give a number of alternative values for these identifiers,
// as a RowSet whose columns are the set of parameters in their uid order,
// and whose values are TypedValues, such that by assigning these values to the parameters,
// the SqlMatchExpr evaluates to a TGraph that is found in the database.

namespace Pyrrho.Level5
{
    /// <summary>
    /// A TGraph is a TypedValue containing a tree of TNodes.
    /// It always begins with the Node with the lowest uid (its representative), 
    /// </summary>
    internal class TGraph : TypedValue
    {
        internal readonly BTree<long,TNode> nodes; // and edges
        internal TGraph(BTree<long, TNode> ns) : base(Domain.Graph)
        {
            nodes = ns;
        }
        internal TGraph(TNode n) : base(Domain.Graph) 
        {
            nodes = new BTree<long,TNode>(n.tableRow.defpos,n);
        }
        public static TGraph operator+(TGraph g,TNode r)
        {
            return new TGraph(g.nodes + (r.tableRow.defpos,r));
        }
        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException();
        }
        internal TNode? Rep()
        {
            return nodes.First()?.value();
        }
        public override int CompareTo(object? obj)
        {
            if (obj is not TGraph tg)
                return 1;
            var tb = tg.nodes.First();
            var b = nodes.First();
            for (; b != null && tb != null; b = b.Next(), tb = tb.Next())
            {
                var c = b.key().CompareTo(tb.key());
                if (c!=0) return c;
            }
            if (b != null) return 1;
            if (tb is not null) return -1;
            return 0;
        }
        internal static CTree<TGraph, bool> Add(Context cx,CTree<TGraph, bool> t, TNode r)
        {
            if (Find(t, r) is not null)
                return t;
            if (r is not TEdge e || e.dataType is not EdgeType et)
                return t + (new TGraph(r), true);
            // Edge: end nodes already must be in t, but may be in different TGraphs
            if (cx.db.objects[et.leavingType] is not NodeType lt || r[et.leaveCol] is not TInt li 
                || et.tableRows[li.value] is not TableRow ln
                || Find(t, new TNode(lt,ln)) is not TGraph lg)
                return t;
            if (cx.db.objects[et.arrivingType] is not NodeType at || r[at.arriveCol] is not TInt ai
                || et.tableRows[ai.value] is not TableRow an
                || Find(t, new TNode(at, an)) is not TGraph ag)
                return t;
            if (lg.Rep() is not TNode lr|| ag.Rep() is not TNode ar)
                return t;
            if (lr.tableRow.defpos == ar.tableRow.defpos) // already connected: add n to one of them
                return t - lg + (lg + r, true);
            else // merge the graphs and add n
                return t - ag -lg + (new TGraph(lg.nodes + ag.nodes + (r.tableRow.defpos, r)), true);
        }
        static TGraph? Find(CTree<TGraph, bool> t, TNode? n)
        {
            if (n is null)
                return null;
            for (var b = t.First(); b != null; b = b.Next())
                if (b.key().nodes.Contains(n.tableRow.defpos))
                    return b.key();
            return null;
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
    }
    internal class TNode : TypedValue
    {
        public readonly TableRow tableRow;
        public long defpos => tableRow.defpos;
        public long id => (tableRow.vals[(dataType as NodeType)?.idCol??-1L]?.ToLong())??-1L;
        internal TNode(NodeType nt, TableRow tr) :base(nt)
        {
            tableRow = tr;
        }
        internal override TypedValue this[long p] => tableRow.vals[p]??TNull.Value;
        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException();
        }
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
        public long leaving => (tableRow.vals[(dataType as EdgeType)?.leaveCol ?? -1L]?.ToLong()) ?? -1L;
        public long arriving => (tableRow.vals[(dataType as EdgeType)?.arriveCol ?? -1L]?.ToLong()) ?? -1L;
        public long leavingType => (dataType as EdgeType)?.leavingType??-1L;
        public long arrivingType => (dataType as EdgeType)?.arrivingType ?? -1L;
        internal TEdge(EdgeType et, TableRow tr) : base(et, tr)
        { }
        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException();
        }
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
        internal enum Type { None=0,Node=1,Edge=2,Path=4,Group=8,Maybe=16 };
        internal readonly long uid;
        internal readonly Type type; // in reverse Polish order
        internal readonly string value;
        public TGParam(long dp,string i,Domain d,Type t) : base(d)
        {
            uid = dp;
            type = t;
            value = i;
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

        internal override TypedValue New(Domain t)
        {
            throw new NotImplementedException();
        }
    }
}
