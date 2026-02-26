using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System.Data.SqlTypes;
using System.Reflection.Metadata;
using System.Text;

namespace Pyrrho.Level5
{
    // Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
    // (nc) Malcolm Crowe, University of the West of Scotland 2004-2026
    //
    // This software is without support and no liability for damage consequential to use.
    // You can view and test this code
    // You may incorporate any part of this code in other software if its origin 
    // and authorship is suitably acknowledged.

       // A NodeType (or EdgeType) corresponds a single database object that defines
       // both a base Table in the database and a user-defined type for its rows. 
       // However, the RDBMS view of graph data in this version is Role-based, so that NodeTypes and subclasses are
       // ElementTypes instead of UDTypes. Thus a base UDT table ut can be the base for different NodeTypes
       // in different roles

       // The Table or UDType is managed by the database engine by default
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
           internal Domain idColDomain => (Domain)(mem[IdColDomain] ?? Ref);
       //    internal Domain label =>       (Domain)(mem[GqlNode._Label] ?? GqlLabel.Empty);
           internal long idCol => (long)(mem[IdCol] ?? -1L);
           internal long idIx => (long)(mem[IdIx] ?? -1L);
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
               r += (Under, dt);
               var rt = dt.rowType;
               var rs = dt.representation;
               var ns = dt.names;
               oi += (ObInfo._Names, ns);
               var md = (TMetadata)(m?[ObInfo._Metadata] 
                   ?? (TMetadata.Empty+(Qlx.NODETYPE, new TChar("NODETYPE"))));
               oi += (ObInfo._Metadata, md);
               r += (_Domain, new Domain(cx, rs, rt, dt.display, new BTree<long, ObInfo>(cx.role.defpos, oi)));
               return r;
           }
        /*       internal TableRow? Get(Context cx, TypedValue? id)
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
                       if (cx._Ob(t.key().defpos) is NodeType nt && nt.Get(cx, id) is TableRow tr)
                           return tr;
                   return null;
               } */
            internal override CTree<Domain, bool> _NodeTypes(Context cx)
               {
                   return new CTree<Domain, bool>(this, true);
               }
            internal override Table AddNodeOrEdgeType(Context cx)
            {
                var ro = cx.role;
                var r = this;
                var oi = infos[cx.role.defpos] ?? new ObInfo(name);
                oi += (Method.TypeDef, this);
                r += (Infos, infos + (cx.role.defpos, oi)); 
                var nm = NameFor(cx); // (label.kind == Qlx.NO) ? name : label.name;
                if (nm != "")
                {
                    ro += (Role.NodeTypes, ro.nodeTypes + (nm, defpos));
                    ro += (Role.DBObjects, ro.dbobjects + (nm, defpos));
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
                }
                cx.db += r;
                cx.db += ro;
                cx.db += (Database.Role, ro);
                return r;
            }
           internal override Domain? HaveNodeOrEdgeType(Context cx)
           {
               if (name != "")
                   if (cx.role.nodeTypes[name] is long p && p < Transaction.Analysing)
                       return this;
               var pn = CTree<string, bool>.Empty;
               for (var b = representation.First(); b != null; b = b.Next())
                   if (cx.NameFor(b.key()) is string n)
                       return this;
               return null;
           }
           internal override TNode Node(Context cx, TableRow r)
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
               return new NodeType(dp, m);
           }
           internal override UDType New(Ident pn, CTree<Domain, bool> un, long dp, Context cx)
           {
               return (UDType)(cx.Add(new PNodeType(pn.ident, (NodeType)Relocate(dp, cx),
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
           //                rt += pn.idCol;
                           rs += (pn.idCol, cx._Ob(pn.idCol)?.domain??Ref);
                       }
                       for (var b = pd?.rowType.First(); b != null; b = b.Next())
                           if (b.value() is long p && pd?.representation[p] is Domain cd && !rs.Contains(p))
                           {
                               rt += ((int)rt.Count,p);
                               rs += (p, cd);
                           }
                       for (var b = rowType.First(); b != null; b = b.Next())
                           if (b.value() is long p && representation[p] is Domain cd && !rs.Contains(p))
                           {
                               rt += ((int)rt.Count,p);
                               rs += (p, cd);
                           }
                   }
               return new Table(cx, rs, rt, display, ii);
           }
           internal override (Context,DBObject) Add(Context cx, TMetadata md)
           {
               var ro = cx.role;
               (cx,var r) = base.Add(cx, md);
               if (md.Contains(Qlx.NODETYPE) && r.infos[ro.defpos] is ObInfo oi
                   && oi.name is not null)
               {
                   ro += (Role.NodeTypes, ro.nodeTypes + (oi.name, defpos));
                   cx.db += ro;
               }
               return (cx,r);
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
               var th = (NodeType)(cx._Ob(defpos)??throw new PEException("PE50001"));
               ds ??= BTree<long, TableRow>.Empty;
 /*              for (var b = cx.db.joinedNodes.First(); b != null; b = b.Next())
                   if (b.value().Contains(this) && th.tableRows[b.key()] is TableRow tr)
                       ds += (cx.GetUid(),new TableRow(tr.defpos, tr.ppos, defpos, tr.vals)); */
               if (defpos < 0)
               {
                   if (kind == Qlx.NODETYPE) // We are Domain.NODETYPE itself: do this for all nodetypes in the role
                   {
                       for (var b = cx.db.role.nodeTypes.First(); b != null; b = b.Next())
                           if (b.value() is long p1 && cx._Ob(p1) is NodeType nt1 
                            && nt1.kind==kind)
                               ds = nt1.For(cx, ms, xn, ds);
                       for (var b = cx.db.unlabelledNodeTypes.First(); b != null; b = b.Next())
                           if (b.value() is long p2 && p2>=0 
                            && cx._Ob(p2) is NodeType nt2)
                               ds = nt2.For(cx, ms, xn, ds);
                   }
                   if (kind == Qlx.EDGETYPE) // We are Domain.EDGETYPE itself: do this for all edgetypes in the role
                   {
                       for (var b = cx.db.role.edgeTypes.First(); b != null; b = b.Next())
                           if (b.value() is long p1 && cx._Ob(p1) is Domain ed)
                           {
                               if (ed is EdgeType nt1)
                                   ds = nt1.For(cx, ms, xn, ds);
                               else if (ed.kind == Qlx.UNION)
                                   for (var c = ed.alts.First(); c != null; c = c.Next())
                                       if (cx._Ob(c.key().defpos) is EdgeType ef)
                                           ds = ef.For(cx, ms, xn, ds);
                           }
                   }
                   return ds;
               }
               if (!ms.flags.HasFlag(MatchStatement.Flags.Schema))
               {
                   var cl = xn.EvalProps(cx, th);
                   if (th.FindPrimaryIndex(cx) is Level3.Index px
                       && px.MakeKey(cl) is CList<TypedValue> pk)
                       return (tableRows[px.rows?.Get(pk, 0) ?? -1L] is TableRow tr0)?
                           ds + (tr0.defpos, tr0):ds;
                   for (var c = indexes.First(); c != null; c = c.Next())
                       for (var d = c.value().First(); d != null; d = d.Next())
                           if (cx._Ob(d.key()) is Level3.Index x
                               && x.MakeKey(cl) is CList<TypedValue> xk)
                               return (th.tableRows[x.rows?.Get(xk, 0) ?? -1L] is TableRow tr)?
                                   ds + (tr.defpos, tr) : ds;
                   // let DbNode check any given properties match
                   var lm = ms.truncating.Contains(defpos) ? ms.truncating[defpos].Item1 : int.MaxValue;
                   var la = ms.truncating.Contains(TypeSpec.defpos) ? ms.truncating[TypeSpec.defpos].Item1 : int.MaxValue;
                   for (var b = th.tableRows.First(); b != null && lm-- > 0 && la-- > 0; b = b.Next())
                       if (b.value() is TableRow tr)
                           ds += (tr.defpos, tr);
                   if (defpos < 0)
                   {
                       // as a last resort try all node types
                       Console.WriteLine("Unconstrained node search");
                       for (var b = cx.role.nodeTypes.First(); b != null; b = b.Next())
                           if (cx._Ob(b.value()) is NodeType un)
                               for (var c = un.tableRows.First(); c != null; c = c.Next())
                                   if (c.value() is TableRow ur)
                                       ds += (ur.defpos, ur);
                   }
               } else  // rowType flag
                   ds += (defpos, th.Schema(cx));
               return ds;
           }
        /*          /// <summary>
                  /// Construct a fake TableRow for a nodetype rowType
                  /// </summary>
                  /// <param name="cx"></param>
                  /// <returns></returns>
                  internal TableRow Schema(Context cx)
                  {
                      var vals = CTree<long, TypedValue>.Empty;
                      for (var b = rowType.First(); b != null; b = b.Next())
                          if (cx._Ob(b.value()) is TableColumn tc)
                              vals += (tc.defpos, new TTypeSpec(tc.domain));
                      return new TableRow(defpos, -1L, defpos, vals);
                  } */
        /*         public override Domain For()
                 {
                     return NodeType;
                 } */
        /*         internal NodeType Specific(Context cx,TableRow tr)
                 {
                     for (var b = subtypes.First(); b != null; b = b.Next())
                         if ((cx._Ob(b.key()) is NodeType t 
                          && t.tableRows.Contains(tr.defpos))
                             return t.Specific(cx, tr);
                     return this;
                 } */
        /*         internal override int Typecode()
                 {
                     return 3;
                 } */
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
                       if (b.value() is QlValue sv && !nt.AllCols(cx).Contains(b.key()))
                       {
                           var pc = new PColumn3(nt, b.key(), sv.domain, "", TMetadata.Empty, 
                               cx.db.nextStmt, cx.db.nextPos, cx, true);
                           nt = (NodeType)(cx.Add(pc)??throw new DBException("42105"));
                       }
               }
               nt = (NodeType)(cx._Ob(nt.defpos) ?? nt);
               return new CTree<Domain,bool>(nt,true);
           }
  /*         internal override Domain MakeUnder(Context cx,DBObject so)
           {
               return (so is NodeType sn) ? ((NodeType)New(defpos, mem + (Under, super + (sn, true)))) : this;
           } */
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
               if (infos[cx.role.defpos] is ObInfo oi)
               {
                   oi += (Method.TypeDef, this);
                   pt.dataType = this + (Infos, infos + (cx.role.defpos, oi));
               }
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
 /*       /// <summary>
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
        internal static (BList<UDType.NodeInfo>, CTree<UDType, int>) NodeTable(Context cx, TNode start)
        {
            var types = new CTree<UDType, int>((UDType)start.dataType, 0);
            var ntable = new BList<UDType.NodeInfo>(new NodeInfo(0, new TChar(start.defpos.ToString()), 0F, 0F, -1L, -1L, start.Summary(cx)));
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
        static void AddType(ref CTree<UDType, int> ts, UDType t)
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
        static Random ran = new(0);
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
        } */
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
           internal GqlLabel(Ident id, Context cx,Domain? lt = null, Domain? at = null, BTree<long,object>? m = null)
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
                   var ln = lo.names ?? Names.Empty;
                   var rn = ro.names ?? Names.Empty;
                   var rt = ro.rowType;
                   for (var b = ln.First(); b != null; b = b.Next())
                       if (ln[b.key()].Item2 is long lp)
                       {
                           var ld = lo.representation[lp];
                           var rd = ro.representation[rn[b.key()].Item2];
                           if (ld is not null && rd is not null && ld.CompareTo(rd) != 0)
                               throw new DBException("22G12", ln);
                           if (!ro.representation.Contains(lp))
                               rt += ((int)rt.Count,lp);
                       }
                   m += (RowType,rt);
                   m += (Representation, rs);
                   m += (ObInfo._Names, ln + rn);
               }
               return m + (QlValue.Left, lf) + (QlValue.Right, rg);
           }
            static BTree<long, object> _Mem(Context cx, string id, long ap, long? lt, long? at, BTree<long, object>? m)
            {
                m ??= BTree<long, object>.Empty;
                m += (ObInfo.Name, id);
                Domain dt = NodeType;
                var da = ((Qlx)(m[Kind] ?? Qlx.NO)) == Qlx.DOUBLEARROW;
                if (!m.Contains(Kind))
                    m += (Kind, (lt is null || at is null) ? Qlx.NODETYPE : Qlx.EDGETYPE);
                else if (da && cx._Ob(lt ?? -1L) is NodeType un)
                {
                    m += (Under, new CTree<Domain, bool>(un, true));
                    dt = un;
                }
                else if (((Qlx)(m[Kind] ?? Qlx.NO)) == Qlx.EDGETYPE)
                    dt = EdgeType;
                Domain? sd = null;
                if (lt is null || at is null) sd = cx.FindTable(id);
                if (sd is EdgeType se)
                    sd = null;
                if (sd?.kind == Qlx.UNION)
                    for (var c = sd.alts.First(); sd is null && c != null; c = c.Next())
                        if (cx._Ob(c.key().defpos) is EdgeType sf)
                            sd = sf;
                sd ??= cx.FindTable(id)??dt;
                if (sd is not null)
                {
                    m = m + (_Domain, sd) + (Kind, sd.kind);
                    cx.AddDefs(ap, sd);
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
                           var xs = xn.For(cx, ms, xn, null);
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
            var tm = TMetadata.Empty;
               switch(k)
               {
                   case Qlx.AMPERSAND:
                   case Qlx.COLON:
                       return lf.OnInsert(cx, ap, lm) + rg.OnInsert(cx, ap, rm);
                   case Qlx.DOUBLEARROW:
                       rg.OnInsert(cx, ap, rm);
                       return lf.OnInsert(cx, ap, lm + (Under, new CTree<Domain, bool>(rg, true)));
                case Qlx.NODETYPE:
                        r += (cx.FindTable(name)?.Build(cx, null, ap, "", dc) ?? TypeFor(name, m, cx), true);
                    return r;
                case Qlx.EDGETYPE:
                    return r + (cx.FindTable(name)?.Build(cx, null, ap, "", dc)
                        ?? TypeFor(name, m, cx), true);
                case Qlx.NO:
                    if (cx.FindTable(name) is Table d)
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
           static Table TypeFor(string nm, BTree<long,object> m, Context cx)
           {
               var un = (CTree<Domain, bool>)(m[Under] ?? CTree<Domain, bool>.Empty);
               var nu = CTree<Domain, bool>.Empty;
               for (var b = un.First(); b != null; b = b.Next())
                   nu += ((b.key() is GqlLabel gl) ? cx.FindTable(gl.name ?? "")
                    ?? throw new DBException("42107", gl.name ?? "??") : b.key(), true);
               var dc = (CTree<string, QlValue>)(m[GqlNode.DocValue]??CTree<string,QlValue>.Empty);
               var nt = cx.FindTable(nm);
               if (nt is null || nt.defpos<0)
               {
                   if (cx.parsingGQL.HasFlag(Context.ParsingGQL.Match))
                       return NodeType;
                   var pt = new PNodeType(nm, NodeType, nu, -1L, cx.db.nextPos, cx);
                   nt = (NodeType)(cx.Add(pt) ?? throw new DBException("42105"));
                   for (var b = dc.First(); b != null; b = b.Next())
                       if (!nt.AllCols(cx).Contains(b.key()) && b.value() is QlValue q)
                       {
                        var pc = new PColumn3(nt, b.key(), q.domain,
                            "", (TMetadata)(m[ObInfo._Metadata]??TMetadata.Empty), 
                            cx.db.nextStmt,cx.db.nextPos, cx, true);
                           nt = (NodeType)(cx.Add(pc)??throw new DBException("42105"));
                       }
                   nt = nt.Build(cx, null, 0L, nm, dc);
               }
               return nt ?? throw new DBException("42105");
           }
 /*          internal static EdgeType EdgeTypeFor(Ident nm, BTree<long, object> m, Context cx, CTree<TypedValue,bool>? cs=null)
           {
               if (cx.ParsingGQL==2)
                   return EdgeType;
               cs ??= CTree<TypedValue, bool>.Empty;
               var un = (CTree<Domain, bool>)(m[Under] ?? CTree<Domain, bool>.Empty);
               var nu = CTree<Domain, bool>.Empty;
               for (var b = un.First(); b != null; b = b.Next())
                   nu += ((b.key() is GqlLabel gl) ? (cx._Ob(cx.role.dbobjects[gl.name ?? ""] ?? -1L)
                       as Domain) ?? throw new DBException("42107", gl.name ?? "??") : b.key(), true);
               var dc = (CTree<string, QlValue>)(m[GqlNode.DocValue] ?? CTree<string, QlValue>.Empty);
               if (cx._Ob(cx.role.edgeTypes[nm.ident] ?? -1L) is not EdgeType et)
               {
                   var pt = new PEdgeType(nm.ident, EdgeType, nu, -1L, cx.db.nextPos, cx, true);
                   et = (EdgeType)(cx.Add(pt) ?? throw new DBException("42105"));
                   var mp = cx.metaPending[nm.uid] ?? CTree<long, CTree<string, TMetadata>>.Empty;
                   for (var b = mp.First(); b != null; b = b.Next())
                       for (var c = b.value().First(); c != null; c = c.Next())
                       {
                           var ms = c.value();
                           for (var d = (ms[Qlx.EDGETYPE] as TSet)?.First(); d != null; d = d.Next())
                               cs += (d.Value(), true);
                       }
                   cx.metaPending -= nm.uid;
                   cx.db += et;
               }
               for (var b = cs.First(); b != null; b = b.Next())
                   if (b.key() is TConnector tc)
                       (et,tc) = et.BuildNodeTypeConnector(cx, tc);
               var ro = cx.role;
               var e = (EdgeType?)cx.obs[et.defpos] ?? throw new DBException("42105");
               for (var b = dc.First(); b != null; b = b.Next())
                   if (!e.AllCols(cx).Contains(b.key()) && b.value() is QlValue q)
                   {
                       var pc = new PColumn3(e, b.key(), -1, q.domain, "", true, GenerationRule.None,
                           q.metadata, cx.db.nextPos, cx, true);
                       e = (EdgeType)(cx.Add(pc) ?? throw new DBException("42105"));
                   }
               e = (EdgeType)e.Build(cx, null, 0L, nm.ident, dc);
               return e;
           } */
           internal override CTree<Domain, bool> _NodeTypes(Context cx)
           {
               var r =  base._NodeTypes(cx);
               if (kind == Qlx.AMPERSAND)
               {
                   if (cx._Ob(left) is NodeType nl)
                       r += nl._NodeTypes(cx);
                   if (cx._Ob(right) is NodeType nr)
                       r += nr._NodeTypes(cx);
               }
               return r;
           }
           public override string ToString()
           {
               var sb = new StringBuilder(GetType().Name);
               if (left > 0)
                   { sb.Append(' '); sb.Append(Uid(left)); }
               sb.Append(' '); sb.Append(kind);
               if (right>0)
                   { sb.Append(' '); sb.Append(Uid(right)); }
               return sb.ToString();
           }
       } 
    /// <summary>
    /// Structural information about edge connections is copied to subtypes.
    /// </summary>
    internal class EdgeType : NodeType
    {
        internal EdgeType(long lp,long dp, string nm, UDType dt, BTree<long,object> m, Context cx)
            : base(lp, dp, nm, dt, _Mem(m), cx)
        { }
        internal EdgeType(Qlx t) : base(t)
        { }
        public EdgeType(long dp, BTree<long, object> m) : base(dp, _Mem(m))
        { }
        static BTree<long, object> _Mem(BTree<long, object> m)
        {
            var md = (TMetadata)(m[ObInfo._Metadata]
                ?? (TMetadata.Empty +(Qlx.EDGETYPE, new TChar("EDGETYPE"))));
            return m + (ObInfo._Metadata, md);
        }
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
            return new EdgeType(dp, m);
        }
        internal override UDType New(Ident pn, CTree<Domain,bool> un, long dp, Context cx) 
        {
            var nd = (EdgeType)Relocate(dp);
            if (nd.defpos!=dp)
                nd.Fix(cx);
            return (UDType)(cx.Add(new PEdgeType(pn.ident, nd, un, -1L, dp, cx))
                ?? throw new DBException("42105").Add(Qlx.EDGETYPE));
        }  
        internal override NodeType Check(Context cx, GqlNode n, long ap, bool allowExtras = true)
        {
            var et = (EdgeType)base.Check(cx, n, ap, allowExtras);
            // TBD
            return et;
        }
        internal override Table AddNodeOrEdgeType(Context cx)
        {
            var ro = cx.role;
            var nm = NameFor(cx);//name ?? label.name;
            if (nm != "")
            {
                var ep = ro.edgeTypes[nm];
                var ed = cx._Ob(ep) as Domain;
                if (ep ==0L || ed?.kind == Qlx.NODETYPE) // second term here is for Metadata.EdgeType
                {
                    ro += (Role.EdgeTypes, ro.edgeTypes + (nm, defpos));
                    cx.db += ro;
                }
                else if (ed is not null && ed.defpos != defpos)
                {
                    if (ed.kind == Qlx.EDGETYPE)
                        ed = (Domain)cx.Add(UnionType(ed.defpos + 1L, ed, this));
                    else if (ed.kind == Qlx.UNION)
                        ed = (Domain)cx.Add(new Table(ed.defpos, ed.alts));
                    else throw new PEException("PE20901");
                    ro += (Role.EdgeTypes, ro.edgeTypes + (nm, ed.defpos));
                    ro += (Role.DBObjects, ro.dbobjects + (nm, ed.defpos));
                    cx.db += ed;
                    cx.db += ro;
                }
            }
            cx.db += (Database.Role, cx._Ob(cx.role.defpos) ?? throw new DBException("42105"));
            var r = this;
            if (infos[cx.role.defpos] is ObInfo oi)
            {
                oi += (Method.TypeDef, this);
                r += (Infos, infos + (cx.role.defpos, oi));
            }
            cx.db += r;
            return r;
        }
        /*       internal EdgeType Connect(Context cx, GqlNode? b, GqlNode? a, TypedValue cc,
                    bool allowChange = false, long dp = -1L)
               {
                   if (cc is not TConnector ec || ec.cp>0L)
                       return this;
                   var found = false;
                   for (var c = (metadata[Qlx.REFERENCES] as TSet)?.First(); c != null; c = c.Next())
                       if (c.Value() is TConnector tc)
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
                               found = true;
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
                       var cn = (ec.cn == "") ? q.ToString() : ec.cn;
                       var nc = new TConnector(q, nn, cn, Ref);
                       if (nn > 0 && defpos > 0)
                           (r, _) = BuildNodeTypeConnector(cx, nc);
                       else
                           cx.MetaPend(dp + 1L, dp + 1L, cn,
                               TMetadata.Empty + (Qlx.REFERENCES, new TSet(Connector) + nc));
                   }
                   return r;
               } */
        /*     internal (EdgeType, CTree<string, QlValue>) Connect(Context cx, TNode? b, TNode a, GqlEdge ed, TypedValue cc,
                     CTree<string, QlValue> ls, bool allowChange = false)
             {
                 if (cc is not TConnector nc)
                     return (this,ls);
                 var found = false;
                 for (var c = (metadata[Qlx.REFERENCES] as TSet)?.First(); c != null; c = c.Next())
                     if (c.Value() is TConnector tc)
                     {
                         TypedValue qv = tc.q switch
                         {
                             Qlx.TO => nc.q switch
                             {
                                 Qlx.ARROW or Qlx.ARROWR => Connect(cx, a, nc, tc, ed), // ]-> ->
                                 Qlx.RARROW or Qlx.ARROWL => Connect(cx, b, nc, tc, ed), // <-[ <-
                                 _ => TNull.Value
                             },
                             Qlx.FROM => nc.q switch
                             {
                                 Qlx.ARROWBASE or Qlx.ARROWR => Connect(cx, b, nc, tc, ed), // -[ ->
                                 Qlx.RARROWBASE or Qlx.ARROWL => Connect(cx, a, nc, tc, ed), // ]- <-
                                 _ => TNull.Value
                             },
                             Qlx.WITH => nc.q switch
                             {
                                 Qlx.ARROWLTILDE or Qlx.RARROWTILDE or Qlx.ARROWBASETILDE // <~ <~[ ~[
                                     => Connect(cx, b, nc, tc, ed),
                                 Qlx.RBRACKTILDE or Qlx.ARROWTILDE or Qlx.ARROWRTILDE // ]~ ]~> ~>
                                     => Connect(cx, a, nc, tc, ed),
                                 Qlx.TILDE => Connect(cx, a, nc, tc, ed) ?? Connect(cx, b, nc, tc, ed), // ~
                                 _ => TNull.Value
                             },
                             _ => TNull.Value
                         };
                         if (qv != TNull.Value)
                         {
                             var n = cx.NameFor(tc.cp) ?? tc.cn;
                             if (tc.cd.kind == Qlx.REF)
                                 ls += (n, new SqlLiteral(cx.GetUid(), qv));
                             else
                             {
                                 var ov = ls[tc.cn]?._Eval(cx) ?? TNull.Value;
                                 ls += (n, new SqlLiteral(cx.GetUid(), tc.cd.Coerce(cx, qv + ov)));
                             }
                             found = true;
                             break;
                         }
                     }
                 var r = this;
                 if (!found)
                 {
                     TNode? nn = null;
                     Qlx q = Qlx.Null;
                     long bt = (b == null) ? -1L : b.dataType.defpos;
                     long at = a.dataType.defpos;
                     if (b != null) switch (nc.q)
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
                             new TConnector(q, nn.dataType.defpos, nc.cn, Ref));
                         ls += (cx.NameFor(rc.cp) ?? rc.cn,
                             (SqlLiteral)cx.Add(new SqlLiteral(cx.GetUid(), new TRef(nn.defpos,nc.ct))));
                     }
                 }
                 return (r, ls);
             }
             internal static TypedValue Connect(Context cx, TNode? n, TConnector nc, TConnector ec, GqlEdge ed,
                 CTree<long,long>? rn = null)
             {
                 if (n == null || (nc.cn != "" && ec.cn.ToUpper() != nc.cn.ToUpper()))
                     return TNull.Value;
                 if (cx._Ob(nc.ct) is GqlLabel gl && gl.kind == Qlx.AMPERSAND)
                     for (var b = gl._NodeTypes(cx).First(); b != null; b = b.Next())
                         if (Connect(cx, n, (NodeType)b.key(), nc, ec, ed, rn) is TypedValue v && v != TNull.Value)
                             return v;
                 if (n.dataType is NodeType nt)
                     return Connect(cx, n, nt, nc, ec, ed, rn)
                             ?? ((cx.values[rn?[n.defpos] ?? -1L] is TNode m)?
                             Connect(cx,m,m.dataType as NodeType,nc,ec,ed,rn) ?? TNull.Value
                             : TNull.Value);
                 throw new DBException("22G0V");
             }
             static TypedValue? Connect(Context cx, TNode n, NodeType? nt, TConnector nc, TConnector ec, GqlEdge ed,
                 CTree<long,long>? rn = null)
             {
                 if (nt is null)
                     return null;
                 if (ec.ct != nt.defpos && cx._Ob(ec.ct) is Domain en)
                 {
                     if (en is NodeType && !nt.EqualOrStrongSubtypeOf(en))
                         return null;
                     if (en is GqlLabel el && el.kind == Qlx.AMPERSAND)
                     {
                         var ok = false;
                         for (var b = el._NodeTypes(cx).First(); (!ok) && b != null; b = b.Next())
                             ok = nt.EqualOrStrongSubtypeOf(b.key());
                         if (!ok)
                             return null;
                     }
                 }
                 var m = (rn?[n.defpos] is long mp && mp > 0) ? mp : n.defpos;
                 if (ec.cd.kind == Qlx.REF)
                     return new TRef(m,n.tableRow.tabledefpos);
                 if (ec.cd.kind == Qlx.SET && ec.cd is Domain de)
                     return (de.kind == Qlx.REF) ? new TRef(m,n.tableRow.tabledefpos) : n;
                 if (cx._Ob(ec.ct) is Domain d && n.dataType.EqualOrStrongSubtypeOf(d))
                     return n;
                 throw new DBException("22G0V");
             }
             internal (EdgeType,TConnector) BuildNodeTypeConnector(Context cx, TConnector tc)
             {
                 if (metadata[Qlx.REFERENCES] is TSet ts && ts.Contains(tc))
                     return (this, tc);
                 var d = tc.ct ?? throw new PEException("PE90151");
                 if (d is NodeType nt)
                     return BuildNodeTypeConnector(cx, tc, nt);
                 else
                     for (var c = d.unionOf.First(); c != null; c = c.Next())
                         if (c.key() is NodeType ct)
                             return BuildNodeTypeConnector(cx, tc, ct);
                 throw new PEException("PE40721");
             }
             (EdgeType,TConnector) BuildNodeTypeConnector(Context cx,TConnector tc, NodeType nt)
             {
                 var ut = cx._Ob(cx.role.edgeTypes[name] ?? -1L) as EdgeType ?? this;
                 var cs = CTree<(Qlx, Domain), CTree<TypedValue,bool>>.Empty;
                 var ns = CTree<string, TConnector>.Empty;
                 var k = 1;
                 for (var b = (ut.metadata[Qlx.REFERENCES] as TSet)?.First(); b != null; b = b.Next())
                     if (b.Value() is TConnector c && c.ct is NodeType n)
                     {
                         var cl = cs[(c.q, n)] ?? CTree<TypedValue,bool>.Empty;
                         if (tc.q == c.q)
                             k++;
                         cs += ((c.q, n), cl+(c,true));
                         ns += (c.cn, c);
                     }
                 var dn = cx._Ob(tc.ct) as Domain ?? throw new PEException("PE90152"); // dn might be a Union of NodeTypes
                 var tt = cs[(tc.q, dn)];
                 var cn = (tc.cn == "")? (tc.q.ToString()+k) : tc.cn;
                 if (ns[cn] is TConnector x && x.cp>0L)
                     return (ut,x);
                 var tn = new TConnector(tc.q, tc.ct, tc.cn, tc.cd, cx.db.nextPos, tc.cs, tc.cm);
                 var md = (tc.cm ?? TMetadata.Empty) + (Qlx.REFERENCES, tn) + (Qlx.OPTIONAL,TBool.False);
                 var pc = new PColumn3(ut, cn, Length, Ref,"", md, cx.db.nextPos, cx, false);
                 ut = (EdgeType)(cx.Add(pc) ?? throw new DBException("42105").Add(Qlx.COLUMN));
                 var nc = (TableColumn)(cx._Ob(pc.ppos) ?? throw new DBException("42105").Add(Qlx.COLUMN));
                 var di = new Domain(-1L, cx, Qlx.ROW, new BList<DBObject>(nc), 1);
                 var px = new PIndex(cn, ut, di, PIndex.ConstraintType.ForeignKey | PIndex.ConstraintType.CascadeUpdate
                             | PIndex.ConstraintType.CascadeDelete,
                     -1L, cx.db.nextPos,cx.db,false);
                 nc += (Level3.Index.RefIndex, px.ppos);
                 ut = (EdgeType)(cx.Add(px) ?? throw new DBException("42105").Add(Qlx.REF));
                 var um = ut.metadata[Qlx.REFERENCES] as TSet ?? new TSet(Connector);
                 ut = (EdgeType)ut.Add(cx,ut.metadata + (Qlx.REFERENCES, um + tn));
                 ut.AddNodeOrEdgeType(cx);
                 return ((EdgeType)cx.Add(ut),tn);
             } */
  /*      internal override Table? Delete(Context cx, Delete del)
        {
            if (tableRows[del.delpos] is TableRow tr && infos[definer] is ObInfo oi)
                for (var b = colRefs.First(); b != null; b = b.Next())
                    for (var c = b.value().First(); c != null; c = c.Next())
                        if (cx._Ob(c.key()) is TableColumn cc)
                            for (var d = cc.cs.First(); d != null; d = d.Next())
                                if (d.value() is TConnector co && co.rd is Table nt
                                && tr.vals[co.cp] is TInt li && li.ToLong() is long lp
                                && nt.sindexes[lp] is CTree<long, CTree<long, bool>> Ll
                                && Ll[lp] is CTree<long, bool> Lll)
                             cx.db += nt + (SysRefIndexes, nt.sindexes + (lp, Ll + (co.cp, Lll - del.delpos)));
            return base.Delete(cx, del);
        }
        internal override void Update(Context cx, TableRow prev, CTree<long, TypedValue> fields)
        {
            var oi = infos[definer] ?? throw new PEException("PE69103");
            for (var b = colRefs.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx._Ob(c.key()) is TableColumn cc)
                        for (var d = cc.cs.First(); d != null; d = d.Next())
                            if (d.value() is TConnector co && co.rd is Table nt
                            && prev.vals[co.cp] is TInt li
                            && fields[co.cp] is TInt lu && li.CompareTo(lu) != 0
                            && li.ToLong() is long lp
                            && nt.sindexes[lp] is CTree<long, CTree<long, bool>> Ll
                            && Ll[co.cp] is CTree<long, bool> Lll)
                          cx.db += nt + (SysRefIndexes, nt.sindexes + (lp, Ll + (co.cp, Lll - prev.defpos)));
        } */
        internal override Domain? HaveNodeOrEdgeType(Context cx)
        {
            if (name!="")
            {
                // this should check if we have the same TConnectors, possibly in a different order
                // and that the target nodetypes match
                if (cx.role.edgeTypes[name] is long ep && cx._Ob(ep) is Domain d)
                {
                    if (d is EdgeType ed)
                        return EdgeSubTypeOf(cx,ed);
                    if (d.kind == Qlx.UNION)
                    {
                        for (var c = d.alts.First(); c != null; c = c.Next())
                            if (cx._Ob(c.key().defpos) is EdgeType ee
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
                    return this;
            return null;
        }
        EdgeType? EdgeSubTypeOf(Context cx,EdgeType d)
        {
            var cons = infos[cx.role.defpos]?.metadata[Qlx.EDGETYPE] as TSet;
            var dcons = d.infos[cx.role.defpos]?.metadata[Qlx.EDGETYPE] as TSet;
            var cs = CTree<TypedValue, bool>.Empty;
            if (cons==null || dcons == null || dcons.tree.Count != cons.tree.Count)
                return null;
            for (var b = cons.First(); b != null; b = b.Next())
            {
                if (b.Value() is TConnector c)
                {
                    for (var db = dcons.First(); db != null; db = db.Next())
                        if (db.Value() is TConnector dc && dc.rd is Domain td
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
                    r = (EdgeType)r.BuildNodeTypeConnector(cx, tc).Item1;
            return r;
        }
/*        /// <summary> This seems to be unreferenced
        /// Two cases: adding new Table metadata (REFERENCES) define a set of connector columns 
        ///            adding a single new Column metadata (CONNECTING) for defining its index
        /// The column case is usually called from the table case (s is likely "" i.e. unchanged)
        /// but we will probably allow the edgetype to be extended by an extra connector.
        /// This means we should check & modify the REFERENCES metadata if needed (but avoid further recursion)
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="s"></param>
        /// <param name="md"></param>
        /// <returns></returns>
        internal override (Context,DBObject) Add(Context cx, TMetadata md)
        {
            var ro = cx.role;
            (cx,var r) = base.Add(cx,md);
            if (infos[definer]?.metadata[Qlx.REFERENCES] is TSet ts && r.infos[ro.defpos] is ObInfo oi
                && oi.name is not null)
            {
                if (oi.name != "")
                {
                    if (cx.role.edgeTypes[oi.name] is long ep && cx._Ob(ep) is Domain ed)
                    {
                        if (ed.kind == Qlx.UNION)
                        {
                            var ev = cx.Add(new Table(ep, ed.alts));
                            ro += (Role.EdgeTypes, ro.edgeTypes + (oi.name, ev.defpos));
                        }
                    }
                    else
                        ro += (Role.EdgeTypes, ro.edgeTypes + (oi.name, defpos));
                    cx.db += ro;
                }
                // watch for an edge subtype
                TSet? sm = null;
                for (var b = (r as EdgeType)?.super.First(); b != null; b = b.Next())
                    if (b.key() is EdgeType be && be.infos[be.definer]?.metadata[Qlx.REFERENCES] is TSet bs)
                        if (sm == null) sm = bs; else sm += bs;
                for (var b = sm?.First(); b != null; b = b.Next())
                    if (b.Value() is TConnector sc)
                        for (var c = ts.First(); c != null; c = c.Next())
                            if (c.Value() is TConnector cc && cc.q == sc.q && cc.cn == sc.cn
                                && cc.cp == cx.db.nextPos && sm != null)
                            {
                                ts -= cc;
                                ts += new TConnector(cc.q, cc.cn, cc.rd, sc.cp, sc.fk, "", cc.cm);
                            }
                if (ts.Cardinality() == 0)
                    md -= Qlx.REFERENCES;
                else
                    md += (Qlx.REFERENCES, ts);
                cx.db += r;
            }
            if (md[Qlx.CONNECTING] is TConnector tc)
            {
                var nc = cx.obs[tc.cp] as TableColumn;
                if (nc == null)
                    if (r is Table t) // this is a surprise but likely allowed
                    {
                        var cm = tc.cm ?? TMetadata.Empty + (Qlx.OPTIONAL, TBool.False);
                        var pc = new PColumn3(t, tc.cn, Ref, "", cm, cx.db.nextStmt, tc.cp, cx, true);
                        pc.reftype = t.defpos;
                        r = (EdgeType)(cx.Add(pc) ?? throw new DBException("42105").Add(Qlx.COLUMN));
                        nc = cx.obs[tc.cp] as TableColumn ?? throw new PEException("PE20832");
                        var om = r.infos[r.definer]?.metadata[Qlx.REFERENCES] as TSet ?? new TSet(Connector);
                        (cx, r) = r.Add(cx, TMetadata.Empty + (Qlx.REFERENCES, om + tc));
                    }
                    else
                        throw new DBException("42000");
                var di = new Domain(-1L, cx, Qlx.ROW, new BList<DBObject>(nc), 1);
                var px = new PIndex(tc.cn, this, di, PIndex.ConstraintType.ForeignKey | PIndex.ConstraintType.CascadeUpdate
                            | PIndex.ConstraintType.CascadeDelete,
                    -1L, cx.db.nextPos, cx.db, false);
       //         nc += (Level3.Index.RefIndex, px.ppos);
                cx.Add(nc);
                r = (EdgeType)(cx.Add(px) ?? throw new DBException("42105").Add(Qlx.REF));
            }
            cx.db += r;
            cx.Add(r);
            return (cx,r);
        } */
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
          //              rt += pn.idCol;
                        rs += (pn.idCol, pn.idColDomain);
                    }
                    for (var b = pd?.rowType.First(); b != null; b = b.Next())
                        if (b.value() is long p && pd?.representation[p] is Domain cd && !rs.Contains(p))
                        {
                            rt += ((int)rt.Count,p);
                            rs += (p, cd);
                        }
                    for (var b = rowType.First(); b != null; b = b.Next())
                        if (b.value() is long p && representation[p] is Domain cd && !rs.Contains(p))
                        {
                            rt += ((int)rt.Count,p);
                            rs += (p, cd);
                        }
                }
            return new Table(cx, rs, rt, display, ii);
        }
        internal override Basis Fix(Context cx)
        {
            var r = New(cx.Fix(defpos), cx.Fix(mem));
            var ro = cx.role;
      //      cx.db += ro + (Role.EdgeTypes, ro.edgeTypes + (name, cx.Fix(defpos)));
            if (cx.db.objects.Contains(defpos))
                cx.db += this;
            if (defpos != -1L)
                cx.Add(r);
            return r;
        }
 /*       public override Domain For()
        {
            return EdgeType;
        } */
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
            var et = cx._Ob(cx.role.edgeTypes[name]) as EdgeType
                    ?? (EdgeType)(cx.Add(new PEdgeType(name, this, super, -1L, cx.db.nextPos,cx, true))
                    ?? throw new DBException("42105"));
            cx.obs += (defpos, et);
            var dc = (CTree<string, QlValue>?)m?[GqlNode.DocValue];
            for (var b = dc?.First(); b != null; b = b.Next())
                if (b.value() is QlValue sv && !et.AllCols(cx).Contains(b.key()))
                {
                    var pc = new PColumn3(et, b.key(), sv.domain, "",TMetadata.Empty, 
                        cx.db.nextStmt, cx.db.nextPos, cx, true);
                    et = (EdgeType)(cx.Add(pc) ?? throw new DBException("42105"));
                }
            for (var b = cs?.First(); b != null; b = b.Next())
                if (b.key() is TConnector tc)
                    et = (EdgeType)et.BuildNodeTypeConnector(cx, tc).Item1;
            return new CTree<Domain, bool>(et, true);
        }
        public override string Describe(Context cx)
        {
            var sb = new StringBuilder();
            sb.Append("DIRECTED ");
            sb.Append("EDGE "); sb.Append(name);
            var cm = " {";
            for (var b = First(); b != null; b = b.Next())
                if (b.value() is long p && representation[p] is Domain d)
           //         && d.kind!=Qlx.REF)
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
        /*       internal TypedValue PreConnect(Context cx, Qlx ab, Domain ct, string cn)
               {
                   TypedValue r = TNull.Value;
                   var q = ab switch
                   {
                       Qlx.ARROWBASE => Qlx.FROM,
                       Qlx.RARROW => Qlx.TO,
                       Qlx.ARROWBASETILDE or Qlx.TILDE or Qlx.RBRACKTILDE => Qlx.WITH,
                       _ => ab
                   };
                   for (var b = (metadata[Qlx.EDGETYPE] as TSet)?.First(); 
                       r==TNull.Value && b != null; b = b.Next())
                       if (b.Value() is TConnector tc && cx._Ob(tc.ct) is Domain dt
                           && tc.q == q && (ct.defpos<0 || ct.EqualOrStrongSubtypeOf(dt)) 
                           && (cn == "" || cn == tc.cn))
                           r = tc;
                   if (r == TNull.Value && cx.parsingGQL!=Context.ParsingGQL.Match)
                       return (EdgeType)BuildNodeTypeConnector(cx, new TConnector(q, ct.defpos, cn, Ref),this).Item2;
                   return r;
               }
               internal TypedValue PostConnect(Context cx, Qlx ba, Domain ct, string cn)
               {
                   TypedValue r = TNull.Value;
                   var q = ba switch
                   {
                       Qlx.ARROW => Qlx.TO,
                       Qlx.RARROWBASE => Qlx.FROM,
                       Qlx.ARROWBASETILDE or Qlx.TILDE or Qlx.RBRACKTILDE => Qlx.WITH,
                       _ => ba
                   };
                   for (var b = (metadata[Qlx.EDGETYPE] as TSet)?.First(); 
                       r==TNull.Value && b != null; b = b.Next())
                       if (b.Value() is TConnector tc && cx._Ob(tc.ct) is Domain dt
                           && tc.q == q && (ct.defpos < 0 || ct.EqualOrStrongSubtypeOf(dt))
                           && (cn == "" || cn == tc.cn))
                           r = tc;
                   if (r == TNull.Value && cx.ParsingGQL!=2)
                       return BuildNodeTypeConnector(cx, new TConnector(q, ct.defpos, cn, Ref), this).Item2;
                   return r;
               } */
    } 
    /// <summary>
    /// See GQL 4.13: it is a set of node types and edge types that are defined as constraints on a Graph
    /// </summary>
    internal class GraphType : Domain
    {
        /*       internal GraphType(PGraphType pg, Context cx, long ap)
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
           { */
        internal const long
            GraphTypes = -122, // CTree<long,bool> GraphType
            Iri = -147, // string
            _Schema = -450;  // long Schema (GQL)
       //     Nodes = -499; // CTree<long,TNode> // and edges
       // internal CTree<long,TNode> nodes =>
       //     (CTree<long, TNode>) (mem[Nodes]??CTree<long,TNode>.Empty);
        internal CTree<long,bool> graphTypes => 
            (CTree<long,bool>)(mem[GraphTypes] ?? CTree<long, bool>.Empty);
        internal long schema => (long)(mem[_Schema]??-1L);
        internal string iri => (string)(mem[Iri]??"");
      //  internal long schema => (long)(mem[Schema] ?? -1L);
        internal GraphType(PGraphType pg,Context cx,long ap)
            : base(pg.ppos,_Mem(cx,pg,ap))
        {
            cx.graph = this;
            cx.schema = cx._Ob(schema) as Schema;
        }
        public GraphType(long dp, BTree<long, object> m) : base(dp, m)
        { }
        static BTree<long,object> _Mem(Context cx,PGraphType ps,long ap)
        {
            var r = BTree<long, object>.Empty
                 + (GraphTypes, ps.types ?? CTree<long, bool>.Empty);
            var nm = ps.iri;
            r += (ObInfo.Name, nm);
            var ix = ps.iri.LastIndexOf('/');
            if (ix >= 0)
            {
                nm = ps.iri[(ix + 1)..];
                ps.name = nm;
                ps.iri = ps.iri[0..ix];
            }
            if (cx.role.schemas[ps.iri] is long sp && sp>=0)
                r += (_Schema, sp);
            var oi = new ObInfo(nm, Grant.AllPrivileges);
            var ns = Names.Empty;
            for (var b = ps.types?.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is Table ut)
                    ns += (ut.name, (ap,b.key()));
            oi += (ObInfo._Names, ns);
            r += (Infos, new BTree<long, ObInfo>(cx.role.defpos, oi));
            var ro = cx.role;
            ro = ro + (Role.Graphs, ro.graphs + (nm, ps.ppos));
            cx.db += (Database.Role, ro);
            return r;
        }
        public static GraphType operator +(GraphType et, (long, object) x)
        {
            return (GraphType)et.New(et.defpos, et.mem + x);
        }
        public static GraphType operator+(GraphType g,TNode r)
        {
            return new GraphType(g.defpos,g.mem + (Nodes,g.nodes+(r.tableRow.defpos,true)));
        }
        public override int CompareTo(object? obj)
        {
            if (obj is not GraphType tg)
                return 1;
            var c = iri.CompareTo(tg.iri);
            if (c != 0) return c;
            c = nodes.CompareTo(tg.nodes);
            if (c!=0) return c;
            return graphTypes.CompareTo(tg.graphTypes);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("GraphType (");
            sb.Append(name);
            var cm = "[";
            for (var b=graphTypes.First();b is not null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.key());
            }
            if (cm==",")
                sb.Append(']');
            return sb.ToString();
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new GraphType(dp,m);
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
            (string)(mem[GraphType.Iri] ?? "");
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
            r += (GraphType.Iri, ps.directoryPath);
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
        protected readonly TableRow _tableRow;
        public virtual TableRow tableRow => _tableRow;
        public long defpos => tableRow.defpos;
//        public TypedValue id => tableRow.vals[(dataType as UDType)?.idCol??-1L]??new TInt(defpos);
        internal TNode(Context cx, TableRow tr)
            : base(_Type(cx,tr))
        {
            _tableRow = tr;
        }
        static Table _Type(Context cx,TableRow tr)
        {
            var nm = cx._Ob(tr.tabledefpos) as Table ?? throw new PEException("PE50402");
            return nm.Specific(cx, tr);
        }
        internal override TypedValue this[long p] => tableRow.vals[p]??TNull.Value;
        internal virtual bool CheckProps(Context cx,TNode n)
        {
            return dataType.defpos == n.dataType.defpos;// && id == n.id;
        }
        internal override TypedValue Fix(Context cx)
        {
            return new TNode(cx,tableRow.Fix(cx));
        }
        internal virtual Names _Names(Context cx)
        {
            return dataType.names ?? Names.Empty;
        }
        internal override string ToString(Context cx)
        {
            if (cx._Ob(dataType.defpos) is not Table nt)
                return "??";
            var sb = new StringBuilder();
            sb.Append(nt.name);
            var cm = '(';
            var tb = nt._PathDomain(cx);
            for (var b = tb.First(); b != null; b = b.Next())
                if (b.value() is long cp && tb.representation[cp] is not null
          //          && tb.representation[cp]?.kind!=Qlx.REF
                    && (cx._Ob(cp) as TableColumn)?.name is string nm)
                {
                    sb.Append(cm); cm = ',';
                    sb.Append(nm); sb.Append('=');
                    sb.Append(tableRow.vals[cp]);
                }
            if (cm==',')
                sb.Append(')');
            return sb.ToString();
        }
        internal virtual string[] Summary(Context cx)
        {
            if (cx._Ob(dataType.defpos) is not Table nt ||
                nt.infos[cx.role.defpos] is not ObInfo ni)
                return [];
            var ss = new string[Math.Max(nt.Length,5)+1];
            ss[0] = ni.name ?? "";
            for (var b = nt.First(); b != null && b.key() < 5; b = b.Next())
                if (b.value() is long cp && cx._Ob(cp) is TableColumn tc 
                    && tc.infos[cx.role.defpos] is ObInfo ci)
                {
                    var u = ci.name;
                    var tv = tableRow.vals[cp];
                    var v = tv?.ToString() ?? "??";
                    if (v.Length > 50)
                        v = v[..50];
                    ss[b.key() + 1] = u + " = " + v;// + Link(cx,tc,tv);
                }
            return ss;
        }
        /*       internal static string Link(Context cx,TableColumn tc,TypedValue? tv)
               {
                   if (tc.tc is not TConnector  || cx._Ob(tc.tabledefpos) is not UDType nt)
                       return "";
                   var et = nt;// as EdgeType;
                   if (nt.infos[cx.role.defpos] is not ObInfo li
                       || cx._Ob(nt.idCol) is not TableColumn il 
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
               } */
        public override int CompareTo(object? obj)
        {
            if (obj is not TNode that) return 1;
            return tableRow.defpos.CompareTo(that.tableRow.defpos);
        }
        internal TNode Cast(Context cx,Domain dt)
        {
            if (dt.defpos == dataType.defpos)
                return this;
            if (dataType is Table nt && nt.tableRows[defpos] is TableRow tr)
            {
                if (dt.defpos < 0) // calculate specific type
                    return new TNode(cx, new TableRow(defpos, tr.prev, Specific(cx,nt), tr.vals));
                return new TNode(cx, new TableRow(defpos, tr.prev, dt.defpos, tr.vals));
            }
            return this;
        }
        long Specific(Context cx,Table nt)
        {
            for (var b = nt.subtypes.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is Table tb && tb.tableRows.Contains(defpos))
                    return Specific(cx, tb);
            return nt.defpos;
        }
        internal override long? ToLong()
        {
            return defpos;
        }
        public override string ToString()
        {
            return "TNode "+DBObject.Uid(defpos)+"["+ DBObject.Uid(dataType.defpos)+"]";
        }
    } 
    internal class TEdge : TNode
    {
   //     public readonly TList? args;
        internal TEdge(Context cx, TableRow tr /*, TList? a = null*/) : base(cx, tr)
        {
    //        args = a;
        }
   /*     public static TEdge operator+(TEdge a, (Context,TypedValue) x)
        {
            var (cx, v) = x;
            if (v is not TList tl)
                throw new PEException("PE40461");
            if (a.args == null)
                return new TEdge(cx, a.tableRow, tl);
            return new TEdge(cx, a.tableRow, a.args + tl);
        }
        public override TableRow tableRow => (args==null)?base.tableRow:; */
        public override string ToString()
        {
            var sb = new StringBuilder("TEdge ");
            sb.Append(DBObject.Uid(defpos));
            sb.Append('['); sb.Append(DBObject.Uid(dataType.defpos)); sb.Append(']');
    /*        var cm = '(';
            for (var b=args?.list.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ',';
                sb.Append(b.value().ToString());
            }
            if (cm == ',')
                sb.Append(')'); */
            return sb.ToString(); 
        }
    }
    /// <summary>
    /// Reference values are structural: record position for the tableRow.
    /// The tableRow at this position is for the most specific type of the record (which may be a join or union)
    /// and is entered in the tableRows trees for the tables of this most specific type, and all their supertypes.
    /// Connector information for expressions is role-dependent and constructed during parsing.
    /// Each TableColumn has a CTree(Domain,TConnector).
    /// The database gives it for the definer's role, and in ObInfo the metadata for other roles.
    /// Indexes are not compulsory: records and references can constructed in a single transaction
    /// (or searched later!). If they are used it is important to reference the type that owns the index.
    /// For example, in the Financial Benchmark database LEGALENTITY has no primary key but its subtype PERSON has, 
    /// and a subtype might provide more indexes. Any such can then be referenced in a foreign key definition.
    /// All reference values TREF(uid,t) will access the same tableRow at uid in the database, which will be
    /// of type t or a subtype of t.
    /// During Activations, each TableColumn immediately contains the connectors for 
    /// the definer's role (for uid-based reference) or the current role (for names and arrows). 
    /// Privileges for the current role are always a subset of those of the definer.
    /// </summary>
    internal class TConnector : TypedValue
    {
        public readonly Qlx q; // FROMn,TOn,WITHn are used in EdgeType, arrows in expressions
        public readonly string cn;  // string connector name (used during edgetype definition)
        public readonly long cp;   // a copy of the referencing Column uid (used during parsing of graph patterns)
        public readonly Domain rd; // The column is a REF: a copy of the declared type of the referenced node
        public readonly bool fk; // if true, it's a foreign key reference using rd's primary key
        public string cs;   // string version of metadata
        public readonly TMetadata? cm;
        internal TConnector(Qlx a,string s,Domain d,long p= -1L,bool f=false, string ss="",TMetadata? tm=null) 
            : base(Domain.Connector)
        {
            if (tm is null || !tm.Contains(Qlx.OPTIONAL))
            {
                tm ??= TMetadata.Empty;
                tm += (Qlx.OPTIONAL, TBool.False);
            }
            if ((a == Qlx.TO || a == Qlx.FROM || a == Qlx.WITH) && s == "")
                s = a + "1";
            q = a; cn = s; rd = d; cp = p; cs = ss;  cm = tm; fk = f;
        }
        internal override TConnector Fix(Context cx)
        {
            var r = new TConnector(q, cn, (Domain)rd.Fix(cx), cx.Fix(cp), fk, cs, 
                (TMetadata)(cm?.Fix(cx)??TMetadata.Empty));
            if (r.rd is Table t && !t.refCols.Contains(cp))
                cx.db += t + (Table.RefCols, t.refCols + (cp, true));
            return r;
        }
        public override int CompareTo(object? obj)
        {
            if (obj is not TConnector that)
                return 1;
            var c = q.CompareTo(that.q);
            if (c != 0)
                return c;
            if (cp == -1L || that.cp == -1L)
            {
                c = cn.CompareTo(that.cn);
                if (c != 0)
                    return c;
            }
            c = cp.CompareTo(that.cp);
            if (c != 0)
                return c;
            if (rd.kind != Qlx.CONTENT && that.rd.kind != Qlx.CONTENT)
            {
                c = rd.CompareTo(that.rd);
                if (c != 0)
                    return c;
            }
            c = cs.CompareTo(that.cs);
            if (c != 0)
                return c;
  /*          if (cm != null)
            {
                c = cm.CompareTo(that.cm);
                if (c != 0)
                    return c;
            }
            else if (that.cm == null)
                return -1;*/
            return 0;
        }
        internal override TypedValue Replaced(Context cx)
        {
            return new TConnector(q, cn, rd.Replaced(cx), cx.Replaced(cp),
                fk, cs, (TMetadata)(cm?.Replaced(cx) ?? TMetadata.Empty));
        }
        public override string ToString()
        {
            var sb = new StringBuilder(DBObject.Uid(cp));
            sb.Append(' '); sb.Append(q); sb.Append(' '); sb.Append(cn);
            sb.Append(' '); sb.Append(DBObject.Uid(rd.defpos));
            if (cm!=TMetadata.Empty)
            { sb.Append(' '); sb.Append(cm); }
            return sb.ToString();
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
            if (type.HasFlag(Type.Node) && cx._Ob(cx.role.nodeTypes[value]) is Table nt)
                return nt;
            if (type.HasFlag(Type.Edge) && cx._Ob(cx.role.edgeTypes[value]) is Domain d)
                return d as Table ??Domain.TypeSpec;
            if (type.HasFlag(Type.Type) && cx._Ob(cx.role.dbobjects[value]) is Table n)
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
