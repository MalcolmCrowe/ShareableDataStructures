using Pyrrho.Level3;
using Pyrrho.Common;
using Pyrrho.Level4;
using static Pyrrho.Level4.SystemRowSet;
using System.Text;
using System.Xml.XPath;
using System.Runtime.CompilerServices;
using System.Data.Common;
using Pyrrho.Level2;
using System.Xml;

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
// an edge type), with a further optional subtype imdication "$Under":id if the type is new.
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

// For the data management language, an SqlNode is an SqlRow whose domain is a Node type.
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
    /// A TGraph is a TypedValue whose dataType is a Node type.
    /// It always begins with the Node with the lowest uid, 
    /// is assembled from the rows of NodeTypes and EdgeTypes.
    /// </summary>
    internal class TGraph : TypedValue
    {
        internal readonly CTree<long, TNode> nodes; // and edges
        internal readonly CTree<string, TNode> nids; // and edges
        internal TGraph (CTree<long, TNode> ns, CTree<string, TNode> nids) : base(Domain.Graph)
        {
            nodes = ns;
            this.nids = nids;
        }
        internal TGraph(TNode n) : base(Domain.Graph)
        {
            nodes = new CTree<long,TNode>(n.uid,n);
            nids = new CTree<string, TNode>(n.id, n);
        }
        public static TGraph operator+(TGraph g,TNode n)
        {
            return new TGraph(g.nodes + (n.uid, n),g.nids + (n.id,n));
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
            if (tb!=null) return -1;
            return 0;
        }
        internal static CTree<TGraph, bool> Add(CTree<TGraph, bool> t, TNode n)
        {
            if (Find(t, n.id) is not null)
                return t;
            if (n is not TEdge)
                return t + (new TGraph(n), true);
            // Edge: end nodes already must be in t, but may be in different TGraphs
            var lu = n[1].ToString();
            var au = n[2].ToString();
            if (Find(t, lu) is not TGraph lg || Find(t, au) is not TGraph ag
                || ag.Rep() is not TNode lr || ag.Rep() is not TNode ar)
                return t;
            if (lr.uid == ar.uid) // already connected: add n to one of them
                return t - lg + (lg + n, true);
            else // merge the graphs and add n
                return t - ag -lg + (new TGraph(lg.nodes + ag.nodes + (n.uid, n), lg.nids + ag.nids + (n.id, n)), true);
        }
        static TGraph? Find(CTree<TGraph, bool> t, string n)
        {
            for (var b = t.First(); b != null; b = b.Next())
                if (b.key().nids.Contains(n))
                    return b.key();
            return null;
        }
        public override string ToString()
        {
            var sb = new StringBuilder("TGraph (");
            var cm = "[";
            for (var b=nodes.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.value());
            }
            sb.Append(']');
            return sb.ToString();
        }
    }
    /// <summary>
    /// A class for an unbound node or edge. As in the CREATE graph syntax, the properties of a
    /// new unbound identifier must be specified on its first occurrence.
    /// The identifier _ is special: we do not care about any of its properties.
    /// </summary>
    internal class TGParam : TypedValue
    {
        internal readonly long uid; 
        internal readonly string id;
        internal readonly Sqlx kind; // LPAREN node, RPAREN edge, COLON specifictype, EQL property
        internal readonly CTree<string, TypedValue> constraints;
        public TGParam(long dp, string i, Sqlx k,Domain dt, CTree<string, TypedValue> constraints) : base(dt)
        {
            uid = dp;
            id = i;
            kind = k;
            this.constraints = constraints;
        }
        internal override TypedValue New(Domain t)
        {
            return new TGParam(uid, id, kind, t, constraints);
        }
        public override int CompareTo(object? obj)
        {
            if (obj is TGParam tp)
            {
                if (id != "_")
                {
                    var c = id.CompareTo(tp.id);
                    if (c != 0)
                        return c;
                }
            }
            return -1;
        }
        public override string ToString()
        {
            return id + ':'+DBObject.Uid(uid);
        }
    }

    /*    /// <summary>
        /// SqlNode is an SqlRow whose first element is special: ID:CHAR.
        /// Its domainis a NodeType in which (ID) is the primary index px.
        /// </summary>
        internal class SqlNode : SqlRow
        {
            internal const long
                NominalType = -301, // long
                Proposals = -175;   // BTree<string,SqlValue> to be added during binding
            public long nominalType => (long)(mem[NominalType] ?? -1L);
            /// <summary>
            /// Before commit, we require props to be Empty, following binding
            /// </summary>
            internal BTree<string, SqlValue> props =>
                (BTree<string, SqlValue>)(mem[Proposals] ?? BTree<string, SqlValue>.Empty);
            protected SqlNode(long dp, BTree<long, object> m) : base(dp, m)
            {  }
            public SqlNode(long dp, NodeType xp, BList<DBObject> vs, BTree<long, object>? m = null) 
                : base(dp, xp, vs)
            {  }
            internal override Basis New(BTree<long, object> m)
            {
                return new SqlNode(defpos,m);
            }
            public static SqlNode operator+(SqlNode s,(long,object)x)
            {
                var (dp, ob) = x;
                if (s.mem[dp] == ob)
                    return s;
                return (SqlNode)s.New(s.mem + x);
            }
            internal override TypedValue Eval(Context cx)
            {
                if (cx._Dom(this) is not NodeType nt || cx.obs[nt.structure] is not Table tb
                    || tb.FindPrimaryIndex(cx) is not Level3.Index px)
                    return TNull.Value;
                var sv = cx._Ob(nt.rowType[0]??-1L) as SqlValue;
                if(sv==null)
                    return TNull.Value;
                var k = new CList<TypedValue>(sv.Eval(cx));
                var p = px.rows?.Get(k, 0);
                if (p==null)
                    return TNull.Value;
                var tr = tb.tableRows[p??-1L];
                if (tr == null)
                    return TNull.Value;
                return new TNode(p ?? -1L, nt, tr.vals);
            }
            public override string ToString()
            {
                var sb = new StringBuilder(base.ToString());
                if (nominalType>=0) 
                {
                    sb.Append(" NominalType "); sb.Append(Uid(nominalType));
                }
                if (props != CTree<string,SqlValue>.Empty)
                {
                    sb.Append(" Proposals (");
                    var cm = "";
                    for (var b = props.First(); b != null; b = b.Next())
                    {
                        sb.Append(cm); cm = ",";
                        sb.Append(b.key()); sb.Append("=(");
                        if (b.value() is SqlNode sn)
                        { sb.Append(sn.GetType().Name); sb.Append(" " + sn.name); }
                        else
                            sb.Append(b.value()); 
                        sb.Append(')');
                    }
                    sb.Append(')');
                }
                return sb.ToString();
            }
        }
        internal class SqlNodeLiteral : SqlNode
        {
            public SqlNodeLiteral(Context cx,TableRow r)
                : base(cx.GetUid(),_Mem(cx,r))
            {  }
            protected SqlNodeLiteral(long dp, BTree<long, object> m) 
                : base(dp, m)
            {   }
            static BTree<long, object> _Mem(Context cx, TableRow r)
            {
                var m = BTree<long, object>.Empty;
                var nt = (NodeType)(cx._Dom(cx._Ob(r.tabledefpos)) ?? throw new DBException("42000"));
                for (var b = nt.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && r.vals[p] is TypedValue v)
                        m += (p, new SqlLiteral(cx.GetUid(), v));
                return m;
            }
        }
        /// <summary>
        /// SqlEdge is an SqlRow whose first three elements are special: ID:CHAR, LEAVING:CHAR, ARRIVING:CHAR
        /// Its Domain is an EdgeType et in which (ID) is the primary index px.
        /// (LEAVING) references the NodeType et.LeavingType using index lx,
        /// and (ARRIVING) references the NodeType et.ArrivingType using index ax.
        /// It is bound if px.Contains(ID), in which case we also have lx.Contains(LEAVING) and ax.Contains(ARRIVING).
        /// </summary>
        internal class SqlEdge : SqlNode
        {
            protected SqlEdge(long dp, BTree<long, object> m) : base(dp, m)
            {
            }
            public SqlEdge(long dp,EdgeType et, BList<DBObject> vs)
                : base(dp, et, vs) { }
            internal override Basis New(BTree<long, object> m)
            {
                return new SqlEdge(defpos, m);
            }
            public static SqlEdge operator +(SqlEdge s, (long, object) x)
            {
                var (dp, ob) = x;
                if (s.mem[dp] == ob)
                    return s;
                return (SqlEdge)s.New(s.mem + x);
            }
        }
        internal class SqlEdgeLiteral : SqlNode
        {
            public TNode node =>
                (TNode)(mem[TrivialRowSet.Singleton] ?? throw new PEException("PE91207"));
            public SqlEdgeLiteral(long dp, TNode rw)
                : base(dp, BTree<long, object>.Empty + (_Domain, rw.dataType)
                     + (TrivialRowSet.Singleton, rw))
            { }
            protected SqlEdgeLiteral(long dp, BTree<long, object> m) : base(dp, m)
            { }
            internal override TypedValue Eval(Context cx)
            {
                return node;
            }
        } */
    /*    /// <summary>
        /// An SqlGraph is an SqlValueMultiset whose domain is Domain.Graph, and evaluates to give a TGraph. 
        /// Its values are SqlNodes from nominally disjoint graph expressions 
        /// (note that disjoint graph expressions can evaluate to non-disjoint TGraphs)
        /// </summary>
        internal class SqlGraph : SqlValueMultiset
        {
            internal static SqlGraph Empty = new SqlGraph();
            SqlGraph() : base(--_uid, Domain.Graph, CTree<long, bool>.Empty) { }
            public SqlGraph(long dp, CTree<long, bool> v) : base(dp, Domain.Graph, v)
            {
            }
            protected SqlGraph(long dp, BTree<long, object> m) : base(dp, m)
            {
            }
            internal SqlGraph(long dp, Context cx) : this(dp, _Mem(dp, cx)) { }
            /// <summary>
            /// Build an SqlGraph for the whole database: each item is a path like the rows in CREATE, MATCH.
            /// Each item starts with a node, and uses a set of nodes and unused edges.
            /// The first item is the unused node with the largest number of edges leaving it, and
            /// is followed by other items that start with this node.
            /// </summary>
            /// <param name="dp">The defpos for the SqlGraph</param>
            /// <param name="cx">The context</param>
            /// <returns>An SqlGraph for the whole database</returns>
            static BTree<long,object> _Mem(long dp, Context cx)
            {
                var r = BTree<long,object>.Empty;
                var un = cx.db.nodeIds; // the set of unused nodes and edges
                while (un!=BTree<string,long?>.Empty)
                {
                    // first find the unused node with the largest number of leaving edges
                }
                return r;
            }
            public static SqlGraph operator+(SqlGraph s,(long,object)x)
            {
                return new SqlGraph(s.defpos, s.mem + x);
            }
            internal override Basis New(BTree<long, object> m)
            {
                return new SqlGraph(defpos,m);
            }
            internal override DBObject New(long dp, BTree<long, object> m)
            {
                return new SqlGraph(dp, m);
            }
            internal override TypedValue Eval(Context cx)
            {
                return (TMultiset)base.Eval(cx);
            }
        }
        /// <summary>
        /// An SqlMatchExpr is an SqlValue containing unbound identifiers. 
        /// The result of a match is to give a number of alternative values for these identifiers, so
        /// it evaluates as a TArray of such rows.
        /// </summary>
        internal class SqlMatchExpr : SqlValue
        {
            public SqlMatchExpr(Context cx, BList<long?> us, SqlGraph sg)
                : base(cx.GetUid(),_Mem(cx,us,sg)) { }
            static BTree<long,object> _Mem(Context cx,BList<long?> us,SqlGraph sg)
            {
                var r = BTree<long, object>.Empty;
                return r;
            }
        } */
    /*    // a document value (field keys are constant strings, values are expressions)
        // this is very weakly typed!
        internal class SqlDocument : SqlRow
        {
            internal BTree<string, SqlValue> props =>
        (BTree<string, SqlValue>)(mem[SqlNode.Proposals] ?? BTree<string, SqlValue>.Empty);
            public SqlDocument(Context cx) 
                : base(cx.GetUid(),new BTree<long,object>(_Domain,Domain.Document))
            { }
            internal SqlDocument(long dp, BTree<long, object> m) : base(dp, m)
            { }
            public SqlDocument(Context cx, TDocument doc) : base(cx.GetUid(), _Mem(cx,doc))
            { }
            static BTree<long,object> _Mem(Context cx,TDocument doc)
            {
                var ps = BTree<string,SqlValue>.Empty;
                for (var f = doc.First(); f != null; f = f.Next())
                {
                    var (n, v) = f.value();
                    ps += (n, new SqlLiteral(cx.GetUid(),v));
                }
                return new BTree<long, object>(SqlNode.Proposals, ps);
            }
            public static SqlDocument operator +(SqlDocument s, (long, object) m)
            {
                return (SqlDocument)s.New(s.mem + m);
            }
            internal override Basis New(BTree<long, object> m)
            {
                return new SqlDocument(defpos, m);
            }
            internal override DBObject Relocate(long dp)
            {
                return (dp == defpos) ? this : new SqlDocument(dp, mem);
            }
            internal override BTree<long,Register> StartCounter(Context cx, RowSet rs, BTree<long, Register> tg)
            {
                for (var b = props.First(); b != null; b = b.Next())
                        tg = b.value().StartCounter(cx, rs, tg);
                return base.StartCounter(cx,rs,tg);
            }
            internal override BTree<long, Register> AddIn(Context cx, Cursor rb, BTree<long, Register> tg)
            {
                var dm = cx._Dom(this);
                for (var b = props.First(); b != null; b = b.Next())
                        tg = b.value().AddIn(cx, rb, tg);
                return tg;
            }
            internal override TypedValue Eval(Context cx)
            {
                var c = CList<(string, TypedValue)>.Empty;
                var n = CTree<string, int>.Empty;
                for (var b = props.First(); b != null; b = b.Next())
                {
                    c += (b.key(),b.value().Eval(cx));
                    n += (b.key(),(int)c.Count);
                }
                return new TDocument(c,n);
            }
            public override string ToString()
            {
                var sb = new StringBuilder(base.ToString());
                if (props != BTree<string, SqlValue>.Empty)
                {
                    sb.Append('{');
                    var cm = "";
                    for (var b = props.First(); b != null; b = b.Next())
                    {
                        sb.Append(cm); cm = ",";
                        sb.Append(b.key());
                        sb.Append(':');
                        sb.Append(b.value().ToString());
                    }
                    sb.Append('}');
                }
                return sb.ToString();
            }
        } */
}
