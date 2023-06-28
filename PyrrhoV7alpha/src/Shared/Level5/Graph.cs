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
            if (tb is not null) return -1;
            return 0;
        }
        internal static CTree<TGraph, bool> Add(CTree<TGraph, bool> t, TNode n)
        {
            if (Find(t, n.id) is not null)
                return t;
            if (n is not TEdge e)
                return t + (new TGraph(n), true);
            // Edge: end nodes already must be in t, but may be in different TGraphs
            if (Find(t, e.leaving.value) is not TGraph lg || Find(t, e.arriving.value) is not TGraph ag
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
            for (var b=nodes.First();b is not null;b=b.Next())
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
        internal readonly CTree<TypedValue, TypedValue> constraints;
        public TGParam(long dp, string i, Sqlx k,Domain dt, CTree<TypedValue, TypedValue> constraints) : base(dt)
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
            var sb = new StringBuilder(id);
            sb.Append(':'); sb.Append(kind); sb.Append(':');sb.Append(DBObject.Uid(uid));
            if (constraints!=CTree<TypedValue,TypedValue>.Empty)
            {
                var cm = "(";
                for (var b=constraints.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(b.key()); sb.Append('='); sb.Append(b.value());
                }
                if (cm != "(")
                    sb.Append(')');
            } 
            return sb.ToString();
        }
    }
}
