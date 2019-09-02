using System;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level4
{
    /// <summary>
    /// A Context contains the main data structures for analysing the meaning of SQL. 
    /// Each Context maintains an indexed set of its enclosed Contexts (named blocks, tables, routine instances etc).
    /// Query and Activation are the families of subclasses of Context. 
    /// Contexts and Activations form a stack in the normal programming implementation sense for execution,
    /// linked downwards by staticLink and dynLink. There is a pushdown Variables structure, so that 
    /// this[Ident] gives access to the TypedValue for that Ident at the top level. 
    /// (The SQL programming language does not need access to deeper levels of the activation stack.)
    /// A Context or Activation can have a pointer to the start of the currently executing query (called query).
    /// During parsing Queries are constructed: the query currently being worked on is called cur in the staticLink Context,
    /// parsing proceeds left to right, with nested Query instances backward loinked using the enc field. 
    /// All these queries have the same staticLink, which is the top of the Context/Activation stack bduring the parse.
    /// in this process a set of defs are accumulated in the query's staticLink.defs.
    /// Activation and Query provide Push and Pop methods for stack management, reflecting their different structure.
    /// Contexts provide a Lookup method for obtaining a TypedValue for a SqlValue sqid in scope;
    /// Queries uses a Lookup0 method that can be safely called from Lookup during parsing, this should not be called separately.
    /// All Context information is volatile and scoped to within the current transaction,to which it maintains a pointer.
    /// </summary>
    internal class Context
	{
        static long _cxid = 0;
        public long cxid = ++_cxid;
        internal string blockid;
        internal long user;
        internal long role;
        internal Context next;
        // debugging
        public override string ToString()
        {
            return "Context " + cxid;
        }
        public Ident alias = null;
        internal virtual bool NameMatches(Ident n)
        {
            return n.HeadsMatch(alias) == true;
        }
        /// <summary>
        /// A Lookup cache: constructed for Lookup calls when the context is created. 
        /// We tolerate 0 for segpos and allow partial matches.
        /// </summary>
        internal ATree<long,TypedValue> lookup = BTree<long, TypedValue>.Empty; 
        /// <summary>
        /// The query currently being parsed
        /// </summary>
        internal Query cur = null;
        /// <summary>
        /// Left-to-right accumulation of definitions during a parse: accessed only by Query
        /// </summary>
        internal Ident.Tree<SqlValue> defs = Ident.Tree<SqlValue>.Empty;
        /// <summary>
        /// Information gathered from view definitions: corresponding identifiers, equality conditions
        /// </summary>
        internal ATree<long, ATree<long,bool>> matching = BTree<long,ATree<long,bool>>.Empty;
        /// <summary>
        /// The current or latest statement
        /// </summary>
        public Executable exec = null;
        /// <summary>
        /// A context can have a datatype (e.g. the row type for its results, or the type of a structure)
        /// Queries can have scalar or row types from Pyrrho 6.2
        /// </summary>
        internal Domain nominalDataType = Domain.Value;
        /// <summary>
        /// Characterise this context for Ident processing
        /// </summary>
        internal Ident.IDType cxType = Ident.IDType.NoInput;
        /// <summary>
        /// database object references in the input string
        /// </summary>
        internal ATree<Ident,long?> refs = Ident.RenTree.Empty; // NB must be Ident.RenTree
        /// <summary>
        /// Create an empty context for the transaction 
        /// </summary>
        /// <param name="t">The transaction</param>
        internal Context(Transaction tr = null)
        {
            blockid = "T" + cxid;
            next = tr?.context;
        }
        internal Context(Role ro,User us)
        {
            role = ro.defpos;
            user = us.defpos;
        }
        /// <summary>
        /// Make a new subcontext, e.g. during SQL parsing
        /// </summary>
        /// <param name="cx">The context</param>
        internal Context(string i, Ident.IDType tp = Ident.IDType.NoInput, Ident n = null, Domain dt = null)
        {
            blockid = i;
            alias = n ?? new Ident(blockid,0);
            alias.blockid = blockid;
            cxType = tp;
            nominalDataType = dt ?? Domain.Value;
        }
        internal Context(Context cx)
        {
            defs = cx.defs;
            exec = cx.exec;
            refs = cx.refs;
            lookup = cx.lookup;
            blockid = cx.blockid;
            alias = cx.alias;
            cxType = cx.cxType;
            nominalDataType = cx.nominalDataType;
        }
        /// <summary>
        /// Simply add a pair
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns>whether we made a change</returns>
        bool _AddMatchedPair(long a, long b)
        {
            if (!matching[a].Contains(b))
            {
                var c = matching[a] ?? BTree<long,bool>.Empty;
                ATree<long, bool>.Add(ref c, b,true);
                ATree<long, ATree<long, bool>>.Add(ref matching, a, c);
                return true;
            }
            return false;
        }
        /// <summary>
        /// Ensure Match relation is transitive after adding a pair
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        internal void AddMatchedPair(long a,long b)
        {
            if (_AddMatchedPair(a, b))
            {
                var more = true;
                while (more)
                {
                    more = false;
                    for (var c = matching.First(); c != null; c = c.Next())
                        for (var d = matching[c.key()].First(); d != null; d = d.Next())
                            if (_AddMatchedPair(c.key(),d.key()) || _AddMatchedPair(d.key(),c.key()))
                                more = true;
                }
            }
        }
        /// <summary>
        /// We do not explore transitivity! Put extra pairs in for multiple matches.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        internal void AddMatchedPair(SqlValue a, SqlValue b)
        {
            if (matching[a.sqid].Contains(b.sqid))
                return;
            if (a.name.segpos > 0 && b.name.segpos > 0)
            {
                AddMatchedPair(a.sqid, b.sqid);
                AddMatchedPair(b.sqid, a.sqid);
            }
        }
        /// <summary>
        /// Update a variable in this context
        /// </summary>
        /// <param name="n">The variable name</param>
        /// <param name="val">The new value</param>
        /// <returns></returns>
        internal virtual TypedValue Assign(Transaction tr, Ident n, TypedValue val)
        {
            return val;
        }
        /// <summary>
        /// If there is a handler for No Data signal, raise it
        /// </summary>
        internal virtual void NoData(Transaction tr)
        {
            // no action
        }
        internal virtual int? FillHere(Ident n)
        {
            return null;
        }
        /// <summary>
        /// Type information for the SQL standard Diagnostics area: 
        /// see Tables 30 and 31 of the SQL standard 
        /// </summary>
        /// <param name="w">The diagnostic identifier</param>
        /// <returns></returns>
        public static Domain InformationItemType(Sqlx w)
        {
            switch (w)
            {
                case Sqlx.COMMAND_FUNCTION_CODE: return Domain.Int;
                case Sqlx.DYNAMIC_FUNCTION_CODE: return Domain.Int;
                case Sqlx.NUMBER: return Domain.Int;
                case Sqlx.ROW_COUNT: return Domain.Int;
                case Sqlx.TRANSACTION_ACTIVE: return Domain.Int; // always 1
                case Sqlx.TRANSACTIONS_COMMITTED: return Domain.Int; 
                case Sqlx.TRANSACTIONS_ROLLED_BACK: return Domain.Int; 
                case Sqlx.CONDITION_NUMBER: return Domain.Int;
                case Sqlx.MESSAGE_LENGTH: return Domain.Int; //derived from MESSAGE_TEXT 
                case Sqlx.MESSAGE_OCTET_LENGTH: return Domain.Int; // derived from MESSAGE_OCTET_LENGTH
                case Sqlx.PARAMETER_ORDINAL_POSITION: return Domain.Int;
            }
            return Domain.Char;
        }
        internal virtual SelectQuery SelQuery()
        {
            for (var q = cur; q != null; q = q.enc)
                if (q is SelectQuery s)
                    return s;
            return null;
        }
        internal virtual QuerySpecification QuerySpec(Transaction tr)
        {
            for (var q = cur; q != null; q = q.enc)
                if (q is QuerySpecification s)
                    return tr.queries[s.blockid] as QuerySpecification;
            return null;
        }
     }
    /// <summary>
    /// A Target class can be used for the left-hand side of an assignment.
    /// (An LValue in compiler-speak)
    /// </summary>
    internal class Target : ITypedValue
    {
        public string blockid = null;
        /// <summary>
        /// The name of the object being assigned
        /// </summary>
        public long vid;
        /// <summary>
        /// The data type expected
        /// </summary>
        public Domain dataType;
        public bool IsNull => throw new NotImplementedException();
        public Target(Activation.Variable v)
        {
            blockid = v.blockid;
            vid = v.vid;
            dataType = v.nominalDataType;
        }
        /// <summary>
        /// Alas, nm might not be in fm.nominalDataType, so we need more subtlety
        /// </summary>
        /// <param name="fm"></param>
        /// <param name="nm"></param>
        protected Target(Transaction t, From fm, Ident nm)
        {
            if (fm.names.Contains(nm))
            {
                var i = fm.names[nm];
                blockid = fm.blockid;
                name = fm.names[i]; // gets defpos info
                dataType = fm.cols[iq.Value].nominalDataType; 
            } else
                blockid = t.context.blockid;
        }
        // subtarget
        public Target(Target tg,Ident sn,Domain dt=null)
        {
            blockid = tg.blockid;
            dataType = dt??Domain.Content;
            name = Ident.Append(tg.name, sn);
        }
        /// <summary>
        /// Constructor: a target for assignment to a field in a structuredactivation
        /// </summary>
        /// <param name="t">The transaction</param>
        /// <param name="dt">The data type of the structured type</param>
        /// <param name="n">The name of the field</param>
        public Target(Transaction t,Domain dt, Ident n)
        {
            blockid = t.context.blockid;
            name = n;
            dataType = Domain.Content; // tolerate an unknown field for documents
            var i = dt.names[n];
            if (i.HasValue)
            {
                dataType = dt.columns[i.Value];
                n.Set(t,dt.names[i.Value]);
            }
        }
        protected Target(Target t, ref ATree<long, SqlValue> vs)
        {
            blockid = t.blockid;
            name = t.name;
            dataType = t.dataType;
        }
        internal virtual Target Copy(ref ATree<long, SqlValue> vs)
        {
            return new Target(this,ref vs);
        }
        internal virtual void Setup(Transaction tr)
        {
        }
        /// <summary>
        /// Assign the given RHS to the target
        /// </summary>
        /// <param name="v">The RHS value</param>
        /// <returns>The value</returns>
        internal virtual TypedValue Assign(Transaction tr,TypedValue v)
        {
            return tr.Ctx(blockid)?.Assign(tr, name, v);
        }
        /// <summary>
        /// Evaluate the target
        /// </summary>
        /// <returns>The value</returns>
        internal virtual TypedValue Eval(Transaction tr)
        {
            var cx = tr.context.cur?.Ctx(name) ?? tr.context.Ctx(name);
            if (cx is Query q)
                return q.row?.Get(name);
            return (cx as Activation).vars[name].Eval(tr,null)[name.sub];
        }
        public override string ToString()
        {
            return name?.ToString() ?? "Target";
        }

        internal virtual Selector GetColumn(Context cnx,Database database)
        {
            if (name.Defpos() > 0)
                return database.GetObject(name.Defpos()) as Selector;
            if (cnx.Ctx(blockid) is From f && f.target is Table tb)
                return tb.GetColumn(cnx,database,name);
  //          if (ctx is CalledActivation ac && database.GetObject(ac.alias?.Defpos()??-1) is Table ta)
   //             return ta.GetColumn(database,name); needs work
            throw new NotImplementedException();
        }
        public virtual int _CompareTo(object obj)
        {
            if (obj is Target that)
            {
                var c = blockid.CompareTo(that.blockid);
                if (c != 0)
                    return c;
                return name.CompareTo(that.name);
            }
            return 1;
        }
    }
    internal class LevelTarget : Target
    {
        public LevelTarget(Context cnx) : base(cnx, new Ident("LEVEL", 0), Domain.Level)
        { }
    }
    internal class FieldTarget : Target
    {
        public Target what;
        public Ident fname;
        public FieldTarget(Transaction tr,Target tg,Ident fn) : base(tr,_Type(tg.dataType,fn),new Ident(tg.name,fn))
        {
            what = tg; fname = fn;
        }
        static Domain _Type(Domain dt,Ident fn)
        {
            var iq = dt.names[fn];
            return dt[iq.Value];
        }
        internal override TypedValue Assign(Transaction tr, TypedValue v)
        {
            return what.Assign(tr, what.Eval(tr).Update((Transaction)tr, fname, v));
        }
        internal override TypedValue Eval(Transaction tr)
        {
            return what.Eval(tr)[fname];
        }
    }
    internal class SubscriptedTarget : Target
    {
        /// <summary>
        /// The array target (don't just use the base)
        /// </summary>
        public Target what;
        /// <summary>
        /// a subscript expression
        /// </summary>
        public SqlValue expr; 
        public SubscriptedTarget(Target tg,SqlValue sub) : base(tg,null,tg.dataType.elType)
        {
            if (dataType == null)
                throw new DBException("22005O", "Collection expected");
            what = tg;
            expr = sub;
        }
        protected SubscriptedTarget(SubscriptedTarget s, ref ATree<long, SqlValue> vs) :base(s,ref vs)
        {
            what = s.what.Copy(ref vs);
            expr = s.expr.Copy(ref vs);
        }
        internal override Target Copy(ref ATree<long, SqlValue> vs)
        {
            return new SubscriptedTarget(this,ref vs);
        }
        public override int _CompareTo(object obj)
        {
            var c = base._CompareTo(obj);
            if (c != 0)
                return c;
            if (obj is SubscriptedTarget tgt)
                return expr.Eval(null)?._CompareTo(tgt.expr.Eval(null)) ?? -1;
            return 1;
        }
        internal override void Setup(Transaction tr)
        {
            what.Setup(tr);
            var nd = BTree<SqlValue, Ident>.Empty;
            SqlValue.Setup(tr,tr.Ctx(blockid) as Query,expr, Domain.Int);
        }
        internal override TypedValue Assign(Transaction tr,TypedValue v)
        {
            if (what.Eval(tr) is TArray a && expr.Eval(tr,(tr.Ctx(blockid) as Query)?.rowSet) is TInt s && s.ToInt().HasValue)
            {
                a[s.ToInt().Value] = v;
                return a;
            }
            throw new DBException("22005P", what.ToString());
        }
        internal override Selector GetColumn(Transaction tr,Database database)
        {
            return what.GetColumn(tr,database);
        }
    }
    internal class TransitionTarget : Target
    {
        public bool old;
        From from;
        public TransitionTarget(Transaction tr, From fm,bool ol,Ident nm) : base(tr,nm,fm.nominalDataType)
        {
            from = fm; old = ol;
        }
        protected TransitionTarget(TransitionTarget t, ref ATree<long, SqlValue> vs) :base(t,ref vs)
        {
            old = t.old;
            var cs = BTree<string, Context>.Empty;
            from = (From)t.from.Copy(ref cs,ref vs);
        }
        internal override Target Copy(ref ATree<long, SqlValue> vs)
        {
            return new TransitionTarget(this,ref vs);
        }
        internal override TypedValue Assign(Transaction tr,TypedValue v)
        {
            if (old)
                ATree<long, TypedValue>.Add(ref from.oldRow, name.Defpos(), v);
            else
                ATree<long, TypedValue>.Add(ref from.newRow, name.Defpos(), v);
            return v;
        }
        public override int _CompareTo(object obj)
        {
            var c = base._CompareTo(obj);
            if (c != 0)
                return c;
            if (obj is TransitionTarget tgt)
            {
                c = from.cxid.CompareTo(tgt.from.cxid);
                return (c != 0) ? c : (old == tgt.old) ? 0 : old ? 1 : -1;
            }
            return 1;
        }
    }
    /// <summary>
    /// A period specification occurs in a table reference: AS OF/BETWEEN/FROM
    /// </summary>
    internal class PeriodSpec
    {
        /// <summary>
        /// the name of the period
        /// </summary>
        public string periodname = "SYSTEM_TIME";
        /// <summary>
        /// AS, BETWEEN, SYMMETRIC, FROM
        /// </summary>
        public Sqlx kind = Sqlx.NO;  
        /// <summary>
        /// The first point in time specified
        /// </summary>
        public SqlValue time1 = null;
        /// <summary>
        /// The second point in time specified
        /// </summary>
        public SqlValue time2 = null;
        /// <summary>
        /// Whether we have set this up
        /// </summary>
        bool setupDone = false;
        /// <summary>
        /// Set up the specification expressions
        /// </summary>
        public void Setup(Transaction tr,Query q)
        {
            if (setupDone)
                return;
            SqlValue.Setup(tr,q,time1,Domain.UnionDate);
            SqlValue.Setup(tr,q,time2,Domain.UnionDate);
            if (time2==null)
                time2 = time1;
            setupDone = true;
        }
    }
    /// <summary>
    /// A Period Version class
    /// </summary>
    internal class PeriodVersion : IComparable
    {
        internal Sqlx kind; // NO, AS, BETWEEN, SYMMETRIC or FROM
        internal DateTime time1;
        internal DateTime time2;
        internal long indexdefpos;
        /// <summary>
        /// Constructor: a Period Version
        /// </summary>
        /// <param name="k">NO, AS, BETWEEN, SYMMETRIC or FROM</param>
        /// <param name="t1">The start time</param>
        /// <param name="t2">The end time</param>
        /// <param name="ix">The index</param>
        internal PeriodVersion(Sqlx k,DateTime t1,DateTime t2,long ix)
        { kind = k; time1 = t1; time2 = t2; indexdefpos = ix; }
        /// <summary>
        /// Compare versions
        /// </summary>
        /// <param name="obj">another PeriodVersion</param>
        /// <returns>-1, 0, 1</returns>
        public int CompareTo(object obj)
        {
            var that = obj as PeriodVersion;
            var c = kind.CompareTo(that.kind);
            if (c != 0)
                return c;
            c = time1.CompareTo(that.time1);
            if (c != 0)
                return c;
            c = time2.CompareTo(that.time2);
            if (c != 0)
                return c;
            return indexdefpos.CompareTo(that.indexdefpos);
        }
    }
}
