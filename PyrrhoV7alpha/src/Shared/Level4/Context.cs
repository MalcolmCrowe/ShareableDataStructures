using System;
using System.Text;
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
    /// Contexts provide a Lookup method for obtaining a TypedValue for a SqlValue defpos in scope;
    /// Queries uses a Lookup0 method that can be safely called from Lookup during parsing, this should not be called separately.
    /// All Context information is volatile and scoped to within the current transaction,to which it maintains a pointer.
    /// </summary>
    internal class Context 
	{
        static int _dbg;
        readonly int dbg = ++_dbg;
        public readonly long cxid;
        public readonly User user;
        internal Context next,parent=null; // contexts form a stack (by nesting or calling)
        /// <summary>
        /// The current set of values of objects in the Context
        /// </summary>
        internal TRow row = null; // row.values
        internal bool rawCols = false;
        public BTree<long,RowSet> data = BTree<long,RowSet>.Empty;
        public long top,frame,result;
        public BList<Rvv> affected = BList<Rvv>.Empty;
        internal RowBookmark rb = null; // 
        internal ETag etag = null;
        internal BTree<long, FunctionData> func = BTree<long, FunctionData>.Empty;
        internal BTree<long, DBObject> obs = BTree<long, DBObject>.Empty; 
        internal BTree<int, BTree<long, DBObject>> depths = BTree<int, BTree<long, DBObject>>.Empty;
        internal BTree<long, TypedValue> values = BTree<long, TypedValue>.Empty;
        /// <summary>
        /// Left-to-right accumulation of definitions during a parse: accessed only by Query
        /// </summary>
        internal Ident.Idents defs = Ident.Idents.Empty;
        // unresolved SqlValues
        internal BTree<long, SqlValue> undef = BTree<long,SqlValue>.Empty;
        // used SqlColRefs by From.defpos
        internal BTree<long,BTree<long,SqlCol>> used = BTree<long, BTree<long, SqlCol>>.Empty;
        internal BTree<long,BTree<long, TypedValue>> matches = BTree<long,BTree<long, TypedValue>>.Empty;
        /// <summary>
        /// Used in Replace cascade
        /// </summary>
        internal BTree<long, DBObject> done = BTree<long, DBObject>.Empty;
        /// <summary>
        /// Used for executing prepared statements (a queue of SqlLiterals)
        /// </summary>
        internal BList<SqlValue> qParams = BList<SqlValue>.Empty;
        /// <summary>
        /// The current or latest statement
        /// </summary>
        public Executable exec = null;
        /// <summary>
        /// local syntax namespace defined in XMLNAMESPACES or elsewhere,
        /// indexed by prefix
        /// </summary>
        internal BTree<string, string> nsps = BTree<string, string>.Empty;
        public Context breakto;
        /// <summary>
        /// The return value: cf proc.ret
        /// </summary>
        public TypedValue ret = TNull.Value;
        /// <summary>
        /// Used for View processing: lexical positions of ends of columns
        /// </summary>
        internal BList<Ident> viewAliases = BList<Ident>.Empty;
        internal ExecuteStatus parse = ExecuteStatus.Obey;
        internal BTree<long, ReadConstraint> rdC = BTree<long, ReadConstraint>.Empty; // copied to and from Transaction
  //      internal BTree<CList<long>, BTree<CList<Domain>,Domain>> domains = 
  //          BTree<CList<long>,BTree<CList<Domain>,Domain>>.Empty; // copied to and from Transaction
        public int rconflicts =0, wconflicts=0;
        /// <summary>
        /// Create an empty context for the transaction 
        /// </summary>
        /// <param name="t">The transaction</param>
        internal Context(Database db)
        {
            next = null;
            user = db.user;
            cxid = db.lexeroffset;
            frame = top = db.uid;
            result = -1;
            rdC = (db as Transaction)?.rdC;
//            domains = (db as Transaction)?.domains;
        }
        internal Context(Context cx)
        {
            next = cx;
            user = cx.user;
            cxid = top = frame = cx.top+1;
            values = cx.values;
            obs = cx.obs;
            defs = cx.defs;
            depths = cx.depths;
            data = cx.data;
            parent = cx.parent; // for triggers
            // and maybe some more?
        }
        internal Context(Context c,Executable e) :this(c)
        {
            exec = e;
        }
        internal Context(Context c,Role r,User u) :this(c)
        {
            user = u;
        }
        internal Context Add(Query q,RowBookmark r)
        {
            if (q!=null)
                values += (q.defpos, r.row);
            for (var b = r.row.info.columns.First(); b != null; b = b.Next())
                values += (b.value().defpos, r.row[b.key()]);
            row = r.row;
            rb = r;
            return this;
        }
        internal Context Add(BTree<long,TypedValue>fl)
        {
            for (var b = fl.First(); b != null; b = b.Next())
                values += (b.key(), b.value());
            return this;
        }
        internal Context Add(DBObject ob,ObInfo oi)
        {
            var on = new Ident(oi.name, ob.defpos);
            defs += (on, ob);
            for (var b = oi.columns?.First(); b != null; b = b.Next())
            {
                var sc = b.value();
                var sn = new Ident(sc.name, sc.defpos);
                defs += (new Ident(on, sn), sc);
                defs += (sn, sc);
            }
            return this;
        }
        internal DBObject Add(DBObject ob)
        {
            if (ob is ObInfo)
            {
                Console.WriteLine("Attempt to add ObInfo to Context");
                return ob;
            }
            if (obs[ob.defpos] is DBObject oo && oo.depth != ob.depth)
            {
                var de = depths[oo.depth];
                depths += (oo.depth, de - ob.defpos);
            }
            if (ob != null && ob.defpos != -1)
            {
                obs += (ob.defpos, ob);
                if (ob.mem.Contains(DBObject._Alias))
                    defs += (new Ident(ob.alias, 0), ob);
                var dp = depths[ob.depth] ?? BTree<long, DBObject>.Empty;
                depths += (ob.depth, dp + (ob.defpos, ob));
            }
            return ob;
        }
        /// <summary>
        /// Context undef is used for undefined SqlColRefs in QuerySpec
        /// and incomplete SqlColRefs in From
        /// </summary>
        /// <param name="qdef"></param>
        /// <param name="colpos"></param>
        internal void Unresolved(SqlValue sc)
        {
            undef += (sc.defpos,sc);
        }
        internal DBObject Add(Transaction tr,long p)
        {
            return Add((DBObject)tr.objects[p]);
        }
        /// <summary>
        /// Update the Query processing context in a cascade to implement a single replacement!
        /// </summary>
        /// <param name="was"></param>
        /// <param name="now"></param>
        internal DBObject Replace(DBObject was, DBObject now)
        {
            Add(now);
            if (was == now)
                return now;
            done = new BTree<long, DBObject>(was.defpos, now);
            // scan by depth to perform the replacement
            for (var b = depths.Last(); b != null; b = b.Previous())
            {
                var bv = b.value();
                for (var c = bv.First(); c != null; c = c.Next())
                {
                    var cv = c.value().Replace(this, was, now); // may update done
                    if (cv != c.value())
                        bv += (c.key(), cv);
                }
                if (bv != b.value())
                    depths += (b.key(), bv);
            }
            // now use the done list to update defs
            defs = defs.Replace(done);
            for (var b = done.First(); b != null; b = b.Next())
                obs += (b.key(), b.value());
            return now;
        }
        internal long Fix(long dp)
        {
            if (done[dp] is DBObject ob)
                return ob.defpos;
            return dp;
        }
        /// <summary>
        /// We have just constructed a new From. Use it to replace any
        /// matching unresolved table references.
        /// </summary>
        /// <param name="f"></param>
        internal void TableRef(From f)
        {
            done = BTree<long, DBObject>.Empty;
            if (!defs.Contains(f.name))
                defs += (new Ident(f.name, 0), f);
            for (var b = depths.Last(); b != null; b = b.Previous())
            {
                var bv = b.value();
                for (var c = bv.First(); c != null; c = c.Next())
                {
                    var co = c.value();
                    var cv = co?.TableRef(this, f);
                    if (co!=cv && cv!=null)
                        bv += (c.key(), cv);
                }
                if (bv != b.value())
                    depths += (b.key(), bv);
            }
            for (var b = done.First(); b != null; b = b.Next())
                obs += (b.key(), b.value());
        }
        internal long AddUidRange(int n)
        {
            var r = top;
            top += n;
            return r;
        }
        /// <summary>
        /// Update a variable in this context
        /// </summary>
        /// <param name="n">The variable name</param>
        /// <param name="val">The new value</param>
        /// <returns></returns>
        internal virtual TypedValue Assign(Transaction tr, Context cx, long dp, TypedValue val)
        {
            row.values += (dp, val);
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
        internal virtual TRow Eval()
        {
            return null;
        }
        internal virtual Context Ctx(Ident n)
        {
            return null;
        }
        internal Context Ctx(long bk)
        {
            for (var cx = this; cx != null; cx = cx.next)
                if (cx.cxid == bk)
                    return cx;
            return null;
        }
        internal Activation GetActivation()
        {
            for (var c = this; c != null; c = c.next)
                if (c is Activation ac)
                    return ac;
            return null;
        }
        internal virtual void SlideDown(Context was)
        {
            ret = was.ret;
        }
        // debugging
        public override string ToString()
        {
            return "Context " + cxid;
        }

        internal virtual TriggerActivation FindTriggerActivation(long tabledefpos)
        {
            return next?.FindTriggerActivation(tabledefpos)
                ?? throw new PEException("PE600");
        }
    }
    internal class FunctionData
    {
        // all the following data is set (in the Context) during computation of this WindowSpec
        // The base RowSet should be a ValueRowSet to support concurrent enumeration
        /// <summary>
        /// Set to true for a build of the wrs collection
        /// </summary>
        internal bool building = false;
        internal bool valueInProgress = false;
        /// <summary>
        /// used for multipass analysis of window specifications
        /// </summary>
        internal bool done = false;
        /// <summary>
        /// list of indexes of TableColumns for this WindowSpec
        /// </summary>
        internal BList<bool> cols = BList<bool>.Empty;
        internal Register cur = new Register();
        internal TMultiset dset = null;
        internal CTree<TRow, Register> regs = new CTree<TRow, Register>(Domain.Null);
        internal class Register
        {
            /// <summary>
            /// The current partition: type is window.partitionType
            /// </summary>
            internal TRow profile = null;
            /// <summary>
            /// The RowSet for helping with evaluation if window!=null.
            /// Belongs to this partition, computed at RowSets stage of analysis 
            /// for our enclosing parent QuerySpecification (source).
            /// </summary>
            internal OrderedRowSet wrs = null;
            /// <summary>
            /// The bookmark for the current row
            /// </summary>
            internal RowBookmark wrb = null;
            /// the result of COUNT
            /// </summary>
            internal long count = 0L;
            /// <summary>
            /// the results of MAX, MIN, FIRST, LAST, ARRAY
            /// </summary>
            internal TypedValue acc;
            /// <summary>
            /// the results of XMLAGG
            /// </summary>
            internal StringBuilder sb = null;
            /// <summary>
            ///  the sort of sum/max/min we have
            /// </summary>
            internal Domain sumType = Domain.Content;
            /// <summary>
            ///  the sum of long
            /// </summary>
            internal long sumLong;
            /// <summary>
            /// the sum of INTEGER
            /// </summary>
            internal Integer sumInteger = null;
            /// <summary>
            /// the sum of double
            /// </summary>
            internal double sum1, acc1;
            /// <summary>
            /// the sum of Decimal
            /// </summary>
            internal Common.Numeric sumDecimal;
            /// <summary>
            /// the boolean result so far
            /// </summary>
            internal bool bval;
            /// <summary>
            /// a multiset for accumulating things
            /// </summary>
            internal TMultiset mset = null;
            internal void Clear()
            {
                count = 0; acc = TNull.Value; sb = null;
                sumType = Domain.Content; sumLong = 0;
                sumInteger = null; sum1 = 0.0; acc1 = 0.0;
                sumDecimal = Numeric.Zero;
            }
        }


    }

    internal class WindowData
    {

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
