using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
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
    /// 
    /// All Context information is volatile and scoped to within the current transaction,for which it may contain a snapshot.
    /// Ideally Context should be immutable (a subclass of Basis), but we do not do this because SqlValue::Eval can affect
    /// the current ReadConstraint and ETag, and these changes must be placed somewhere. The Context is the simplest.
    /// </summary>
    internal class Context 
	{
        static int _dbg;
        readonly int dbg = ++_dbg;
        public readonly long cxid;
        public readonly int dbformat; 
        public User user;
        public Role role;
        internal Context next,parent=null; // contexts form a stack (by nesting or calling)
        /// <summary>
        /// The current set of values of objects in the Context
        /// </summary>
        internal bool rawCols = false;
        public BTree<long, Cursor> cursors = BTree<long, Cursor>.Empty;
        internal BTree<long, RowSet> data = BTree<long, RowSet>.Empty;
        internal BTree<long, long> from = BTree<long, long>.Empty; // SqlValue to cursors/data
        public long nextHeap, parseStart;
        public TypedValue val = TNull.Value;
        internal BTree<long,ETag> etag = BTree<long,ETag>.Empty;
        internal Database db = null;
        internal Transaction tr => db as Transaction;
        internal BTree<long, FunctionData> func = BTree<long,FunctionData>.Empty;
        internal BTree<long, BTree<long, bool>> copy = BTree<long, BTree<long, bool>>.Empty;
        internal BTree<long, DBObject> obs = BTree<long, DBObject>.Empty; 
        internal BTree<int, BTree<long, DBObject>> depths = BTree<int, BTree<long, DBObject>>.Empty;
        internal BTree<long, TypedValue> values = BTree<long, TypedValue>.Empty;
        /// <summary>
        /// Left-to-right accumulation of definitions during a parse: accessed only by Query
        /// </summary>
        internal Ident.Idents defs = Ident.Idents.Empty;
        /// <summary>
        /// Lexical positions to DBObjects (if dbformat<51)
        /// </summary>
        public BTree<long,(string,long)> digest = BTree<long,(string,long)>.Empty;
        // unresolved SqlValues
        internal BTree<long, SqlValue> undef = BTree<long,SqlValue>.Empty;
        // used SqlColRefs by From.defpos
        internal BTree<long,BTree<long,SqlValue>> used = BTree<long, BTree<long, SqlValue>>.Empty;
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
        /// <summary>
        /// Used for View processing: lexical positions of ends of columns
        /// </summary>
        internal BList<Ident> viewAliases = BList<Ident>.Empty;
        internal ExecuteStatus parse = ExecuteStatus.Obey;
        internal BTree<long, ReadConstraint> rdC = BTree<long, ReadConstraint>.Empty; // copied to and from Transaction
        public int rconflicts =0, wconflicts=0;
        /// <summary>
        /// Create an empty context for the transaction 
        /// </summary>
        /// <param name="t">The transaction</param>
        internal Context(Database db)
        {
            next = null;
            user = db.user;
            role = db.role;
            cxid = db.lexeroffset;
            nextHeap = db.nextPrep;
            dbformat = db.format;
            parseStart = 0L;
            this.db = db;
            rdC = (db as Transaction)?.rdC;
//            domains = (db as Transaction)?.domains;
        }
        internal Context(Context cx)
        {
            next = cx;
            db = cx.db;
            user = cx.user;
            role = cx.role;
            nextHeap = cx.nextHeap;
            parseStart = cx.parseStart;
            values = cx.values;
            obs = cx.obs;
            defs = cx.defs;
            depths = cx.depths;
            copy = cx.copy;
            data = cx.data;
            from = cx.from;
            cursors = cx.cursors;
            etag = cx.etag;
            val = cx.val;
            parent = cx.parent; // for triggers
            dbformat = cx.dbformat;
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
        internal DBObject Get(Ident ic, ObInfo oi)
        {
            DBObject ob;
            if (ic.Length > 1 && defs.Contains(ic.ToString())
                    && defs[ic.ToString()].Item1 is SqlValue s0)
                ob = s0;
            else
                ob = defs[ic];
            if (ob != null && !oi.domain.CanTakeValueOf(ob.domain))
                throw new DBException("42000", ic);
            return ob;
        }
        internal virtual TypedValue AddValue(DBObject s,TypedValue tv)
        {
            if (tv is Cursor || tv is RowSet)
                Console.WriteLine("AddValue??");
            var p = s.Defpos();
            values += (p, tv);
            if (from.Contains(p) && cursors[from[p]] is Cursor cu)
                cu.values += (p, tv);
            return tv;
        }
        internal void Add(Physical ph, long lp=0)
        {
            if (lp == 0)
                lp = db.loadpos;
            db.Add(this, ph, lp);
        }
        internal DBObject Add(DBObject ob,bool framing=false)
        {
            long rp;
            if (dbformat < 51)
            {
                if (ob is SqlValue sv && (rp = sv.target) > 0 && sv.defpos > Transaction.TransPos
                    && sv.name != "" && rp< Transaction.TransPos)
                    digest += (sv.defpos, (sv.name, rp));
                else if (ob is From fm && (rp = fm.target) > 0 && fm.defpos > Transaction.TransPos
                    && fm.name != "" && rp < Transaction.TransPos)
                    digest += (fm.defpos, (fm.name, rp));
            }
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
                _Add(ob);
                var nm = ob.alias ?? (string)ob.mem[Basis.Name] ??"";
                if (nm!="" && !framing)
                    defs += (new Ident(nm, ob.defpos), ob);
            }
            if (framing && ob is Query q && !data.Contains(ob.defpos))
                data += (ob.defpos, q.RowSets(this));
            return ob;
        }
        internal DBObject _Add(DBObject ob)
        {
            if (ob != null && ob.defpos != -1)
            {
                obs += (ob.defpos, ob);
                var dp = depths[ob.depth] ?? BTree<long, DBObject>.Empty;
                depths += (ob.depth, dp + (ob.defpos, ob));
            }
            return ob;
        }
        internal void _Remove(long dp)
        {
            obs -= dp;
            for (var b=depths.First();b!=null;b=b.Next())
                depths += (b.key(), b.value() - dp);
        }
        internal void AddRowSetsPair(SqlValue a,SqlValue b)
        {
            if (data[a.from] is RowSet ra)
                data += (a.from, ra + (a.defpos, b.defpos));
            if (data[b.from] is RowSet rb)
                data += (a.from, rb + (a.defpos, b.defpos));
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
            if (dbformat<51)
                Add(now);
            if (was == now)
                return now;
            done = new BTree<long, DBObject>(was.defpos, now) +(now.defpos,now);
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
                Add(b.value());
            if (was.defpos != now.defpos)
                _Remove(was.defpos);
            return now;
        }
        internal long Fix(long dp)
        {
            if (done[dp] is DBObject ob)
                return ob.defpos;
            return dp;
        }
        /// <summary>
        /// Simply add a pair of equivalent uids
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        BTree<long,BTree<long,bool>> _Copy(BTree<long,BTree<long,bool>>co,long a, long b)
        {
            if (co[a]?.Contains(b) !=true)
            {
                var c = co[a] ?? BTree<long, bool>.Empty;
                c += (b, true);
                co += (a, c);
            }
            return co;
        }
        /// <summary>
        /// Ensure Copy relation is transitive after adding a pair
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        internal BTree<long,BTree<long,bool>> Copy(BTree<long, BTree<long, bool>> cp,long a, long b)
        {
            if (a == b)
                return cp;
            if (a < 0 || b < 0)
                throw new PEException("PE195");
            var co = cp;
            var r = _Copy(cp,a, b);
            for (; ; )
            {
                if (co == r)
                    return r;
                co = r;
                for (var c = co.First(); c != null; c = c.Next())
                    for (var d = co[c.key()].First(); d != null; d = d.Next())
                    {
                        r = _Copy(co,c.key(), d.key());
                        r = _Copy(co,d.key(), c.key());
                    }
            }
            // not reached
        }
        internal void Copy(long a, long b)
        {
            copy += Copy(copy, a, b);
        }
        internal BTree<long,BTree<long,bool>> Copy(Domain d,Domain e)
        {
            if (e == null)
                return null;
            if (d.Length != e.Length)
                throw new PEException("PE196");
            var r = copy;
            for (var j = 0; j < d.Length; j++)
                r = Copy(r, d.representation[j].Item1, e.representation[j].Item1);
            return r;
        }
        internal static BTree<long,BTree<long,bool>> Copy(BTree<SqlValue,BTree<SqlValue,bool>> mg)
        {
            var r = BTree<long, BTree<long, bool>>.Empty;
            for (var b=mg.First();b!=null;b=b.Next())
            {
                var s = BTree<long, bool>.Empty;
                for (var c = b.value().First(); c != null; c = c.Next())
                    s += (c.key().defpos, true);
                r += (b.key().defpos, s);
            }
            return r;
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
        internal void AddDefs(Domain ut,Database db)
        {
            for (var b = ((ObInfo)db.role.obinfos[ut.defpos]).columns.First();
                    b != null; b = b.Next())
            {
                var iv = b.value();
                defs += (iv.name, iv, Ident.Idents.For(iv,db,this));
                Add(iv);
            }
        }
        internal void AddParams(Procedure pr)
        {
            var pi = new Ident(pr.name, 0);
            for (var b = pr.ins.First(); b != null; b = b.Next())
            {
                var pp = b.value();
                var pn = new Ident(pp.name, 0);
                defs += (pn, pp);
                defs += (new Ident(pi, pn), pp);
                Add(pp);
            }
        }
        /// <summary>
        /// If there is a handler for No Data signal, raise it
        /// </summary>
        internal virtual void NoData()
        {
            // no action
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
            val = was.val;
            db = was.db;
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
        internal TMultiset dset = null;
        internal CTree<TRow, Register> regs;
        internal Register cur = null;
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
            internal RTree wrs = null;
            /// <summary>
            /// The bookmark for the current row
            /// </summary>
            internal Cursor wrb = null;
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
            internal bool bval = false;
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
                wrs = null;
            }
        }
        public FunctionData() { }
        public FunctionData(Sqlx kind) { regs = new CTree<TRow, Register>(kind); }
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
