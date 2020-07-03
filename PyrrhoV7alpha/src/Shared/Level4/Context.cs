using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

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
        static long _cxid;
        internal static Context _system;
        internal readonly long cxid = ++_cxid;
        public readonly int dbformat = 51;
        public User user => db.user;
        public Role role => db.role;
        internal Context next, parent = null; // contexts form a stack (by nesting or calling)
        /// <summary>
        /// The current set of values of objects in the Context
        /// </summary>
        internal bool rawCols = false;
        public BTree<long, Cursor> cursors = BTree<long, Cursor>.Empty;
        internal BTree<long, RowSet> data = BTree<long, RowSet>.Empty;
        internal BTree<long, RowSet.Finder> from = BTree<long, RowSet.Finder>.Empty; 
        public long nextHeap, nextStmt, parseStart, srcFix;
        public TypedValue val = TNull.Value;
        internal BTree<long, ETag> etag = BTree<long, ETag>.Empty;
        internal Database db = null;
        internal Transaction tr => db as Transaction;
        internal BTree<long, BTree<long, bool>> copy = BTree<long, BTree<long, bool>>.Empty;
        internal BTree<long, DBObject> obs = BTree<long, DBObject>.Empty;
        internal class Framing // for compiled code
        {
            public BTree<long, DBObject> obs;
            public Ident.Idents defs;
            public BTree<int, BTree<long, DBObject>> depths;
        } 
        internal Framing frame = null;
        protected BTree<long, Domain> domains = BTree<long, Domain>.Empty; 
        internal BTree<int, BTree<long, DBObject>> depths = BTree<int, BTree<long, DBObject>>.Empty;
        internal BTree<long, TypedValue> values = BTree<long, TypedValue>.Empty;
        internal BTree<long, Register> funcs = BTree<long, Register>.Empty; // volatile
        internal BTree<long, BTree<long,TableRow>> newTables = BTree<long, BTree<long,TableRow>>.Empty;
        /// <summary>
        /// Left-to-right accumulation of definitions during a parse: accessed only by Query
        /// </summary>
        internal Ident.Idents defs = Ident.Idents.Empty;
        internal bool constraintDefs = false;
        /// <summary>
        /// Lexical positions to DBObjects (if dbformat<51)
        /// </summary>
        public BTree<long, (string, long)> digest = BTree<long, (string, long)>.Empty;
        // UnHeap things for Procedure, Trigger, and Constraint bodies
        internal BTree<long, long> uids = BTree<long, long>.Empty;
        // used SqlColRefs by From.defpos
        internal BTree<long, BTree<long, SqlValue>> used = BTree<long, BTree<long, SqlValue>>.Empty;
        /// <summary>
        /// Used in Replace cascade
        /// </summary>
        internal BTree<long, DBObject> done = BTree<long, DBObject>.Empty;
        /// <summary>
        /// Used for executing prepared statements (a queue of SqlLiterals)
        /// </summary>
        internal BList<SqlValue> qParams = BList<SqlValue>.Empty;
        /// <summary>
        /// Helpers for SelectList parsing.
        /// If selectlist source has item.* or * at position pos
        /// the stars has 
        /// (pos,item.defpos or -1L)
        /// and From constructor splices in items as appropriate.
        /// We watch out for possible SelectList nesting.
        /// </summary>
        internal BList<(int, long)> stars = BList<(int, long)>.Empty;
        internal int selectLength = 0;
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
        public int rconflicts = 0, wconflicts = 0;
        /// <summary>
        /// Create an empty context for the transaction 
        /// </summary>
        /// <param name="t">The transaction</param>
        internal Context(Database db)
        {
            next = null;
            cxid = db.lexeroffset;
            nextHeap = db.nextPrep;
            nextStmt = db.nextStmt;
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
            nextHeap = cx.nextHeap;
            nextStmt = cx.nextStmt;
            parseStart = cx.parseStart;
            values = cx.values;
            obs = cx.obs;
            defs = cx.defs;
            depths = cx.depths;
            domains = cx.domains;
            copy = cx.copy;
            data = cx.data;
            from = cx.from;
            cursors = cx.cursors;
            etag = cx.etag;
            val = cx.val;
            parent = cx.parent; // for triggers
            dbformat = cx.dbformat;
            rdC = cx.rdC;
            // and maybe some more?
        }
        internal Context(Context c, Executable e) : this(c)
        {
            exec = e;
        }
        internal Context(Context c, Role r, User u) : this(c)
        {
            db = db + (Database.Role, r.defpos) + (Database.User,u.defpos);
        }
        /// <summary>
        /// During Parsing
        /// </summary>
        /// <param name="ic"></param>
        /// <param name="xp"></param>
        /// <returns></returns>
        internal DBObject Get(Ident ic, long xp)
        {
            DBObject v;
            if (ic.Length > 0 && defs.Contains(ic.ToString())
                    && obs[defs[ic.ToString()].Item1] is SqlValue s0)
                v = s0;
            else
                v = obs[defs[ic]];
            if (v != null && !Dom(xp).CanTakeValueOf(this,Dom(v.defpos)))
                throw new DBException("42000", ic);
            return v;
        }
        internal long GetUid(int n)
        {
            long r;
            if (db is Transaction)
            {
                r = nextHeap;
                nextHeap += n;
            } else
            {
                r = nextStmt;
                nextStmt += n;
            }
            return r;
        }
        // Sabotaged in order to do GetUid for Trigger, Procedure and Check bodies.
        // At the end of parsing, heap uids are changed to nextStmt uids:
        // these will be in the Analysing range within Transactions,
        // and the physical range on Load. 
        // See UnHeap() and DBObject.UnHeap()
        internal void SrcFix(long pp)
        {
            if (db is Transaction)
                srcFix = db.lexeroffset;
            else
                srcFix = pp;
        }
        internal long GetUid()
        {
            return nextHeap++;
        }
        internal long Unheap(long p)
        {
            if (p < Transaction.Heap)
                return p;
            if (uids.Contains(p))
                return uids[p];
            while (obs.Contains(srcFix))
                srcFix++;
            uids += (p, srcFix);
            return srcFix;
        }
        internal int Depth(BList<long> os)
        {
            var r = 1;
            for (var b=os.First();b!=null;b=b.Next())
            {
                var ob = obs[b.value()];
                if (ob.depth >= r)
                    r = ob.depth + 1;
            }
            return r;
        }
        internal Domain _Dom(long xp)
        {
            if (xp == -1)
                return Domain.Content;
            if (xp < 0)
            {
                var so = (DBObject)Database._system.objects[xp];
                if (so is Domain sd)
                    return sd;
                return so.domain;
            }
            if (obs[xp] is SqlValue sv)
                return sv.domain;
            if (domains[xp] is Domain dm)
                return dm;
            if (obs[xp] is Query q)
                return q.domain;
            if (obs[xp] is Procedure pr)
                return pr.domain;
            if (db.objects[xp] is DBObject ob)
            {
                if (ob is Domain od)
                    return od;
                return ob.domain;
            }
            return null;
        }
        internal Domain Dom(long xp)
        {
            return _Dom(xp) ??
            throw new PEException("PE200");
        }
        internal ObInfo Inf(long dp)
        {
            if ((dp<-1 || (dp >= 0 && dp < Transaction.Analysing)) && role.infos.Contains(dp))
                return (ObInfo)(role.infos[dp]);
            return null;
        }
        internal CList<long> Cols(long dp)
        {
            return obs[dp]._Cols(this);
        }
        internal BTree<long, SqlValue> Map(BList<long> s)
        {
            var r = BTree<long, SqlValue>.Empty;
            for (var b = s.First(); b != null; b = b.Next())
            {
                var p = b.value();
                r += (p, (SqlValue)obs[p]);
            }
            return r;
        }
        internal BList<SqlValue> Pick(BList<long> s)
        {
            var r = BList<SqlValue>.Empty;
            for (var b = s.First(); b != null; b = b.Next())
                r += (SqlValue)obs[b.value()];
            return r;
        }
        internal virtual void AddValue(DBObject s, TypedValue tv)
        {
            if (tv is Cursor || tv is RowSet)
                Console.WriteLine("AddValue??");
            s.Set(this, tv);
            var p = s.defpos;
            var f = from[p];
            if (from.Contains(p) && cursors[f.rowSet] is Cursor cu)
                cursors+=(f.rowSet,cu + (this, f.col, tv));
        }
        internal void Add(Physical ph, long lp = 0)
        {
            if (lp == 0)
                lp = db.loadpos;
            db.Add(this, ph, lp);
        }
        internal void Inf(BTree<long,ObInfo> infos)
        {
            for (var b = infos?.First();b!=null;b=b.Next())
            {
                var oi = b.value();
                defs += (oi.name, oi.defpos, Ident.Idents.Empty);
            }
        }
        internal Domain Add(Domain dm)
        {
            if (dm.defpos>=0)
                domains += (dm.defpos, dm);
            return dm;
        }
        internal DBObject Add(DBObject ob)
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
            if (obs[ob.defpos] is DBObject oo && oo.depth != ob.depth)
            {
                var de = depths[oo.depth];
                depths += (oo.depth, de - ob.defpos);
            }
            if (ob != null && ob.defpos != -1)
            {
                _Add(ob);
                var nm = ob.alias ?? (string)ob.mem[Basis.Name] ?? "";
                if (nm != "")
                {
                    var ic = new Ident(nm, ob.defpos);
                    // Careful: we don't want to overwrite an undefined Ident by an unreified one
                    // as it is about to be Resolved
                    var od = defs[ic];
                    if (od == -1L || !(ob.defpos >= Transaction.Heap && obs[od] is SqlValue ov
                        && ov.domain == Domain.Content))
                        defs += (ic, ob.defpos);
                }
            }
            return ob;
        }
        /// <summary>
        /// Add an object to the database/transaction and update the cache
        /// </summary>
        /// <param name="ob"></param>
        internal void Install(DBObject ob, long p)
        {
            db += (ob, p);
            obs += (ob.defpos, ob);
        }
        internal Context ForConstraintParse(BTree<long,ObInfo>ns)
        {
            // Set up the information for parsing the generation rule
            // The table domain and cx.defs should contain the columns so far defined
            var cx = new Context(this);
            cx.constraintDefs = true;
            cx.Frame();
            var rs = BTree<long, Domain>.Empty;
            Ident ti = null;
            Table tb = null;
            for (var b = ns?.First(); b != null; b = b.Next())
            {
                var oi = b.value();
                var ic = new Ident(oi.name, b.key());
                if (oi.domain.kind == Sqlx.TABLE)
                {
                    ti = ic;
                    tb = (Table)db.objects[b.key()];
                }
                else if (ti!=null)
                {
                    cx.defs += (ic, b.key());
                    ic = new Ident(ti, ic);
                    rs += (b.key(), oi.domain);
                }
                cx.defs += (ic, b.key());
            }
            if (tb!=null)
             cx.Add(tb + (DBObject._Domain, tb.domain + (Domain.Representation, rs)));
            return cx;
        }
        /// <summary>
        ///  Not to be used for Domain or ObInfo
        /// </summary>
        /// <param name="pos"></param>
        /// <returns></returns>
        internal DBObject Fixed(long pos)
        {
            var p = Unheap(pos);
            if (p == pos)
                return obs[p];
            if (obs[p] is DBObject x)
                return x;
            var ob = obs[pos];
            if (pos > Transaction.TransPos)
            {
                ob = ob.Relocate(p).Relocate(this);
                obs -= pos;
                obs += (p, ob);
            }
            return ob;
        }
        internal void Frame()
        {
            if (frame != null)
                throw new PEException("PE401");
            frame = new Framing { obs = obs, defs = defs, depths = depths };
        }
        internal void Frame(long p,long q=-1L)
        {
            if (p < 0 || obs.Contains(p))
                return;
            if (q < 0)
                q = p;
            var ob = (DBObject)db.objects[q];
            if (ob is From fm)
                Console.WriteLine(fm.ToString());
            obs += (ob.defpos, ob);
            obs += (ob.framing,true);
        }
        internal DBObject _Add(DBObject ob,string alias=null)
        {
            if (ob != null && ob.defpos != -1)
            {
                ob._Add(this);
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
            _Add(now);
            done = new BTree<long, DBObject>(now.defpos,now);
            if (was.defpos != now.defpos)
                done += (was.defpos, now);
            // scan by depth to perform the replacement
            for (var b = depths.First(); b != null; b = b.Next())
            {
                var bv = b.value();
                for (var c = bv.First(); c != null; c = c.Next())
                {
                    var p = c.value();
                    var cv = p.Replace(this, was, now); // may update done
                    if (cv != p)
                        bv += (c.key(), cv);
                }
                if (bv != b.value())
                    depths += (b.key(), bv);
            }
            // now scan by depth to install the new versions
            for (var b = depths.First(); b != null; b = b.Next())
            {
                var bk = b.key();
                for (var c = done.First(); c != null; c = c.Next())
                {
                    var cv = c.value();
                    if (cv.depth==bk)
                        Add(cv);
                }
            }
            defs = defs.ApplyDone(this);
            if (was.defpos != now.defpos)
                _Remove(was.defpos);
            return now;
        }
        internal long Replace(long dp,DBObject was,DBObject now)
        {
            if (done.Contains(dp))
                return done[dp].defpos;
            return obs[dp]?._Replace(this,was, now)?.defpos??-1L;
        }
        internal DBObject _Replace(long dp, DBObject was, DBObject now)
        {
            if (done.Contains(dp))
                return done[dp];
            return obs[dp]?._Replace(this, was, now);
        }
        internal DBObject _Ob(long dp)
        {
            return done[dp] ?? obs[dp];
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
        internal void AddDefs(Domain ut,Database db)
        {
            for (var b = ut.representation.First();b != null; b = b.Next())
            {
                var p = b.key();
                var iv = Inf(p);
                defs += (iv.name, p, Ident.Idents.For(p,db,this));
            }
        }
        internal void AddDefs(Ident id, BList<long> s, bool force = false)
        {
  //          if ((!force) && (!constraintDefs) && obs[id.iix] is Table)
  //              return;
            for (var b = s?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var ob = obs[p] ?? (DBObject)db.objects[p];
   //             if ((!force) && (!constraintDefs) && (ob is Table || ob is TableColumn))
   //                 continue;
                var n = (ob is SqlValue v) ? v.name : Inf(p)?.name;
                if (n == null)
                    continue;
                var ic = new Ident(n, p);
                defs += (new Ident(id, ic), ob.defpos);
                defs += (ic, ob.defpos);
            }
        }
        internal void AddParams(Procedure pr)
        {
            var pi = new Ident(pr.name, 0);
            for (var b = pr.ins.First(); b != null; b = b.Next())
            {
                var pd = (FormalParameter)obs[b.value()];
                var pn = new Ident(pd.name, 0);
                defs += (pn, pd.defpos);
                defs += (new Ident(pi, pn), pd.defpos);
                Add(pd);
            }
        }
        internal void AddProc(Procedure pr)
        {
            var p = db.loadpos;
            var ro = role + pr;
            db = db + (ro,p)+(pr,p);
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
        internal TypedValue Eval(long dp,CList<long> s)
        {
            return new SqlRow(dp, this, -1, s).Eval(this);
        }
        internal PRow MakeKey(BList<long>s)
        {
            PRow k = null;
            for (var b = s.Last(); b != null; b = b.Previous())
                k = new PRow(obs[b.value()].Eval(this), k);
            return k;
        }
        internal Activation GetActivation()
        {
            for (var c = this; c != null; c = c.next)
                if (c is Activation ac)
                    return ac;
            return null;
        }
        internal virtual Context SlideDown()
        {
            return this;
        }
        // debugging
        public override string ToString()
        {
            return GetType().Name + " "+ cxid;
        }

        internal virtual TriggerActivation FindTriggerActivation(long tabledefpos)
        {
            return next?.FindTriggerActivation(tabledefpos)
                ?? throw new PEException("PE600");
        }
        internal string Obs
        {
            get
            {
                var sb = new StringBuilder();
                for (var b = obs.First(); b != null; b = b.Next())
                {
                    sb.Append(DBObject.Uid(b.key()));
                    sb.Append(": "); sb.Append(b.value().ToString(this, 0));
                    sb.Append(";");
                }
                return sb.ToString();
            }
        }
        internal string Data
        {
            get
            {
                var sb = new StringBuilder();
                for (var b = data.First(); b != null; b = b.Next())
                {
                    sb.Append(DBObject.Uid(b.key()));
                    sb.Append(": "); sb.Append(b.value().ToString(this, 0));
                    sb.Append(";");
                }
                return sb.ToString();
            }
        }
        internal string Cursors
        {
            get
            {
                var sb = new StringBuilder();
                for (var b = cursors.First(); b != null; b = b.Next())
                {
                    sb.Append(DBObject.Uid(b.key()));
                    sb.Append(": "); sb.Append(b.value());
                    sb.Append("\n\r");
                }
                return sb.ToString();
            }
        }
    }
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
