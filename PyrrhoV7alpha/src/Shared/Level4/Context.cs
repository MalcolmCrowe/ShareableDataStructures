using System;
using System.Configuration;
using System.Diagnostics.Eventing.Reader;
using System.Runtime.ExceptionServices;
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
    /// 
    /// All Context information is volatile and scoped to within the current transaction,for which it may contain a snapshot.
    /// Ideally Context should be immutable (a subclass of Basis), but we do not do this because SqlValue::Eval can affect
    /// the current ReadConstraint and ETag, and these changes must be placed somewhere. The Context is the simplest.
    /// 
    /// So Context is mutable: but places we have the separate Contexts are important. 
    ///     1. During the UnLex relocations, with _Relocate(cx,nc) nc is the new context containing the Unlexed objects
    ///     2, The first step in Commit is to create Reader and a Writer based on the Database and not the Transaction
    ///     Then wr.cx.db is extended by the committed objects from the Transaction (see Physical.Commit)
    ///     3. During Parsing, the parsing context contains the evolving Transaction (i.e. cx.db is tr)
    ///     4. During Execution we can have temporary Contexts/Activations for doing aggregations, triggers,
    ///     recursion and exception handling
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
        internal Framing frame = null;
        internal BTree<int, BTree<long, DBObject>> depths = BTree<int, BTree<long, DBObject>>.Empty;
        internal BTree<long, TypedValue> values = BTree<long, TypedValue>.Empty;
        internal BTree<long, Register> funcs = BTree<long, Register>.Empty; // volatile
        internal BTree<long, BTree<long,TableRow>> newTables = BTree<long, BTree<long,TableRow>>.Empty;
        /// <summary>
        /// Left-to-right accumulation of definitions during a parse: accessed only by Query
        /// </summary>
        internal Ident.Idents defs = Ident.Idents.Empty;
        /// <summary>
        /// Lexical positions to DBObjects (if dbformat<51)
        /// </summary>
        public BTree<long, (string, long)> digest = BTree<long, (string, long)>.Empty;
        // UnHeap things for Procedure, Trigger, and Constraint bodies
        internal BTree<long, long> obuids = BTree<long, long>.Empty;
        internal BTree<long, long> rsuids = BTree<long, long>.Empty;
        // Keep track of rowsets for query
        internal BTree<long, long> results = BTree<long, long>.Empty; 
        internal RowSet result;
        internal bool unLex = false;
        internal BTree<long, RowSet.Finder> Needs(BTree<long, RowSet.Finder> nd, 
            RowSet rs,BList<long> rt)
        {
            for (var b = rt?.First(); b != null; b = b.Next())
                nd += obs[b.value()].Needs(this,rs);
            return nd;
        }
        internal BTree<long, bool> Needs(BTree<long,bool> nd,BList<long> rt)
        {
            for (var b = rt?.First(); b != null; b = b.Next())
                nd += obs[b.value()].Needs(this);
            return nd;
        }
        internal BTree<long, RowSet.Finder> Needs<V>(BTree<long, RowSet.Finder> nd, 
            RowSet rs, BTree<long, V> wh)
        {
            for (var b = wh?.First(); b != null; b = b.Next())
                nd += obs[b.key()].Needs(this,rs);
            return nd;
        }
        internal BTree<long, bool> Needs<V>(BTree<long, bool> nd, BTree<long,V> wh)
        {
            for (var b = wh?.First(); b != null; b = b.Next())
                nd += obs[b.key()].Needs(this);
            return nd;
        }
        // used SqlColRefs by From.defpos
        internal BTree<long, BTree<long, SqlValue>> used = BTree<long, BTree<long, SqlValue>>.Empty;
        /// <summary>
        /// Used in Replace cascade
        /// </summary>
        internal BTree<long, DBObject> done = BTree<long, DBObject>.Empty;
        /// <summary>
        /// Used for executing prepared statements (will be a list of SqlLiterals)
        /// </summary>
        internal CList<long> qParams = CList<long>.Empty;
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
        internal BTree<Audit,bool> auds = BTree<Audit,bool>.Empty;
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
        protected Context(Context cx,long trs)
        {
            db = cx.db;
            nextHeap = cx.nextHeap;
            nextStmt = cx.nextStmt;
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
        internal DBObject Get(Ident ic, Domain xp)
        {
            DBObject v;
            if (ic.Length > 0 && defs.Contains(ic.ToString())
                    && obs[defs[ic.ToString()].Item1] is SqlValue s0)
                v = s0;
            else
                v = obs[defs[ic.ident].Item1];
            if (v != null && !xp.CanTakeValueOf(v.domain))
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
        PRow Filter(PRow f)
        {
            if (f == null)
                return null;
            var t = Filter(f._tail);
            if (f._head is TQParam q)
                return new PRow(values[q.qid], t);
            return new PRow(f._head,t);
        }
        internal BTree<long,TypedValue> Filter(Table tb,BTree<long,bool> wh)
        {
            var r = BTree<long, TypedValue>.Empty;
            for (var b = wh.First(); b != null; b = b.Next())
                r += obs[b.key()].AddMatch(this, r, tb);
            return r;
        }
        internal BTree<long, TypedValue> Filter(Table tb, BTree<long, SqlValue> wh)
        {
            var r = BTree<long, TypedValue>.Empty;
            for (var b = wh.First(); b != null; b = b.Next())
                r += b.value().AddMatch(this, r, tb);
            return r;
        }
        internal void Install1(Framing fr)
        {
            obs += (fr.obs,true);
            defs += fr.defs;
        }
        internal void Install2(Framing fr)
        {
            for (var b = fr.data.First(); b != null; b = b.Next())
            {
                if (b.value() is FilterRowSet frs)
                    data += (b.key(), frs + (FilterRowSet.IxFilter, Filter(frs.filter)));
                else
                    data += (b.key(), b.value());
            }
            results = fr.results;
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
        internal void ObUnheap(long p)
        {
            if (unLex)
                ObUnLex(p);
            else if (!obuids.Contains(p))
            {
                if (p < Transaction.Executables)
                    obuids += (p, p);
                else
                {
                    while (obs.Contains(srcFix))
                        srcFix++;
                    obs += (srcFix, SqlNull.Value);
                    obuids += (p, srcFix);
                }
            } 
        }
        internal void ObScanned(long p)
        {
            if (!obuids.Contains(p))
                (obs[p]??(DBObject)db.objects[p])?.Scan(this);
        }
        internal void RsUnheap(long p)
        {
            if (unLex)
                RsUnLex(p);
            else if (!rsuids.Contains(p))
            {
                if (p < Transaction.Executables)
                    rsuids += (p, p);
                else if (obuids.Contains(p))
                    rsuids += (p, obuids[p]);
                else
                {
                    while (obs.Contains(srcFix))
                        srcFix++;
                    rsuids += (p, srcFix);
                }
            }
        }
        internal void RsScanned(long p)
        {
            if (!rsuids.Contains(p))
                data[p]?.Scan(this);
        }
        internal void ObUnLex(long p)
        {
            if (!obuids.Contains(p))
            {
                if (p >= Transaction.TransPos && p < Transaction.Executables)
                    obuids += (p, nextHeap++);
                else
                    obuids += (p, p);
            }
        }
        internal void RsUnLex(long p)
        {
            if (p < Transaction.TransPos || p >= Transaction.Executables)
                rsuids += (p, p);
            else if (!rsuids.Contains(p))
            {
                if (obuids.Contains(p))
                    rsuids += (p, obuids[p]);
                else
                    rsuids += (p, nextHeap++);
            }
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
        internal ObInfo Inf(long dp)
        {
            if ((dp<-1 || (dp >= 0 && dp < Transaction.Analysing)) && role.infos.Contains(dp))
                return (ObInfo)(role.infos[dp]);
            return null;
        }
        internal CList<long> Cols(long dp)
        {
            return obs[dp]?._Cols(this)??CList<long>.Empty;
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
        /// <summary>
        /// Check that two OrderSpecs for the same dataType have the same ordering.
        /// </summary>
        /// <param name="that"></param>
        /// <returns></returns>
        internal bool SameRowType(Query q, CList<long> a,CList<long> b)
        {
            if (a?.Length != b?.Length)
                return false;
            var ab = a.First();
            for (var bb = b.First(); bb != null; bb = bb.Next(), ab = ab.Next())
                if (!((SqlValue)obs[ab.value()]).MatchExpr(this, q, (SqlValue)obs[bb.value()]))
                    return false;
            return true;
        }
        internal bool HasItem(CList<long> rt,long p)
        {
            for (var b = rt.First(); b != null; b = b.Next())
                if (b.value() == p)
                    return true;
            return false;
        }
        internal virtual void AddValue(DBObject s, TypedValue tv)
        {
            if (tv is Cursor)
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
            throw new PEException("PE999");
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
                    if (od == -1L || !(ob.defpos >= Transaction.Executables && obs[od] is SqlValue ov
                        && ov.domain.kind == Sqlx.CONTENT))
                        defs += (ic, ob.defpos);
                }
            }
            if (ob.defpos >= PyrrhoServer.Preparing)
                done += (ob.defpos, ob);
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
            cx.Frame();
            var rs = CTree<long, Domain>.Empty;
            Ident ti = null;
            Table tb = null;
            for (var b = ns?.First(); b != null; b = b.Next())
            {
                var oi = b.value();
                var ic = new Ident(oi.name, b.key());
                if (oi.domain.kind == Sqlx.TABLE)
                {
                    ti = ic;
                    var p = b.key();
                    tb = (Table)db.objects[p];
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
        internal void Frame()
        {
            if (frame == null)
            {
                obs = BTree<long, DBObject>.Empty;
                data = BTree<long, RowSet>.Empty;
                frame = new Framing(this);
            }
        }
        internal void Frame(long p,long q=-1L)
        {
            if (p < 0 || obs.Contains(p))
                return;
            if (q < 0)
                q = p;
            var ob = (DBObject)db.objects[q];
            obs += (ob.defpos, ob);
            Install1(ob.framing);
            Install2(ob.framing);
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
                    if (cv.depth == bk)
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
        internal void AddDefs(Ident id, BList<long> s)
        {
            for (var b = s?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var ob = obs[p] ?? (DBObject)db.objects[p];
                var n = (ob is SqlValue v) ? v.alias??v.name : Inf(p)?.name;
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
                var pp = (FormalParameter)obs[b.value()];
                var pn = new Ident(pp.name, 0);
                defs += (pn, pp.defpos);
                defs += (new Ident(pi, pn), pp.defpos);
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
        internal TypedValue Eval(long dp,CList<long> s)
        {
            return new SqlRow(dp, this, Domain.Row, s).Eval(this);
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
        internal virtual Context FindCx(long c)
        {
            return this;
        }
        internal virtual TriggerActivation FindTriggerActivation(long tabledefpos)
        {
            return next?.FindTriggerActivation(tabledefpos)
                ?? throw new PEException("PE600");
        }
        internal CList<long> RowType(CList<SqlValue> ls)
        {
            var r = CList<long>.Empty;
            for (var b = ls.First(); b != null; b = b.Next())
                r += b.value().defpos;
            return r;
        }
        internal void Scan(BList<long> ord)
        {
            for (var b = ord?.First(); b != null; b = b.Next())
                ObUnheap(b.value());
        }
        internal void Scan<K>(BTree<K,BTree<long,bool>> os) where K:IComparable
        {
            for (var b = os?.First(); b != null; b = b.Next())
                Scan(b.value());
        }
        internal BList<long> Fix(BList<long> ord)
        {
            var r = BList<long>.Empty;
            var ch = false;
            for (var b = ord?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var f = obuids[p];
                if (p != f)
                    ch = true;
                r += f;
            }
            return ch ? r : ord;
        }
        internal CList<long> Fix(CList<long> ord)
        {
            var r = CList<long>.Empty;
            var ch = false;
            for (var b = ord?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var f = obuids[p];
                if (p != f)
                    ch = true;
                r += f;
            }
            return ch ? r : ord;
        }
        internal void Scan(BList<Domain> ds)
        {
            for (var b = ds.First(); b != null; b = b.Next())
                b.value().Scan(this);
        }
        internal BList<Domain> Fix(BList<Domain> ds)
        {
            var r = BList<Domain>.Empty;
            var ch = false;
            for (var b = ds?.First(); b != null; b = b.Next())
            {
                var d = b.value();
                var nd = (Domain)d.Fix(this);
                if (d != nd)
                    ch = true;
                r += nd;
            }
            return ch ? r : ds;
        }
        internal void Scan(BList<UpdateAssignment> us)
        {
            for (var b = us.First(); b != null; b = b.Next())
                b.value().Scan(this);
        }
        internal BList<UpdateAssignment> Fix(BList<UpdateAssignment> us)
        {
            var r = BList<UpdateAssignment>.Empty;
            var ch = false;
            for (var b=us.First();b!=null;b=b.Next())
            {
                var u = (UpdateAssignment)b.value().Fix(this);
                ch = ch || u != b.value();
                r += u;
            }
            return ch ? r : us;
        }
        internal void Scan(BList<Grouping> gs)
        {
            for (var b = gs.First(); b != null; b = b.Next())
                b.value().Scan(this);
        }
        internal BList<Grouping> Fix(BList<Grouping> gs)
        {
            var r = BList<Grouping>.Empty;
            var ch = false;
            for (var b=gs.First();b!=null;b=b.Next())
            {
                var g = (Grouping)b.value().Fix(this);
                ch = ch || g != b.value();
                r += g;
            }
            return ch ? r : gs;
        }
        internal void Scan(BTree<UpdateAssignment,bool> us)
        {
            for (var b = us.First(); b != null; b = b.Next())
                b.key().Scan(this);
        }
        internal BTree<UpdateAssignment,bool> Fix(BTree<UpdateAssignment,bool> us)
        {
            var r = BTree<UpdateAssignment,bool>.Empty;
            var ch = false;
            for (var b = us.First(); b != null; b = b.Next())
            {
                var u = (UpdateAssignment)b.key().Fix(this);
                ch = ch || u != b.key();
                r += (u,true);
            }
            return ch ? r : us;
        }
        internal void Scan(BList<TypedValue> key)
        {
            for (var b = key?.First(); b != null; b = b.Next())
                b.value().Scan(this);
        }
        internal BList<TypedValue> Fix(BList<TypedValue> key)
        {
            var r = BList<TypedValue>.Empty;
            var ch = false;
            for (var b = key?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var f = p.Fix(this);
                if (p != f)
                    ch = true;
                r += f;
            }
            return ch ? r : key;
        }
        internal CList<TypedValue> Fix(CList<TypedValue> key)
        {
            var r = CList<TypedValue>.Empty;
            var ch = false;
            for (var b = key?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var f = b.value().Fix(this);
                if (p != f)
                    ch = true;
                r += f;
            }
            return ch ? r : key;
        }
        internal void Scan(BList<TRow> rws)
        {
            for (var b = rws.First(); b != null; b = b.Next())
                b.value().Scan(this);
        }
        internal BList<TRow> Fix(BList<TRow> rws)
        {
            var r = BList<TRow>.Empty;
            var ch = false;
            for (var b=rws?.First();b!=null;b=b.Next())
            {
                var v = (TRow)b.value().Fix(this);
                ch = ch || v != b.value();
                r += v;
            }
            return ch ? r : rws;
        }
        internal void Scan<V>(BTree<long,V> ms)
        {
            for (var b = ms.First(); b != null; b = b.Next())
                ObScanned(b.key());
        }
        internal BTree<long,V> Fix<V>(BTree<long,V> ms)
        {
            var r = BTree<long, V>.Empty;
            var ch = false;
            for (var b=ms.First();b!=null;b=b.Next())
            {
                var m = obuids[b.key()];
                ch = ch || m != b.key();
                r += (m, b.value());
            }
            return ch ? r : ms;
        }
        internal void Scan<K>(BTree<K,long> ms) where K:IComparable
        {
            for (var b = ms.First(); b != null; b = b.Next())
                ObScanned(b.value());
        }
        internal BTree<K,long> Fix<K>(BTree<K, long> ms) where K:IComparable
        {
            var r = BTree<K,long>.Empty;
            var ch = false;
            for (var b = ms.First(); b != null; b = b.Next())
            {
                var m = obuids[b.value()];
                ch = ch || m != b.value();
                r += (b.key(),m);
            }
            return ch ? r : ms;
        }
        internal void Scan(BTree<long, BList<TypedValue>> refs)
        {
            for (var b = refs?.First(); b != null; b = b.Next())
            {
                ObUnheap(b.key());
                Scan(b.value());
            }
        }
        internal BTree<long,BList<TypedValue>> Fix(BTree<long,BList<TypedValue>> refs)
        {
            var r = BTree<long, BList<TypedValue>>.Empty;
            var ch = false;
            for (var b=refs?.First();b!=null;b=b.Next())
            {
                var p = obuids[b.key()];
                var vs = Fix(b.value());
                ch = ch || (p != b.key()) || vs != b.value();
                r += (p, vs);
            }
            return ch? r:refs;
        }
        internal void Scan(PRow rw)
        {
            if (rw!=null)
            {
                rw._head.Scan(this);
                Scan(rw._tail);
            }
        }
        internal void Scan(BTree<long,RowSet.Finder> fi)
        {
            for (var b=fi?.First();b!=null;b=b.Next())
            {
                ObScanned(b.key());
                var f = b.value();
                RsScanned(f.rowSet);
                ObScanned(f.col);
            }
        }
        internal BTree<long,RowSet.Finder> Fix(BTree<long,RowSet.Finder> fi)
        {
            var r = BTree<long,RowSet.Finder>.Empty;
            var ch = false;
            for (var b = fi?.First(); b != null; b = b.Next())
            {
                var p = obuids[b.key()];
                var f = b.value().Relocate(this);
                if (p != b.key() || (object)f!=(object)b.value())
                    ch = true;
                r += (p,f);
            }
            return ch ? r : fi;
        }
        internal void Scan(BTree<long,TypedValue> vt)
        {
            for (var b=vt.First();b!=null;b=b.Next())
            {
                ObScanned(b.key());
                b.value().Scan(this);
            }
        }
        internal BTree<long, TypedValue> Fix(BTree<long, TypedValue> vt)
        {
            var r = BTree<long, TypedValue>.Empty;
            var ch = false;
            for (var b = vt?.First(); b != null; b = b.Next())
            {
                var p = obuids[b.key()];
                var v = b.value().Fix(this);
                if (p != b.key() || v != b.value())
                    ch = true;
                r += (p, v);
            }
            return ch ? r : vt;
        }
        internal void Scan(BTree<SqlValue, TypedValue> vt)
        {
            for (var b = vt.First(); b != null; b = b.Next())
            {
                b.key().Scan(this);
                b.value().Scan(this);
            }
        }
        internal BTree<SqlValue, TypedValue> Fix(BTree<SqlValue, TypedValue> vt)
        {
            var r = BTree<SqlValue, TypedValue>.Empty;
            var ch = false;
            for (var b = vt?.First(); b != null; b = b.Next())
            {
                var p = (SqlValue)b.key().Fix(this);
                var v = b.value().Fix(this);
                if (p != b.key() || v != b.value())
                    ch = true;
                r += (p, b.value());
            }
            return ch ? r : vt;
        }
        internal void Scan(BTree<string,TypedValue> a)
        {
            for (var b = a.First(); b != null; b = b.Next())
                b.value().Scan(this);
        }
        internal BTree<string,TypedValue> Fix(BTree<string, TypedValue> a)
        {
            var r = BTree<string, TypedValue>.Empty;
            var ch = false;
            for (var b = a?.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var v = b.value().Fix(this);
                ch = ch || v != b.value();
                r += (p, v);
            }
            return ch?r:a;
        }
        internal void Scan(BList<TXml> cl)
        {
            for (var b = cl?.First(); b != null; b = b.Next())
                b.value().Scan(this);
        }
        internal void Scan(BList<(SqlXmlValue.XmlName,long)> es)
        {
            for (var b = es?.First(); b != null; b = b.Next())
                ObScanned(b.value().Item2);
        }
        internal BList<TXml> Fix(BList<TXml> cl)
        {
            var r = BList<TXml>.Empty;
            var ch = false;
            for (var b = cl?.First(); b != null; b = b.Next())
            {
                var v = (TXml)b.value().Fix(this);
                ch = ch || v != b.value();
                r += v;
            }
            return ch? r:cl;
        }
        internal BList<(SqlXmlValue.XmlName,long)> Fix(BList<(SqlXmlValue.XmlName, long)> cs)
        {
            var r = BList<(SqlXmlValue.XmlName, long)>.Empty;
            var ch = false;
            for (var b = cs?.First(); b != null; b = b.Next())
            {
                var (n, p) = b.value();
                var np = obuids[p];
                ch = ch || p!=np;
                r += (n,np);
            }
            return ch ? r : cs;
        }
        internal void Scan(CTree<TypedValue,long> mu)
        {
            for (var b = mu?.First(); b != null; b = b.Next())
            {
                b.key().Scan(this);
                ObUnheap(b.value());
            }
        }
        internal CTree<TypedValue,long> Fix(CTree<TypedValue,long> mu)
        {
            var r = CTree<TypedValue,long>.Empty;
            var ch = false;
            for (var b = mu?.First(); b != null; b = b.Next())
            {
                var p = b.key().Fix(this);
                var q = (obuids.Contains(b.value()))?obuids[b.value()]:-1L;
                ch = ch || p != b.key() || q != b.value();
                r += (p, q);
            }
            return ch? r : mu;
        }
        internal void Scan(BTree<long,BTree<long,bool>> ma)
        {
            for (var b=ma.First();b!=null;b=b.Next())
            {
                ObUnheap(b.key());
                Scan(b.value());
            }
        }
        internal BTree<long,BTree<long,bool>> Fix(BTree<long, BTree<long,bool>> ma)
        {
            var r = BTree<long, BTree<long,bool>>.Empty;
            var ch = false;
            for (var b = ma?.First(); b != null; b = b.Next())
            {
                var p = obuids[b.key()];
                var v = Fix(b.value());
                ch = ch || p != b.key() || v != b.value();
                r += (p, v);
            }
            return ch ? r : ma;
        }
        internal void Scan(BTree<long,Domain> rs)
        {
            for (var b=rs.First();b!=null;b=b.Next())
            {
                ObUnheap(b.key());
                b.value().Scan(this);
            }
        }
        internal CTree<long,Domain> Fix(CTree<long,Domain> rs)
        {
            var r = CTree<long, Domain>.Empty;
            var ch = false;
            for (var b=rs.First();b!=null;b=b.Next())
            {
                var p = obuids[b.key()];
                var d = (Domain)b.value().Fix(this);
                ch = ch || p != b.key() || d != b.value();
                r += (p, d);
            }
            return ch?r:rs;
        }
        internal void Scan(BTree<long, DBObject> os)
        {
            for (var b = os.First(); b != null; b = b.Next())
            {
                ObUnheap(b.key());
                b.value().Scan(this);
            }
        }
        internal void Scan(BTree<long, RowSet> rs)
        {
            for (var b = rs.First(); b != null; b = b.Next())
            {
                RsUnheap(b.key());
                b.value().Scan(this);
            }
        }
        internal void Scan(BTree<long, SqlValue> rs)
        {
            for (var b = rs.First(); b != null; b = b.Next())
            {
                ObUnheap(b.key());
                b.value().Scan(this);
            }
        }
        internal BTree<long, RowSet> Fix(BTree<long, RowSet> rs)
        {
            var r = BTree<long, RowSet>.Empty;
            var ch = false;
            for (var b = rs.First(); b != null; b = b.Next())
            {
                var p = rsuids[b.key()];
                var d = (RowSet)b.value().Fix(this);
                ch = ch || p != b.key() || d != b.value();
                r += (p, d);
            }
            return ch ? r : rs;
        }
        internal BTree<long, SqlValue> Fix(BTree<long, SqlValue> rs)
        {
            var r = BTree<long, SqlValue>.Empty;
            var ch = false;
            for (var b = rs.First(); b != null; b = b.Next())
            {
                var p = obuids[b.key()];
                var d = (SqlValue)b.value().Fix(this);
                ch = ch || p != b.key() || d != b.value();
                r += (p, d);
            }
            return ch ? r : rs;
        }
        internal BTree<PTrigger.TrigType, BTree<long, bool>> Fix(BTree<PTrigger.TrigType, BTree<long, bool>> t)
        {
            var r = BTree<PTrigger.TrigType, BTree<long, bool>>.Empty;
            for (var b = t.First(); b != null; b = b.Next())
            {
                var p = b.key();
                r += (p, Fix(b.value()));
            }
            return r;
        }
    }
    internal class Framing : Basis // for compiled code
    {
        internal const long
            Data = -450,    // BTree<long,RowSet>
            Defs = -451,    // Ident.Idents
            Obs = -449,     // BTree<long,DBObject>
            Relocated = -456, // bool
            Result = -452,  // long
            Results = -453; // BTree<long,long> Query RowSet
        public BTree<long, DBObject> obs => 
            (BTree<long,DBObject>)mem[Obs]??BTree<long,DBObject>.Empty;
        public BTree<long, RowSet> data =>
            (BTree<long,RowSet>)mem[Data]??BTree<long,RowSet>.Empty;
        public Ident.Idents defs =>
            (Ident.Idents)mem[Defs]??Ident.Idents.Empty;
        public long result => (long)(mem[Result]??-1L);
        public bool relocated => (bool)(mem[Relocated] ?? false);
        public BTree<long, long> results =>
            (BTree<long, long>)mem[Results] ?? BTree<long, long>.Empty;
 //       public BTree<int, BTree<long, DBObject>> depths =>
 //           (BTree<int,BTree<long,DBObject>>)mem[Depths]
 //           ??BTree<int,BTree<long,DBObject>>.Empty;
        public readonly static Framing Empty = new Framing();
        Framing() { }
        Framing(BTree<long,object> m) :base(m) { }
        public Framing(Context cx) 
            : base(BTree<long,object>.Empty+(Obs,cx.obs)+(Data,cx.data)
                  +(Results,cx.results)
                  +(Defs,cx.defs)+(Result,cx.result?.defpos??-1L))//+(Depths,cx.depths))
        { }
        internal override Basis New(BTree<long, object> m)
        {
            return new Framing(m);
        }
        public static Framing operator+(Framing f,(long,object)x)
        {
            return new Framing(f.mem + x);
        }
        public static Framing operator+(Framing f,DBObject ob)
        {
            var p = ob.defpos;
   //         var d = f.depths[ob.depth]??BTree<long,DBObject>.Empty;
            return f + (Obs, f.obs + (p, ob));// + (Depths, f.depths + (ob.depth, d + (p, ob)));
        }
        public static Framing operator+(Framing f,RowSet r)
        {
            return f + (Data, f.data + (r.defpos, r));
        }
        public static Framing operator-(Framing f, RowSet r)
        {
            return f + (Data, f.data - r.defpos);
        }
        public static Framing operator-(Framing f,long p)
        {
            return f + (Obs, f.obs - p);
        }
        internal override void Scan(Context cx)
        {
            cx.Scan(obs);
            cx.Scan(data);
            defs.Scan(cx);
            cx.RsUnheap(result);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (relocated)
                return this;
            var r = (Framing)base._Relocate(wr) + (Relocated,true);
            for (var b = r.obs.First(); b != null; b = b.Next())
            {
                var p = b.key();
                if (p >= Transaction.TransPos)
                    wr.cx.obs += (p, b.value());
            }
            for (var b = r.data.First(); b != null; b = b.Next())
            {
                var p = b.key();
                wr.cx.data += (p, b.value());
            }
            for (var b = r.obs.First(); b != null; b = b.Next())
            {
                var p = b.key();
 //               if (p < Transaction.TransPos || p >= Transaction.Analysing)
                    r += wr.Fixed(p);
            }
            for (var b = r.obs.First(); b != null; b = b.Next())
            {
                var p = b.key();
                if (p >= Transaction.TransPos)
                    r -= p;
            }
            for (var b = r.data.First(); b != null; b = b.Next())
            {
                var or =  b.value();
                var nr = (RowSet)or._Relocate(wr);
                if (or.defpos != nr.defpos)
                    r -= or;
                r += nr;
            }
            var rs = BTree<long, long>.Empty;
            for (var b=results.First();b!=null;b=b.Next())
                rs += (wr.Fix(b.key()),wr.Fix(b.value()));
            r += (Defs, r.defs.Relocate(wr));
            r += (Result, wr.Fix(r.result));
            r += (Results, rs);
            return r;
        }
        internal override Basis _Relocate(Context cx, Context nc)
        {
            var r = Empty;
            for (var b = obs.First(); b != null; b = b.Next())
                r += (DBObject)b.value()._Relocate(cx, nc);
            for (var b = data.First(); b != null; b = b.Next())
                r += (RowSet)b.value()._Relocate(cx, nc);
            var rs = BTree<long, long>.Empty;
            for (var b = results.First(); b != null; b = b.Next())
                rs += (cx.obuids[b.key()], cx.rsuids[b.value()]);
            nc.results = rs;
            r += (Defs, defs.Relocate(cx));
            r += (Result, cx.rsuids[result]);
            r += (Results, rs);
            return r;
        }
        public void Install(Context cx)
        {
            cx.obs += obs;
  //          cx.depths += depths;
            cx.defs = (Ident.Idents)cx.defs.Add(defs);
            cx.data += data;
            cx.results += results;
            cx.result = cx.data[result];
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' '); sb.Append(defs);
            var cm = " Obs: (";
            for (var b = obs.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ","; sb.Append(DBObject.Uid(b.key())); 
                sb.Append(' '); sb.Append(b.value());
            }
            cm = ") Data: (";
            for (var b=data.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ","; sb.Append(DBObject.Uid(b.key())); 
                sb.Append(' '); sb.Append(b.value());
            }
            sb.Append(") Results: (");
            for (var b = results.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ","; sb.Append(DBObject.Uid(b.key())); 
                sb.Append(' '); sb.Append(DBObject.Uid(b.value()));
            }
            sb.Append(")");
            return sb.ToString();
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
        internal Numeric sumDecimal;
        /// <summary>
        /// the boolean result so far
        /// </summary>
        internal bool bval = false;
        /// <summary>
        /// a multiset for accumulating things
        /// </summary>
        internal TMultiset mset = null;
        public override string ToString()
        {
            var s = new StringBuilder("{");
            if (count != 0L) s.Append(count);
            if (sb != null) s.Append(sb);
            switch(sumType.kind)
            {
                case Sqlx.INT:
                case Sqlx.INTEGER:
                    sb.Append(sumInteger?.ToString() ?? sumLong.ToString());
                    break;
                case Sqlx.REAL:
                    sb.Append(sum1); break;
                case Sqlx.NUMERIC:
                    sb.Append(sumDecimal); break;
                case Sqlx.BOOLEAN:
                    sb.Append(bval); break;
            }
            s.Append("}");
            return s.ToString();
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
