using System;
using System.Configuration;
using System.Diagnostics.Eventing.Reader;
using System.Runtime.ExceptionServices;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
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
        internal CTree<long,bool> withViews = CTree<long,bool>.Empty;
        internal Rvv affected = null;
        internal CTree<string,Rvv> etags = CTree<string,Rvv>.Empty;
        internal int physAtStepStart;
        public BTree<long, Cursor> cursors = BTree<long, Cursor>.Empty;
        internal BTree<long, RowSet> data = BTree<long, RowSet>.Empty;
        internal CTree<long, RowSet.Finder> finder = CTree<long, RowSet.Finder>.Empty; 
        public long nextHeap, nextStmt, parseStart, srcFix;
        public TypedValue val = TNull.Value;
        internal Database db = null;
        internal Transaction tr => db as Transaction;
        internal BTree<long, Physical> physicals = BTree<long, Physical>.Empty;
        internal BList<TriggerActivation> deferred = BList<TriggerActivation>.Empty;
        internal BList<Exception> warnings = BList<Exception>.Empty;
        internal BTree<long, DBObject> obs = BTree<long, DBObject>.Empty;
        internal Framing frame = null;
        internal BTree<int, BTree<long, DBObject>> depths = BTree<int, BTree<long, DBObject>>.Empty;
        internal CTree<long, TypedValue> values = CTree<long, TypedValue>.Empty;
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
        internal BTree<long, long?> obuids = BTree<long, long?>.Empty;
        internal BTree<long, long?> rsuids = BTree<long, long?>.Empty;
        // Keep track of rowsets for query
        internal BTree<long, long> results = BTree<long, long>.Empty; 
        internal long result;
        internal bool unLex = false;
        internal CTree<long, RowSet.Finder> Needs(CTree<long, RowSet.Finder> nd, 
            RowSet rs,CList<long> rt)
        {
            for (var b = rt?.First(); b != null; b = b.Next())
                nd += obs[b.value()].Needs(this,rs);
            return nd;
        }
        internal CTree<long, bool> Needs(CTree<long,bool> nd,CList<long> rt)
        {
            for (var b = rt?.First(); b != null; b = b.Next())
                nd += obs[b.value()].Needs(this);
            return nd;
        }
        internal CTree<long, RowSet.Finder> Needs<V>(CTree<long, RowSet.Finder> nd, 
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
            nextHeap = db.nextPrep; // NB
            nextStmt = db.nextStmt;
            dbformat = db.format;
            parseStart = 0L;
            physAtStepStart = (int)physicals.Count;
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
            data = cx.data;
            finder = cx.finder;
            cursors = cx.cursors;
            val = cx.val;
            parent = cx.parent; // for triggers
            dbformat = cx.dbformat;
            rdC = cx.rdC;
            // and maybe some more?
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
        internal long GetUid()
        {
            return (parse==ExecuteStatus.Obey) ? nextHeap++ : nextStmt++;
        }
        internal long GetUid(int n)
        {
            long r;
            if (parse == ExecuteStatus.Obey)
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
        internal long Next(long dp)
        {
            return obuids[dp] ?? rsuids[dp] ?? GetUid();
        }
        internal long GetPrevUid()
        {
            return ((parse == ExecuteStatus.Obey) ? nextHeap : nextStmt)-1L;
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
        internal CTree<long,TypedValue> Filter(Table tb,CTree<long,bool> wh)
        {
            var r = CTree<long, TypedValue>.Empty;
            for (var b = wh.First(); b != null; b = b.Next())
                r += obs[b.key()].AddMatch(this, r, tb);
            return r;
        }
        internal void Install1(Framing fr)
        {
            obs += (fr.obs,true);
     //       for (var b = fr.obs.First(); b != null; b = b.Next())
     //           Add(b.value());
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
            result = fr.result;
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
        internal long ObUnheap(long p)
        {
            var r = p;
            if (unLex)
                r = ObUnLex(p);
            else if ((!obuids.Contains(p)) && p >= Transaction.HeapStart)
            {
                while (obs.Contains(srcFix)||data.Contains(srcFix))
                    srcFix++;
                obs += (srcFix, SqlNull.Value);
                obuids += (p, srcFix);
                r = srcFix;
            }
            return r;
        }
        internal long RsUnheap(long p)
        {
            var r = p;
            if (unLex)
                r = RsUnLex(p);
            else if ((!rsuids.Contains(p))&& p >= Transaction.HeapStart)
            {
                if (obuids.Contains(p))
                {
                    rsuids += (p, obuids[p]);
                    r = obuids[p]??p;
                } 
                else
                {
                    while (obs.Contains(srcFix)||data.Contains(srcFix))
                        srcFix++;
                    rsuids += (p, srcFix);
                    data += (srcFix, new EmptyRowSet(srcFix,this,Domain.Null));
                    r = srcFix;
                }
            }
            return r;
        }
        internal long ObUnLex(long p)
        {
            var r = p;
            if ((!obuids.Contains(p)) && p >= Transaction.TransPos && p < Transaction.Executables)
            {
                r = GetUid();
                obuids += (p, r);
            }
            return r;
        }
        internal long RsUnLex(long p)
        {
            var r = p;
            if (p >= Transaction.TransPos && p < Transaction.Executables
                && !rsuids.Contains(p))
            {
                if (obuids.Contains(p))
                {
                    r = obuids[p]??p;
                    rsuids += (p, r);
                }
                else
                {
                    r = GetUid();
                    rsuids += (p, r);
                }
            }
            return r;
        }
        internal int Depth(CList<long> os)
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
        internal BTree<long, SqlValue> Map(CList<long> s)
        {
            var r = BTree<long, SqlValue>.Empty;
            for (var b = s.First(); b != null; b = b.Next())
            {
                var p = b.value();
                r += (p, (SqlValue)obs[p]);
            }
            return r;
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
        }
        internal void Add(Physical ph, long lp = 0)
        {
            if (lp == 0)
                lp = db.loadpos;
            if (PyrrhoStart.DebugMode && db is Transaction)
                Console.WriteLine(ph.ToString());
            db.Add(this, ph, lp);
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
                    // if it is about to be Resolved
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
            var ldpos = db.loadpos;
            for (var cc = next; cc != null; cc = cc.next)
                ldpos = cc.db.loadpos;
            for (var b = depths.First(); b != null; b = b.Next())
            {
                var bv = b.value();
                for (var c = bv.PositionAt(ldpos); c != null; c = c.Next())
                {
                    var p = c.value();
                    var cv = p.Replace(this, was, now); // may update done
                    if (cv != p)
                        bv += (c.key(), cv);
                }
                if (bv != b.value())
                    depths += (b.key(), bv);
            }
            for (var b = data.First(); b != null; b = b.Next())
                b.value()._Replace(this, was, now);
            // now scan by depth to install the new versions
            for (var b = depths.First(); b != null; b = b.Next())
            {
                var bk = b.key();
                for (var c = done.First(); c != null; c = c.Next())
                {
                    var cv = c.value();
                    if (cv.depth == bk)
                    {
                        obs += (cv.defpos, cv);
                        if (obs[c.key()] is DBObject oo && oo.depth != cv.depth)
                        {
                            var de = depths[oo.depth];
                            depths += (oo.depth, de - cv.defpos);
                        }
                        if (cv is SqlValue sv)
                            defs += (new Ident(sv.name,sv.defpos), cv.defpos);
                    }
                }
            }
            defs = defs.ApplyDone(this);
            if (was.defpos != now.defpos)
                _Remove(was.defpos);
            return now;
        }
        internal long Replace(long dp,DBObject was,DBObject now)
        {
            if (dp < Transaction.TransPos)
                return dp;
            if (done.Contains(dp))
                return done[dp].defpos;
            return obs[dp]?._Replace(this,was, now)?.defpos??-1L;
        }
        internal CTree<long,RowSet.Finder> Replaced(CTree<long,RowSet.Finder>fi)
        {
            var r = CTree<long, RowSet.Finder>.Empty;
            for (var b=fi.First();b!=null;b=b.Next())
            {
                var f = b.value();
                var k = done[b.key()]?.defpos??b.key();
                var nc = done[f.col]?.defpos??f.col;
                r += (k, new RowSet.Finder(nc, f.rowSet));
            }
            return r;
        }
        internal BTree<long,Cursor> Replaced(BTree<long,Cursor> ec)
        {
            var r = BTree<long, Cursor>.Empty;
            for (var b = ec.First();b!=null;b=b.Next())
            {
                var c = b.value();
                var k = done[b.key()]?.defpos ?? b.key();
                var nc = (Cursor)c.Replaced(this);
                r += (k, nc);
            }
            return r;
        }
        internal CList<long> Replaced(CList<long> ks)
        {
            var r = CList<long>.Empty;
            for (var b = ks.First(); b != null; b = b.Next())
                r += done[b.value()]?.defpos ?? b.value();
            return r;
        }
        internal CTree<long,bool> Replaced(CTree<long,bool> wh)
        {
            var r = CTree<long,bool>.Empty;
            for (var b = wh.First(); b != null; b = b.Next())
                r += (done[b.key()]?.defpos ?? b.key(),b.value());
            return r;
        }
        internal CTree<long,long> Replaced(CTree<long,long> wh)
        {
            var r = CTree<long, long>.Empty;
            for (var b = wh.First(); b != null; b = b.Next())
                r += (done[b.key()]?.defpos ?? b.key(), done[b.value()]?.defpos??b.value());
            return r;
        }
        internal CTree<long, TypedValue> Replaced(CTree<long, TypedValue> wh)
        {
            var r = CTree<long, TypedValue>.Empty;
            for (var b = wh.First(); b != null; b = b.Next())
                r += (done[b.key()]?.defpos ?? b.key(), b.value().Replaced(this));
            return r;
        }
        internal CTree<string,CTree<long,long>> Replaced
            (CTree<string, CTree<long, long>> vc)
        {
            var r = CTree<string, CTree<long, long>>.Empty;
            for (var b = vc.First(); b != null; b = b.Next())
                r += (b.key(), Replaced(b.value()));
            return r;
        }
        /// <summary>
        /// This is called at the end of CursorSpecification.RowSets and just before
        /// Obeying SqlInsert, SqlUpdate, and SqlDelete.
        /// We review the rowsets adding matches information wherever we can,
        /// and then review each rowset to see if it can be replaced with its source.
        /// </summary>
        internal void Review(RowSet r, CTree<long,bool> ags, CTree<long,TypedValue> matches,
            CTree<UpdateAssignment,bool> asg)
        {
            for (var vb = withViews.First(); vb != null; vb = vb.Next())
                if (data[vb.key()] is RestRowSet rr)
                {
                    var vi = (ObInfo)db.role.infos[rr.restView];
                    var map = BTree<long, SqlValue>.Empty;
                    for (var b = r.rt.First(); b != null; b = b.Next())
                    {
                        var sc = (SqlValue)obs[b.value()];
                        (_, map) = sc.ColsForRestView(this, rr, vi, null, map); // transform for aggregations and filters
                    }
                    for (var b = map.First(); b != null; b = b.Next())
                        _Add(b.value());
                }
            var todo = new BList<(long, CTree<long,bool>, CTree<long, TypedValue>,
                CTree<UpdateAssignment,bool>,CTree<long,CTree<long,bool>>)>
                ((r.defpos, ags, matches, asg, r.matching));
            var skip = BTree<long, bool>.Empty;
            while (todo.Count > 0)
            {
                var (rp, ag, ma, sg, mg) = todo[0];
                todo -= 0;
                var rs = data[rp];
                if (rp >= Transaction.TransPos)
                {
                    mg += rs.matching;
                    ma += rs.matches;
                    // if uids x and y eventually have to match, then
                    // any matches condition on x can become a condition on y
                    // (and remember the matching tree is symmetric)
                    for (var b = mg.First(); b != null; b = b.Next())
                    {
                        var x = b.key();
                        if (ma[x] is TypedValue v)
                            for (var c = b.value().First(); c != null; c = c.Next())
                            {
                                var y = c.key();
                                if (ma[y] is TypedValue w)
                                { 
                                    if (v.CompareTo(w) != 0)
                                    { // the rowset will be empty
                                        skip += (rp, true);
                                        goto skipped;
                                    }
                                    // we have it already
                                }
                                else
                                    ma += (y, v);
                            }
                    }
                    ma = rs.Review(this, ma);
                    if (ma != rs.matches)
                    {
                        rs += (Query._Matches, ma);
                        data += (rp, rs);
                    }
                    ag = rs.Review(this, ag);
                    if (ag != rs.aggs)
                    {
                        rs += (Query.Aggregates, ag);
                        data += (rp, rs);
                    }
                    sg = rs.Review(this, sg);
                    if (sg != rs.assig)
                    {
                        rs += (Query.Assig, sg);
                        data += (rp, rs);
                    }
                    for (var b = rs.Sources(this).First(); b != null; b = b.Next())
                         todo += (b.value(), ag, ma, sg, mg);
                    skipped:;
                }
            }
            var tbd = new BList<long>(r.defpos);
            var done = BTree<long, bool>.Empty;
            while (tbd.Count > 0)
            {
                var rp = tbd[0];
                tbd -= 0;
                var rs = data[rp];
                if (rp >= Transaction.TransPos && !done.Contains(rp))
                {
                    done += (rp, true);
                    rs = rs.Review(this,skip);
                    if (rs.defpos != rp)
                    {
                        rsuids += (rs.defpos, rp);
                        rs = (RowSet)rs.Relocate(rp);
                    }
                    tbd += rs.Sources(this);
                    data += (rp, rs);
                }
            }
            for (var b=done.First();b!=null;b=b.Next())
            {
                var rs = data[b.key()];
                rs = rs.Relocate1(this);
                data += (rs.defpos, rs);
            }
            // problem: if one or more rowsets have been removed
            // so that two entries in cx.data point to the same thing
            // we'd better Fix everything
            FixAll(db.loadpos - 1);
            // This is the right time to check on Needed uids in rowsets
            for (var b = data.First(); b != null; b = b.Next())
                b.value().ComputeNeeds(this);
        }
        // Following View.Instance cascade or Review
        internal void FixAll(long dp)
        {
            for (var b = obs.PositionAt(dp); b != null; b = b.Next())
            {
                var p = b.key();
                var np = obuids[p] ?? p;
                var nb = (DBObject)b.value().Fix(this);
                obs += (np, nb);
            }
            for (var b = data.PositionAt(dp + 1); b != null; b = b.Next())
            {
                var p = b.key();
                var np = rsuids[p] ?? p;
                var nr = (RowSet)b.value().Fix(this);
                data += (np, nr);
            }
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
        internal void AddDefs(Ident id, Domain dm)
        {
            for (var b = dm?.rowType.First(); b != null;// && b.key() < dm.display; 
                b = b.Next())
            {
                var p = b.value();
                var ob = obs[p] ?? (DBObject)db.objects[p];
                var n = (ob is SqlValue v) ? v.alias??v.name : Inf(p)?.name;
                if (n == null)
                    continue;
                var ic = new Ident(n, p);
                var iq = new Ident(id, ic);
                if (defs[iq]<0)
                    defs += (iq, ob.defpos);
                if (defs[ic]<0)
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
        internal PRow MakeKey(CList<long>s)
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
            if (next == null)
                return this;
            next.values += values;
            next.warnings += warnings;
            next.physicals += physicals;
            next.deferred += deferred;
            next.val = val;
            next.nextHeap = nextHeap;
            next.nextStmt = nextStmt;
            next.db = db; // adopt the transaction changes done by this
            return next;
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
        internal void Scan(CList<long> ord)
        {
            for (var b = ord?.First(); b != null; b = b.Next())
                ObUnheap(b.value());
        }
        internal BList<long> Fix(BList<long> ord)
        {
            var r = BList<long>.Empty;
            var ch = false;
            for (var b = ord?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var f = obuids[p] ?? p;
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
                var f = obuids[p]??p;
                if (p != f)
                    ch = true;
                r += f;
            }
            return ch ? r : ord;
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
        internal CList<UpdateAssignment> Fix(CList<UpdateAssignment> us)
        {
            var r = CList<UpdateAssignment>.Empty;
            var ch = false;
            for (var b=us.First();b!=null;b=b.Next())
            {
                var u = (UpdateAssignment)b.value().Fix(this);
                ch = ch || u != b.value();
                r += u;
            }
            return ch ? r : us;
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
        internal CTree<UpdateAssignment,bool> Fix(CTree<UpdateAssignment,bool> us)
        {
            var r = CTree<UpdateAssignment,bool>.Empty;
            var ch = false;
            for (var b = us.First(); b != null; b = b.Next())
            {
                var u = (UpdateAssignment)b.key().Fix(this);
                ch = ch || u != b.key();
                r += (u,true);
            }
            return ch ? r : us;
        }
        internal CTree<string,CTree<long,long>> Fix
            (CTree<string,CTree<long,long>> vc)
        {
            var r = CTree<string, CTree<long, long>>.Empty;
            for (var b = vc.First(); b != null; b = b.Next())
                r += (b.key(),Fix(b.value()));
            return r;
        }
        internal BList<Cursor> Fix(BList<Cursor> rws)
        {
            var r = BList<Cursor>.Empty;
            var ch = false;
            for (var b=rws.First();b!=null;b=b.Next())
            {
                var rr = b.value();
                var fr = rr?._Fix(this);
                ch = ch || fr != rr;
                r += fr;
            }
            return r;
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
        internal CTree<long,V> Fix<V>(CTree<long,V> ms) where V:IComparable
        {
            var r = CTree<long, V>.Empty;
            var ch = false;
            for (var b=ms?.First();b!=null;b=b.Next())
            {
                var k = b.key();
                var m = obuids[k]??k;
                ch = ch || m != k;
                r += (m, b.value());
            }
            return ch ? r : ms;
        }
        internal CTree<K,long> Fix<K>(CTree<K, long> ms) where K:IComparable
        {
            var r = CTree<K,long>.Empty;
            var ch = false;
            for (var b = ms.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var m = obuids[p]??p;
                ch = ch || m != p;
                r += (b.key(),m);
            }
            return ch ? r : ms;
        }
        internal CTree<long,RowSet.Finder> Fix(CTree<long,RowSet.Finder> fi)
        {
            var r = CTree<long,RowSet.Finder>.Empty;
            var ch = false;
            for (var b = fi?.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = obuids[k]??k;
                var f = b.value().Relocate(this);
                if (p != b.key() || (object)f!=(object)b.value())
                    ch = true;
                r += (p,f);
            }
            return ch ? r : fi;
        }
        internal BTree<long, Cursor> Fix(BTree<long, Cursor> vt)
        {
            var r = BTree<long, Cursor>.Empty;
            var ch = false;
            for (var b = vt?.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = obuids[k] ?? k;
                var v = (Cursor)b.value().Fix(this);
                if (p != b.key() || v != b.value())
                    ch = true;
                r += (p, v);
            }
            return ch ? r : vt;
        }
        internal BList<BTree<long, Cursor>> Fix(BList<BTree<long, Cursor>> vt)
        {
            var r = BList<BTree<long, Cursor>>.Empty;
            var ch = false;
            for (var b = vt?.First(); b != null; b = b.Next())
            {
                var v = Fix(b.value());
                if (v != b.value())
                    ch = true;
                r += v;
            }
            return ch ? r : vt;
        }
        internal CTree<long, TypedValue> Fix(CTree<long, TypedValue> vt)
        {
            var r = CTree<long, TypedValue>.Empty;
            var ch = false;
            for (var b = vt?.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = obuids[k] ?? k;
                var v = b.value().Fix(this);
                if (p != b.key() || v != b.value())
                    ch = true;
                r += (p, v);
            }
            return ch ? r : vt;
        }
        internal CTree<string,TypedValue> Fix(CTree<string, TypedValue> a)
        {
            var r = CTree<string, TypedValue>.Empty;
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
        internal CList<TXml> Fix(CList<TXml> cl)
        {
            var r = CList<TXml>.Empty;
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
                var np = obuids[p]??p;
                ch = ch || p!=np;
                r += (n,np);
            }
            return ch ? r : cs;
        }
        internal CTree<TypedValue,long> Fix(CTree<TypedValue,long> mu)
        {
            var r = CTree<TypedValue,long>.Empty;
            var ch = false;
            for (var b = mu?.First(); b != null; b = b.Next())
            {
                var p = b.key().Fix(this);
                var q = obuids[b.value()]??-1L;
                ch = ch || p != b.key() || q != b.value();
                r += (p, q);
            }
            return ch? r : mu;
        }
        internal BTree<string,long> Fix(BTree<string,long>cs)
        {
            var r = BTree<string, long>.Empty;
            for (var b = cs?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                r += (b.key(), obuids[p]??p);
            }
            return r;
        }
        internal CTree<long,CTree<long,bool>> Fix(CTree<long, CTree<long,bool>> ma)
        {
            var r = CTree<long, CTree<long,bool>>.Empty;
            var ch = false;
            for (var b = ma?.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = obuids[k]??k;
                var v = Fix(b.value());
                ch = ch || p != k || v != b.value();
                r += (p, v);
            }
            return ch ? r : ma;
        }
        internal CTree<long,Domain> Fix(CTree<long,Domain> rs)
        {
            var r = CTree<long, Domain>.Empty;
            var ch = false;
            for (var b=rs.First();b!=null;b=b.Next())
            {
                var k = b.key();
                var p = obuids[k]??k;
                var d = (Domain)b.value().Fix(this);
                ch = ch || p != k || d != b.value();
                r += (p, d);
            }
            return ch?r:rs;
        }
        internal BTree<long, RowSet> Fix(BTree<long, RowSet> rs)
        {
            var r = BTree<long, RowSet>.Empty;
            var ch = false;
            for (var b = rs.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = rsuids[k]??k;
                var d = (RowSet)b.value().Fix(this);
                ch = ch || p != k || d != b.value();
                r += (p, d);
            }
            return ch ? r : rs;
        }
        internal CTree<long, long> Fix(CTree<long, long> rs)
        {
            var r = CTree<long, long>.Empty;
            var ch = false;
            for (var b = rs.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = obuids[k]??k;
                var v = b.value();
                var d = obuids[v] ?? v;
                ch = ch || p != k || d != v;
                r += (p, d);
            }
            return ch ? r : rs;
        }
        internal CTree<PTrigger.TrigType, CTree<long, bool>> Fix(CTree<PTrigger.TrigType, CTree<long, bool>> t)
        {
            var r = CTree<PTrigger.TrigType, CTree<long, bool>>.Empty;
            for (var b = t.First(); b != null; b = b.Next())
            {
                var p = b.key();
                r += (p, Fix(b.value()));
            }
            return r;
        }
    }
    // shareable as of 26 April 2021
    internal class Framing : Basis // for compiled code
    {
        internal const long
            Data = -450,    // BTree<long,RowSet>
            ObRefs = -451, // BTree<long,BTree<long,VIC?>> referer references VIC K->V
            Obs = -449,     // BTree<long,DBObject>
            RefObs = -460, // BTree<long,BTree<long,VIC?>> reference referers VIC V->K
            Result = -452,  // long
            Results = -453; // BTree<long,long> Query RowSet
        public BTree<long, DBObject> obs => 
            (BTree<long,DBObject>)mem[Obs]??BTree<long,DBObject>.Empty;
        public BTree<long, RowSet> data =>
            (BTree<long,RowSet>)mem[Data]??BTree<long,RowSet>.Empty;
        public long result => (long)(mem[Result]??-1L);
        public BTree<long, long> results =>
            (BTree<long, long>)mem[Results] ?? BTree<long, long>.Empty;
        public Rvv withRvv => (Rvv)mem[Rvv.RVV];
        public BTree<long, BTree<long, VIC?>> obrefs =>
            (BTree<long, BTree<long, VIC?>>)mem[ObRefs] ?? BTree<long, BTree<long, VIC?>>.Empty;
        public BTree<long, BTree<long, VIC?>> refObs =>
            (BTree<long, BTree<long, VIC?>>)mem[RefObs] ?? BTree<long, BTree<long, VIC?>>.Empty;
        //       public BTree<int, BTree<long, DBObject>> depths =>
        //           (BTree<int,BTree<long,DBObject>>)mem[Depths]
        //           ??BTree<int,BTree<long,DBObject>>.Empty;
        public readonly static Framing Empty = new Framing();
        Framing() { }
        Framing(BTree<long,object> m) :base(m) { }
        public Framing(Context cx) 
            : base(BTree<long,object>.Empty+(Obs,cx.obs)+(Data,cx.data)
                  +(Results,cx.results)
                  +(Result,cx.result)
                  +(Rvv.RVV,cx.affected))
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
        public static Framing operator+(Framing f,(long,RowSet) r)
        {
            return f + (Data, f.data + (r.Item1, r.Item2));
        }
        public static Framing operator-(Framing f, (long,RowSet) r)
        {
            return f + (Data, f.data - r.Item1);
        }
        public static Framing operator-(Framing f,long p)
        {
            return f + (Obs, f.obs - p);
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = (Framing)base._Relocate(wr);
            for (var b = r.obs.First(); b != null; b = b.Next())
            {
                var p = b.key();
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
                if ((p >= Transaction.TransPos && p < Transaction.Executables)
                    || p >= wr.oldStmt) // we want to called Fixed on our Executables
                    r += wr.Fixed(p);
            }
            for (var b = r.obs.First(); b != null; b = b.Next())
            {
                var p = b.key();
                if ((p >= Transaction.TransPos && p<Transaction.Executables)
                    ||p>=Transaction.HeapStart) // we don't want to delete our Executables
                    r -= p;
            }
            for (var b = r.data.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var np = wr.Fix(p);
                var or =  b.value();
                var nr = (RowSet)or._Relocate(wr);
                if (p!=np)
                    r -= (p,or);
                r += (np,nr);
            }
            var rs = BTree<long, long>.Empty;
            for (var b=results.First();b!=null;b=b.Next())
                rs += (wr.Fix(b.key()),wr.Fix(b.value()));
            r += (Result, wr.Fix(r.result));
            r += (Results, rs);
            return r;
        }
        /// <summary>
        /// create maps taking heap uids to a shared range.
        /// (1) after Commit to move objects out of the heap:
        ///   obuids and rsuids are prepared by Context.ObUnheap() and RsUnheap()
        /// (2) in View Instancing to move referenced framing objects into the heap
        ///   obuids and rsuids are prepared in View.Instance()
        /// Fix(cx) uses the maps.
        /// </summary>
        /// <param name="cx"></param>
        internal void Relocate(Context cx)
        {
            for (var b = obs.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var np = cx.ObUnheap(p);
                if (p!=np)
                    cx.obuids += (p,np);
            }
            for (var b = results.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var np = cx.RsUnheap(p);
                if (p != np)
                    cx.rsuids += (p, np);
            } 
            // belt and braces
            for (var b = data.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var np = cx.RsUnheap(p);
                if (p != np)
                    cx.rsuids += (p, np);
            }
        }
        internal override Basis Fix(Context cx)
        {
            var r = this;
            var rs = BTree<long, long>.Empty;
            var os = BTree<long, DBObject>.Empty;
            var da = BTree<long, RowSet>.Empty;
            r.Install(cx);
            for (var b = obs.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var nk = cx.obuids[k] ?? k;
                os += (nk, (DBObject)b.value().Fix(cx));
                if (k != nk)
                    os -= k;
            }
            for (var b = data.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var nk = cx.rsuids[k] ?? k;
                da += (nk, (RowSet)b.value().Fix(cx));
                if (k != nk)
                    da -= k;
            }
            for (var b = results.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var nk = cx.rsuids[k] ?? k;
                var v = b.value();
                rs += (nk, cx.rsuids[v]??v);
                if (k != nk)
                    rs -= k;
            }
            r += (Obs, os);
            r += (Data, da);
            r += (Result, cx.rsuids[r.result]??r.result);
            r += (Results, rs);
            return r;
        }
        public void Install(Context cx)
        {
            cx.obs += obs;
            cx.data += data;
            cx.result = result;
            cx.results += results;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = " (";
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
            sb.Append(")");
            if (PyrrhoStart.VerboseMode && obrefs!=BTree<long,BTree<long,VIC?>>.Empty)
            {
                sb.Append(" ObRefs: ");
                cm = "(";
                for (var b=obrefs.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(DBObject.Uid(b.key()));sb.Append("=");
                    var cn = "(";
                    for (var c=b.value().First();c!=null;c=c.Next())
                    {
                        sb.Append(cn); cn = ",";
                        sb.Append(DBObject.Uid(c.key()));sb.Append("=");
                        sb.Append(c.value().Value);
                    }
                    if (cn==",")
                        sb.Append(")");
                }
                sb.Append(")");
            }
            if (result>=0)
            {
                sb.Append(" Result ");sb.Append(DBObject.Uid(result));
            }
            sb.Append(" Results: (");
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
    /// shareable as of 26 April 2021
    /// </summary>
    internal class PeriodSpec
    {
        /// <summary>
        /// the name of the period
        /// </summary>
        public readonly string periodname;
        /// <summary>
        /// AS, BETWEEN, SYMMETRIC, FROM
        /// </summary>
        public readonly Sqlx kind = Sqlx.NO;  
        /// <summary>
        /// The first point in time specified
        /// </summary>
        public readonly SqlValue time1 = null;
        /// <summary>
        /// The second point in time specified
        /// </summary>
        public readonly SqlValue time2 = null;
        internal PeriodSpec(string n,Sqlx k,SqlValue t1,SqlValue t2)
        {
            periodname = n; kind = k; time1 = t1; time2 = t2;
        }
    }
}
