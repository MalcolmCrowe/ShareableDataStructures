using System;
using System.Configuration;
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
                v = obs[defs[ic]];
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
        internal void Install(Framing fr)
        {
            obs += (fr.obs,true);
            data += (fr.data,true);
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
        internal long ObUnheap(long p)
        {
            if (unLex)
                return ObUnLex(p);
            if (p < Transaction.Executables)
                return p;
            if (obuids.Contains(p))
                return obuids[p];
            while (obs.Contains(srcFix))
                srcFix++;
            obuids += (p, srcFix);
            return srcFix;
        }
        internal long RsUnheap(long p)
        {
            if (unLex)
                return RsUnLex(p);
            if (p < Transaction.Executables)
                return p;
            if (rsuids.Contains(p))
                return rsuids[p];
            if (obuids.Contains(p))
            {
                rsuids += (p, obuids[p]);
                return obuids[p];
            }
            while (obs.Contains(srcFix))
                srcFix++;
            rsuids += (p, srcFix);
            return srcFix;
        }
        internal long ObUnLex(long p)
        {
            if (p<Transaction.TransPos || p >= Transaction.Executables)
                return p;
            if (obuids.Contains(p))
                return obuids[p];
            obuids += (p, nextHeap);
            return nextHeap++;
        }
        internal long RsUnLex(long p)
        {
            if (p < Transaction.TransPos || p >= Transaction.Executables)
                return p;
            if (rsuids.Contains(p))
                return rsuids[p];
            if (obuids.Contains(p))
            {
                rsuids += (p, obuids[p]);
                return obuids[p];
            }
            rsuids += (p, nextHeap);
            return nextHeap++;
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
        /// <summary>
        ///  Not to be used for Domain or ObInfo
        /// </summary>
        /// <param name="pos"></param>
        /// <returns></returns>
        internal DBObject Fixed(long pos,Context nc)
        {
            var p = ObUnheap(pos);
            if (p == pos)
                return obs[p]??(DBObject)db.objects[p];
            if (obs[p] is DBObject x)
                return x;
            var ob = obs[pos];
            if (pos > Transaction.TransPos)
            {
                obs += (p, SqlNull.Value); // mark p as used
                ob = ob.Relocate(p).Relocate(this,nc);
                obs -= pos;
                obs += (p, ob);
            }
            return ob;
        }
        internal void Frame()
        {
            if (frame == null)
                frame = new Framing(this);
        }
        internal void Frame(long p,long q=-1L)
        {
            if (p < 0 || obs.Contains(p))
                return;
            if (q < 0)
                q = p;
            var ob = (DBObject)db.objects[q];
            obs += (ob.defpos, ob);
            Install(ob.framing);
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
        internal void AddDefs(Ident id, BList<long> s)
        {
            for (var b = s?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var ob = obs[p] ?? (DBObject)db.objects[p];
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
                var pp = (ParamInfo)obs[b.value()];
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
        internal CList<long> Fix(CList<long> ord)
        {
            var r = CList<long>.Empty;
            var ch = false;
            for (var b = ord?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var f = ObUnheap(p);
                if (p != f)
                    ch = true;
                r += f;
            }
            return ch ? r : ord;
        }
        internal CList<TypedValue> Fix(CList<TypedValue> ord, Context nc)
        {
            var r = CList<TypedValue>.Empty;
            var ch = false;
            for (var b = ord?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var f = p.Relocate(this,nc);
                if (p != f)
                    ch = true;
                r += f;
            }
            return ch ? r : ord;
        }
        internal BList<TypedValue> Fix(BList<TypedValue> ord, Context nc)
        {
            var r = BList<TypedValue>.Empty;
            var ch = false;
            for (var b = ord?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var f = p.Relocate(this, nc);
                if (p != f)
                    ch = true;
                r += f;
            }
            return ch ? r : ord;
        }
        internal BList<long> Fix(BList<long> ord)
        {
            var r = BList<long>.Empty;
            var ch = false;
            for (var b = ord?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var f = ObUnheap(p);
                if (p != f)
                    ch = true;
                r += f;
            }
            return ch ? r : ord;
        }
        internal BTree<long,BList<TypedValue>> Fix(BTree<long,BList<TypedValue>> refs,Context nc)
        {
            var r = BTree<long, BList<TypedValue>>.Empty;
            for (var b=refs?.First();b!=null;b=b.Next())
            {
                var p = b.key();
                var vs = Fix(b.value(),nc);
                r += (p, vs);
            }
            return r;
        }
        internal PRow Fix(PRow rw,Context nc)
        {
            if (rw == null)
                return null;
            return new PRow(rw._head?.Relocate(this,nc), Fix(rw._tail,nc));
        }
        internal BTree<long,RowSet.Finder> Fix(BTree<long,RowSet.Finder> fi)
        {
            var r = BTree<long,RowSet.Finder>.Empty;
            var ch = false;
            for (var b = fi?.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var np = ObUnheap(p);
                var f = b.value().Relocate(this);
                if (p != np || (object)f!=(object)b.value())
                    ch = true;
                r += (np,f);
            }
            return ch ? r : fi;
        }
        internal BTree<long, bool> Fix(BTree<long, bool> fi)
        {
            var r = BTree<long, bool>.Empty;
            var ch = false;
            for (var b = fi?.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var np = ObUnheap(p);
                if (p != np)
                    ch = true;
                r += (np, true);
            }
            return ch ? r : fi;
        }
        internal BTree<long, TypedValue> Fix(BTree<long, TypedValue> fi)
        {
            var r = BTree<long, TypedValue>.Empty;
            var ch = false;
            for (var b = fi?.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var np = ObUnheap(p);
                if (p != np)
                    ch = true;
                r += (np, b.value());
            }
            return ch ? r : fi;
        }
        internal BTree<string, TypedValue> Fix(BTree<string, TypedValue> a,Context nc)
        {
            var r = BTree<string, TypedValue>.Empty;
            for (var b = a?.First(); b != null; b = b.Next())
            {
                var p = b.key();
                r += (p, b.value().Relocate(this,nc));
            }
            return a;
        }
        internal BList<TXml> Fix(BList<TXml> ch,Context nc)
        {
            var r = BList<TXml>.Empty;
            for (var b = ch.First(); b != null; b = b.Next())
                r += (TXml)b.value().Relocate(this,nc);
            return r;
        }
        internal CTree<TypedValue,long?> Fix(CTree<TypedValue,long?> mu,Context nc)
        {
            var r = CTree<TypedValue,long?>.Empty;
            for (var b = mu?.First(); b != null; b = b.Next())
            {
                var p = b.key().Relocate(this,nc);
                var np = b.value();
                if (np!=null)
                r = (r+(p, np.Value));
            }
            return r;
        }
        internal BTree<long, BTree<long,bool>> Fix(BTree<long, BTree<long,bool>> fi)
        {
            var r = BTree<long, BTree<long,bool>>.Empty;
            var ch = false;
            for (var b = fi?.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var np = ObUnheap(p);
                if (p != np)
                    ch = true;
                r += (np, Fix(b.value()));
            }
            return ch ? r : fi;
        }
        internal int PosFor(TableColumn tc)
        {
            var oi = (ObInfo)db.role.infos[tc.tabledefpos];
            for (var b = oi.domain.rowType.First(); b != null; b = b.Next())
                if (b.value() == tc.defpos)
                    return b.key();
            return -1;
        }
    }
    internal class Framing : Basis // for compiled code
    {
        internal const long
            Data = -450,    // BTree<long,RowSet>
            Defs = -451,    // Ident.Idents
            Obs = -449,     // BTree<long,DBObject>
            Result = -452;  // long
        public BTree<long, DBObject> obs => 
            (BTree<long,DBObject>)mem[Obs]??BTree<long,DBObject>.Empty;
        public BTree<long, RowSet> data =>
            (BTree<long,RowSet>)mem[Data]??BTree<long,RowSet>.Empty;
        public Ident.Idents defs =>
            (Ident.Idents)mem[Defs]??Ident.Idents.Empty;
        public long result => (long)(mem[Result]??-1L);
 //       public BTree<int, BTree<long, DBObject>> depths =>
 //           (BTree<int,BTree<long,DBObject>>)mem[Depths]
 //           ??BTree<int,BTree<long,DBObject>>.Empty;
        public readonly static Framing Empty = new Framing();
        Framing() { }
        Framing(BTree<long,object> m) :base(m) { }
        public Framing(Context cx) 
            : base(BTree<long,object>.Empty+(Obs,cx.obs)+(Data,cx.data)
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
        internal override Basis _Relocate(Writer wr)
        {
            var r = (Framing)base._Relocate(wr);
            for (var b = r.obs.First(); b != null; b = b.Next())
                r += (DBObject)b.value()._Relocate(wr);
            for (var b = r.data.First(); b != null; b = b.Next())
                r += (RowSet)b.value()._Relocate(wr);
            r += (Defs, r.defs.Relocate(wr));
            r += (Result, wr.Fix(r.result));
            return r;
        }
        internal override Basis _Relocate(Context cx, Context nc)
        {
            var r = Empty;
            for (var b = obs.First(); b != null; b = b.Next())
                r += (DBObject)b.value()._Relocate(cx, nc);
            for (var b = data.First(); b != null; b = b.Next())
                r += (RowSet)b.value()._Relocate(cx, nc);
            r += (Defs, defs.Relocate(cx));
            r += (Result, cx.RsUnheap(result));
            return r;
        }
        public void Install(Context cx)
        {
            cx.obs += obs;
  //          cx.depths += depths;
            cx.defs = (Ident.Idents)cx.defs.Add(defs);
            cx.data += data;
            cx.result = cx.data[result];
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(defs);
            var cm = " Obs: (";
            for (var b = obs.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(DBObject.Uid(b.key())); sb.Append(b.value());
            }
            cm = ") Data: (";
            for (var b=data.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(DBObject.Uid(b.key())); sb.Append(b.value());
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
