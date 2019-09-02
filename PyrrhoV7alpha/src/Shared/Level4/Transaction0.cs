using System.Collections.Generic;
using System;
using System.Text;
using PyrrhoBase;
using Pyrrho.Common;
using Pyrrho.Level1;
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
	/// A Transaction maintains a local copy of the connection
	/// where all databases are really Participants.
	/// </summary>
	internal class Transaction : Connection
	{
        internal Connection parent = null;
#if MONGO
        internal bool mongocmd = false;
#endif
        internal string Trid
        {
            get { return "" + starttime + "#" + cid; }
        }
        public override string ToString()
        {
            return "Transaction " + cid + ((context==null)?" bad":""); 
        }
        /// <summary>
        /// A new Transaction makes a list of Participants corresponding to the databases in the connection
        /// </summary>
        internal List<Database> databases;
        internal Participant this[From f] { get => databases?[f.target?.dbix ?? 0].AsParticipant; }
        internal Participant this[DBObject ob] { get => databases?[ob.dbix].AsParticipant; }
        internal Participant this[Ident id] { get => databases?[id.dbix].AsParticipant;  }
        /// <summary>
        /// The front participant (used to be called "local").
        /// This will throw an exception if databases[0] is not a Participant (e.g. readonly)
        /// </summary>
        internal override Participant Front {  get =>  databases[0].AsParticipant;}
        internal Ident firstConnected;
        /// <summary>
        /// A provenance for ImportTransactions
        /// </summary>
        internal string provenance = null;
        internal bool autoCommit = true;
        /// <summary>
        /// The start time in ticks
        /// </summary>
		internal long starttime;
        /// <summary>
        /// For Drop/Rename the referenced object
        /// </summary>
        internal DBObject refObj = null;
        /// <summary>
        /// 4.5: Transaction.Conflict enquiry support
        /// </summary>
        internal DBException conflicted = null;
        /// <summary>
        /// 4.5 Transaction.Conflict enquiry support
        /// </summary>
        internal Ident.Tree<long?> locals = Ident.Tree<long?>.Empty;
        /// <summary>
        /// 5.2 Improved type handling. SqlDataType->bool.
        /// This structure is used to ensure that all query processing in the transaction
        /// uses the single SqlDataType instance in the keys of this tree. 
        /// It is empty at the start of the transaction and populated only as required.
        /// </summary>
        internal ATree<SqlDataType, SqlDataType> types = BTree<SqlDataType, SqlDataType>.Empty;
        /// <summary>
        /// Accumulated ETags information for this transaction
        /// </summary>
        internal List<string> etags = new List<string>();
        /// <summary>
        /// Constructor: a new Transaction object
        /// Behaviour for connected databases depends on our role
        /// We are Server: check authority and make LocalTransactions
        /// We are Master: make VirtBases
        /// We are Storage: no action
        /// </summary>
        /// <param name="conn">the connection</param>
        internal Transaction(Connection conn)
            : base(conn)
        {
            parent = conn;
            databaseList = conn.databaseList;
            context = new Context(this);
            databases = new List<Database> { databaseList.First().value() };
            for (var i = 0; i < conn.Count; i++)
            {
                var cd = GetCD(i);
                cd.Configure().Add(cd,this);
                if (cd.details.Contains("Coordinator"))
                {
                    autoCommit = false;
                    coordinator = i;
                }
                if (cd.details.Contains("Password"))
                {
                    var d = databases[0];
                    var us = d._User as User;
                    if (us?.pwd == "") // we update the password to the one supplied 
                    {
                        var nu = new User(d, us.name, Physical.Type.PUser)
                        {
                            pwd = cd.details["Password"]
                        };
                        d.Change(nu, TAction.NoOp, null);
                    }
                }
            }
            firstConnected = databases[0].name;
#if !EMBEDDED && !LOCAL
/*            lock (databases[0].pb.df.dbStorage.dslock)
            {
                var lk = databases[0].pb.df.dbStorage.dslock.OnLock(true); */
                databases[0].cd.OpenOthers();
/*                front.pb.df.dbStorage.dslock.Unlock(lk);
            } */
#endif
            starttime = DateTime.Now.ToFileTime();
            //        Console.WriteLine("New Transaction (C) = " + tid);
        }
        /// <summary>
        /// Constructor: used for cascades and renames 
        /// </summary>
        /// <param name="tb">The Database to connect to</param>
        /// <param name="ro">The object being renamed or dropped</param>
        protected Transaction(Participant db, DBObject ro):base(db.transaction)
		{
            context = new Context(this);
            databaseList = new Ident.IdTree<Database>(db.name, db);
            databases = new List<Database>
            {
                db
            };
            firstConnected = db.name;
   //         Console.WriteLine("New Transaction (L) = " + tid);
            refObj = ro;
		}
        /// <summary>
        /// Get the current role
        /// </summary>
        /// <param name="db">the database</param>
        /// <returns>the defpos of the current role or -1</returns>
        internal override Role _Role(int dbix)
        {
            var rp = _authority?.GetRole(dbix) ?? -1;
            return databases[dbix].GetObject(rp) as Role ?? throw new PEException("PE002");
        }
        internal override User _User(int dbix)
        {
            var rp = _authority?.GetUser(dbix) ?? -1;
            return databases[dbix].GetObject(rp) as User ?? throw new PEException("PE002");
        }
        internal override void Restore(Ident.Tree<Participant> mark)
        {
            base.Restore(mark);
            for (int i = 0; i < databases.Count; i++)
                databases[i] = mark[databases[i].name];
        }
        internal Connection GetCD(int i)
        {
            if (databases.Count>i && databases[i] is Participant p)
                return p.cd;
            return new Connection( connectionList[i],this, i);
        }
        internal Database GetDB(Ident nm)
        {
            var r = databaseList[nm];
            if (r!=null)
                return r;
            Transact(nm, BTree<string, string>.Empty);
            return databaseList[nm];
        }
        internal override Database Db(int i)
        {
            return databases[i];
        }
        internal override int DbCount { get => databases.Count; }
        internal Connection Transact(Ident n, ATree<string, string> dt) 
        {
            for (int j = 0; j < databases.Count; j++)
            {
                var d = databases[j].AsParticipant;
                if (d.name == n)
                    return d.cd;
            }
            Connection cc = null;
            int i = 0;
            for (; i < connectionList.Count; i++)
            {
                var c = connectionList[i];
                if (c.name.CompareTo(n)==0)
                {
                    cc = c;
                    break;
                }
            }
#if !EMBEDDED && !LOCAL
            var sr = Configuration.defaultServerRole;
            if (PyrrhoServer.serverRoles.Contains(n))
                sr = PyrrhoServer.serverRoles[n];
#endif
            if (cc == null)
                cc = new Connection(this, n, 
#if !EMBEDDED && !LOCAL
                    sr, 
#endif
                    dt);
            var cd = new Connection(cc,this, i);
            cd.Configure().Add(cd, this);
            return cd;
        }
        internal override Transaction RequireTransaction()
        {
            return this;
        }
        /// <summary>
        /// Implement BeginTransaction
        /// </summary>
        /// <returns></returns>
		internal override Connection BeginTransaction()
		{
            if (!autoCommit)
                throw new DBException("25001", "Nested transactions are not supported").ISO();
			autoCommit = false;
   //         Console.WriteLine("Explicit Transaction " + tid);
            return this;
		}
        /// <summary>
        /// 4.5 Test to see if a conflict has already occurred
        /// </summary>
        internal override bool Conflict
        {
            get
            {
                if (conflicted!=null)
                    return true;
                if (locals.Count==0)
                for (var cd = databaseList.First();cd!= null;cd=cd.Next())
                 {
                    var d = cd.value() as Participant;
                        Ident.Tree<long?>.Add(ref locals, cd.key(), d.posAtStart);
                }
                for(var cd = databaseList.First();cd!= null;cd=cd.Next())
                {
                    var d = cd.value();
                    var st = locals[cd.key()].Value;
                    conflicted = d.Commit1(ref st);
                    if (conflicted!=null)
                        return true;
                    Ident.Tree<long?>.Update(ref locals, cd.key(), st);
                }
                return false;
            }
        }
        /// <summary>
        /// Implement Grant or Revoke
        /// </summary>
        /// <param name="grant">true=grant,false=revoke</param>
        /// <param name="tb">the database</param>
        /// <param name="pr">the privilege</param>
        /// <param name="obj">the database object</param>
        /// <param name="grantees">a list of grantees</param>
		void DoAccess(bool grant,Database d,Grant.Privilege pr,long obj,DBObject[] grantees)
		{
			if (grantees==null) // PUBLIC
			{
				if (grant)
					d.Add(this,new Grant(pr,obj,-1L,d));
				else
					d.Add(this,new Revoke(pr,obj,-1L,d));
				return;
			}
			foreach(var mk in grantees)
			{
				long gee = -1;
				gee = mk.defpos;
				if (grant)
					d.Add(this,new Grant(pr,obj,gee,d));
				else
					d.Add(this,new Revoke(pr,obj,gee,d));
			}
		}
        /// <summary>
        /// Implement Grant/Revoke on a list of TableColumns
        /// </summary>
        /// <param name="grant">true=grant, false=revoke</param>
        /// <param name="tb">the database</param>
        /// <param name="pr">the privileges</param>
        /// <param name="tb">the table</param>
        /// <param name="list">a list of TableColumns</param>
        /// <param name="grantees">a list of grantees</param>
		void AccessColumns(bool grant,Participant db,Grant.Privilege pr,Table tb,PrivNames list,DBObject[] grantees)
		{
#if NET2_0
            bool SelectCond = (pr & Grant.Privilege.Select) == Grant.Privilege.Select;
            bool InsertCond = (pr & Grant.Privilege.Insert) == Grant.Privilege.Insert;
            bool UpdateCond = (pr & Grant.Privilege.Update) == Grant.Privilege.Update;
#else
            bool SelectCond = pr.HasFlag(Grant.Privilege.Select);
            bool InsertCond = pr.HasFlag(Grant.Privilege.Insert);
            bool UpdateCond = pr.HasFlag(Grant.Privilege.Update);
#endif
            int inserts = 0;
            if (InsertCond)
                for (var cp = db._Role.defs[tb.defpos].props.First();cp!= null;cp=cp.Next())
                if (cp.value().HasValue) 
                {
                    var co = db.GetObject((long)cp.value()) as TableColumn;
                    if (co.notNull)
                        inserts++;
                }
			for(int i=0;i<list.names.Length;i++)
			{
                var cn = list.names[i];
				var co = tb.GetColumn(this,db,cn);
                if (co == null)
                    throw Exception("42112", cn).Mix();
                if (SelectCond && ((co.Generated!=PColumn.GenerationRule.No) || co.NotNull))
                    SelectCond = false;
                if (co.NotNull)
                    inserts--;
                if (InsertCond && (co.Generated!=PColumn.GenerationRule.No))
                    throw Exception("28107", cn).Mix();
                if (UpdateCond && (co.Generated!=PColumn.GenerationRule.No) && co.update=="")
                    throw Exception("28108", cn).Mix();
				DoAccess(grant,db,pr,co.defpos,grantees);
			}
            if (grant && SelectCond)
                throw Exception("28105").Mix();
            if (inserts > 0)
                throw Exception("28106").Mix();
		}
        /// <summary>
        /// Implement Grant/Revoke on a list of RoleColumns
        /// </summary>
        /// <param name="grant">true=grant, false=revoke</param>
        /// <param name="db">the database</param>
        /// <param name="pr">the privileges</param>
        /// <param name="r">the Role</param>
        /// <param name="e">the R</param>
        /// <param name="list">a list of RoleColumn</param>
        /// <param name="grantees">a list of grantees</param>
        void AccessProperties(bool grant, Participant db, Grant.Privilege pr, Role r, RoleObject e, PrivNames list, DBObject[] grantees)
        {
            foreach (var n in list.names)
            {
                var p = e.props[n];
                if (p == null)
                    throw Exception("4210B", n).Mix();
                DoAccess(grant, db, pr, (long)p, grantees);
     //           if (grant)
      //              DefineProperty(db, r, e, p, grantees);
            }
        }
        /// <summary>
        /// Implement grant/revoke on a Role
        /// </summary>
        /// <param name="grant">true=grant, false=revoke</param>
        /// <param name="roles">a list of Roles (ids)</param>
        /// <param name="grantees">a list of Grantees</param>
        /// <param name="opt">whether with ADMIN option</param>
		internal void AccessRole(bool grant,Ident[] roles,DBObject[] grantees,bool opt)
		{
            var db = Front;
            Grant.Privilege op = Grant.Privilege.NoPrivilege;
            if (opt == grant) // grant with grant option or revoke
                op = Grant.Privilege.UseRole | Grant.Privilege.AdminRole;
            else if (opt && !grant) // revoke grant option for
                op = Grant.Privilege.AdminRole;
            else // grant
                op = Grant.Privilege.UseRole;
            foreach(var s in roles)
            {
                Role ro = db.GetRole(s);
                if (ro == null)
                    throw Exception("42135", s).Mix();
                DoAccess(grant, db, op, ro.defpos, grantees);
            }
		}
        /// <summary>
        /// Implement grant/revoke on a database obejct
        /// </summary>
        /// <param name="grant">true=grant, false=revoke</param>
        /// <param name="priv">the privileges</param>
        /// <param name="ob">the database object defining position</param>
        /// <param name="grantees">a list of grantees</param>
        /// <param name="opt">whether with GRANT option (grant) or GRANT for (revoke)</param>
        internal void AccessObject(bool grant, PrivNames[] priv, long ob, DBObject[] grantees, bool opt)
        {
            var db = Front;
            DBObject t = (ob == 0L) ? null : db.objects[ob];
            Grant.Privilege defp = Grant.Privilege.NoPrivilege;
            if (!grant)
                defp = (Grant.Privilege)0x3fffff;
            var p = defp; // the privilege being granted
            var gp = t?.roles[db.Transrole]??Grant.Privilege.NoPrivilege; // grantor's privileges
            var changeTable = true;
            if (priv == null) // all (grantor's) privileges
            {
                if (grant)
                    p = gp;
                if (t is Table tb)
                    for (var cp = db._Role.defs[tb.defpos].props.First(); cp != null; cp = cp.Next())
                        if (cp.value().HasValue)
                        {
                            var tc = db.GetObject((long)cp.value());
                            gp = tc.roles[db.Transrole];
                            Grant.Privilege pp = defp;
                            if (grant)
                                pp = gp;
                            DoAccess(grant, db, pp, (long)cp.value(), grantees);
                        }
            }
            else
                foreach (var mk in priv)
                {
                    Grant.Privilege q = Grant.Privilege.NoPrivilege;
                    switch (mk.priv)
                    {
                        case Sqlx.SELECT: q = Grant.Privilege.Select; break;
                        case Sqlx.INSERT: q = Grant.Privilege.Insert; break;
                        case Sqlx.DELETE: q = Grant.Privilege.Delete; break;
                        case Sqlx.UPDATE: q = Grant.Privilege.Update; break;
                        case Sqlx.REFERENCES: q = Grant.Privilege.References; break;
                        case Sqlx.EXECUTE: q = Grant.Privilege.Execute; break;
                        case Sqlx.TRIGGER: break; // ignore for now (?)
                        case Sqlx.USAGE: q = Grant.Privilege.Usage; break;
                        case Sqlx.OWNER: q = Grant.Privilege.Owner;
                            if (!grant)
                                throw Exception("4211A", mk).Mix();
                            break;
                        default: throw Exception("4211A", mk).Mix();
                    }
                    Grant.Privilege pp = (Grant.Privilege)(((int)q) << 0x400);
                    if (opt == grant)
                        q |= pp;
                    else if (opt && !grant)
                        q = pp;
                    if (mk.names.Length != 0)
                    {
                        if (changeTable)
                            changeTable = grant;
                        AccessColumns(grant, db, q, (Table)t, mk, grantees);
                    }
                    else
                        p |= q;
                }
            if (changeTable)
                DoAccess(grant, db, p, t?.defpos??0, grantees);
        }
        /// <summary>
        /// Define a new Role for the local database
        /// </summary>
        /// <param name="name">the name of the role</param>
		internal void CreateRole(Ident name, string dt)
		{
            var db = Front;
			PRole a = new PRole(name,dt,db);
			db.Add(this,a);
		}
        /// <summary>
        /// Drop an object
        /// </summary>
        /// <param name="tb">the database</param>
        /// <param name="ob">the object</param>
        /// <param name="act">the drop behaviour</param>
 		internal void Drop(Database db,DBObject ob,Sqlx act)
		{
			db.OnDrop(ob,act);
		}
        /// <summary>
        /// Perform an INSERT.
        /// The SQL syntax gives a way of making table columns correspond with the supplied values.
        /// Note that in INSERT..(SELECT the names must match (eg via aliasing); but SELECT * is ok.
        /// If you don't want this behaviour the appropriate SQL is INSERT .. VALUES((SELECT..)).
        /// Most of the work here is about triggers, constraints and generated columns
        /// and all of these use definer's roles: so little of the code that uses the session role
        /// </summary>
        /// <param name="s">the SqlInsert object</param>
		internal List<RowSet> Execute(SqlInsert s)
        {
            var r = new List<RowSet>();
            var db = Front;
            Level cl = db._User.clearance;
            if (db._User.defpos != db.owner && s.from.target is Table tb
                && tb.enforcement.HasFlag(Grant.Privilege.Insert)
                && !cl.ClearanceAllows(tb.classification))
                    throw new DBException("42105");
            rowCount = s.from.Insert(s.provenance,s.from.data, new Common.Adapters(), 
                r,s.classification, s.autokey);
            return r;
        }

        /// <summary>
        /// Lookup a Table by name. Although tables and view share the same namespace, they are looked up separately.
        /// </summary>
        /// <param name="name">The name</param>
        /// <param name="tb">out: the database where it is found</param>
        /// <returns>The Table found (or null)</returns>
        internal override Table GetTable(Ident name, out Database d)
        {
            Table t = null;
            d = null;
            foreach (var cd in databases)
                if ((t = cd.GetTable(name)) != null)
                {
                    d = cd;
                    break;
                }
            return t;
        }
        /// <summary>
        /// Search databases in the transaction for a given anonymous type (OWL).
        /// If there is more than one match, return the most local
        /// </summary>
        /// <param name="t">The data type</param>
        /// <param name="d">The database found in</param>
        /// <returns></returns>
        internal override Domain GetDomain(SqlDataType t, out Database d)
        {
            foreach(var cd in databases)
                if (cd.GetDomain(t) is Domain dm)
                {
                    d = cd;
                    return dm;
                }
            d = null;
            return null;
        }
        /// <summary>
        /// Lookup a Domain or UDType by name
        /// </summary>
        /// <param name="name">The name</param>
        /// <param name="tb">out: the database where it is found</param>
        /// <returns>The Domain/UDType found (or null)</returns>
        internal override Domain GetDomain(Ident name, out Database d)
        {
            d = null;
            Domain r = null;
            foreach(var cd in databases) // order is important
                if ((r = cd.GetDomain(name)) != null)
                {
                    d = cd;
                    break;
                }
            return r;
        }
        /// <summary>
        /// Lookup a Trigger by name
        /// </summary>
        /// <param name="name">The name</param>
        /// <param name="tb">out: the database where it is found</param>
        /// <returns>The Trigger found (or null)</returns>
        internal override Trigger GetTrigger(Ident name, out Database d)
        {
            d = null;
            Trigger r = null;
            foreach (var cd in databases) // order is important
                if ((r = cd.GetTrigger(name)) != null)
                {
                    d = cd as Participant;
                    break;
                }
            return r;
        }
        /// <summary>
        /// Lookup an Index by name
        /// </summary>
        /// <param name="name">The name</param>
        /// <param name="tb">out: the database where it is found</param>
        /// <returns>The Index found</returns>
        internal override Index GetIndex(Ident name, out Database d)
        {
            d = null;
            Index r = null;
            foreach(var cd in databases) // order is important
                if ((r = cd.GetIndex(name)) != null)
                {
                    d = cd as Participant;
                    break;
                }
            return r;
        }
        /// <summary>
        /// Lookup a table by name for the current Authority
        /// </summary>
        /// <param name="name">the name of the entity</param>
        /// <param name="ro">the role defining the entity</param>
        /// <param name="db">the database defining the role</param>
        /// <returns>the entity found</returns>
        internal Table GetTable(Ident name, out Role ro, out Database db)
        {
            db = null;
            ro = null;
            foreach (var cd in databases) // order is important
            {
                ro = _Role(cd);
                if (ro == null)
                    continue;
                var rx = ro.names[name];
                if (rx == null)
                    continue;
                if (cd.GetObject((long)rx) is Table et)
                    return et;
            }
            ro = null;
            return GetTable(name, out db);
        }
        /// <summary>
        /// Lookup a Procedure by name and arity
        /// </summary>
        /// <param name="nameAndArity">the procedure name$arity</param>
        /// <param name="d">out: the database where the procedure was found</param>
        /// <returns>The procedure found</returns>
        internal override Procedure GetProcedure(Ident nameAndArity, out Database d)
        {
            d = null;
            foreach (var cd in databases) // order is important 
            {
                if (cd.beingParsed?.nameAndArity.ident==nameAndArity.ident)
                {
                    d = cd;
                    return cd.beingParsed;
                }
                if (cd.GetProcedure(nameAndArity) is Procedure r)
                {
                    d = cd;
                    return r;
                }
            }
            return null;
        }
        /// <summary>
        /// Lookup a Method by typename, name and arity
        /// </summary>
        /// <param name="tname">the UDType name</param>
        /// <param name="mname">the method name$arity</param>
        /// <param name="d">out: the database where the procedure was found</param>
        /// <returns>The method found</returns>
        internal override Method GetMethod(Ident tname, Ident mname, out Database d)
        {
            d = null;
            foreach (var cd in databases) // order is important 
            {
                if (cd.beingParsed?.nameAndArity.ident == mname.ident)
                {
                    d = cd;
                    return cd.beingParsed as Method;
                }
                if (cd.GetMethod(tname,mname) is Method r)
                {
                    d = cd;
                    return r;
                }
            }
            return null;
        }
        /// <summary>
        /// Lookup a View by name. Although tables and views share the same namespace, they are looked up separately.
        /// </summary>
        /// <param name="name">The name</param>
        /// <param name="tb">out: the database where it is found</param>
        /// <returns>The View found</returns>
        internal override View GetView(Ident name, out Database d)
        {
            d = null;
            View r = null;
            foreach(var cd in databases) // order is important
                if ((r = cd.GetView(name)) != null)
                {
                    d = cd;
                    break;
                }
            return r;
        }
        /// <summary>
        /// Execute a DeletedSearched statement
        /// </summary>
        /// <param name="q">the QuerySearch supplying the rows to delete</param>
		internal void Execute(QuerySearch q)
		{
			if (parse!=ExecuteStatus.Obey)
				return;
            var dfs = q.from.defs;
            q.from.Analyse(this);
            q.from.defs = dfs;
            var oq = q.from.Push(this);
            try
            {
                var dr = BTree<string, bool>.Empty;
                if (!(q.from.target is RestView))
                    for (var a = q.from.rowSet.First(); a != null; a = a.Next())
                    a._Rvv()?.Add(ref dr);
                rowCount = q.from.Delete(this, dr, new Adapters());
                if (rowCount == 0 && !(q.from.target is RestView))
                    GetActivation()?.NoData(this);
            }
            catch (Exception e) { throw e; }
            finally { q.from.Pop(this,oq); }
		}
        /// <summary>
        /// Execute an UpdateSearched statement
        /// </summary>
        /// <param name="su">the UpdateSearch</param>
		internal RowSet Execute(UpdateSearch su)
        {
            var rs = new List<RowSet>();
            if (parse != ExecuteStatus.Obey)
                return null;
            var dfs = su.from.defs;
            su.from.Analyse(this);
            su.from.defs = dfs;
            var oq = su.from.Push(this);
            try
            {
                var oldaf = affected.Count;
                var ur = BTree<string, bool>.Empty;
                if (!(su.from.target is RestView))
                    for (var a = su.from.rowSet.First(); a != null; a = a.Next())
                        a._Rvv()?.Add(ref ur);
                rowCount = su.from.Update(this, ur, new Adapters(), rs);
            }
            catch (Exception e) { throw e; }
            finally { su.from.Pop(this, oq); }
            return (rs.Count > 0) ? rs[0] : null;
        }
        /// <summary>
        /// Execute some other sort of statement
        /// </summary>
        /// <param name="e">the Executable</param>
		internal override void Execute(Executable e)
		{
            switch (e.type)
            {
                case Executable.Type.Select: Execute(e as SelectStatement); return;
                case Executable.Type.Insert: Execute(e as SqlInsert); return;
                case Executable.Type.DeleteWhere: Execute(e as QuerySearch); return;
                case Executable.Type.UpdateWhere: Execute(e as UpdateSearch); return;
            }
            base.Execute(e);
		}
        /// <summary>
        /// Close a DataReader
        /// </summary>
        internal override Connection RdrClose()
        {
            var r = result;
            result = null;
            if (!autoCommit)
            {
                r?.rowSet.qry.Close(this);
                return this;
            }
            Commit();
            if (result != null)
            {
                result.bmk?.Close(this);
                result.bmk = null;
            }
            r?.rowSet.qry.Close(this);
            context.Close(this);
            context = null;
            databases.Clear();
            databases = null;
            databaseList = null;
            parent.result = null;
            parent.rowCount = rowCount;
            Close();
            return parent;
        }
        /// <summary>
        /// Retrieve an SQL standard diagnostics entry
        /// </summary>
        /// <param name="w">A diagnostic identifier</param>
        /// <returns>the value found or computed</returns>
        internal override TypedValue Get(Sqlx w)
        {
            if (parent != null)
                switch (w)
                {
                    case Sqlx.ROW_COUNT: return new TInt(rowCount);
                    case Sqlx.TRANSACTIONS_COMMITTED: return new TInt(parent.commits);
                    case Sqlx.TRANSACTIONS_ROLLED_BACK: return new TInt(parent.rollbacks);
#if !EMBEDDED
                    case Sqlx.SERVER_NAME: return new TChar(PyrrhoStart.cfg.hp.host);
#endif
                    case Sqlx.CONNECTION_NAME: return new TChar(databases[0].name);
                }
            return base.Get(w);
        }
        /// <summary>
        /// Contsruct a DBException and add in some diagnostics information
        /// </summary>
        /// <param name="sig">The name of the exception</param>
        /// <param name="obs">The objects for the format string</param>
        /// <returns>the DBException</returns>
        public override DBException Exception(string sig, params object[] obs)
        {
            var r = new DBException(sig, obs);
            for (var s = diagnostics.First(); s != null; s = s.Next())
                r.Add(s.key(), s.value());
            if (context.exec is Executable ex)
            {
                r.Add(Sqlx.COMMAND_FUNCTION, new TChar(ex.type.ToString()));
                r.Add(Sqlx.COMMAND_FUNCTION_CODE, new TInt((int)ex.type));
            }
            r.Add(Sqlx.CONNECTION_NAME, new TChar(databases[0].name));
#if !EMBEDDED
            r.Add(Sqlx.SERVER_NAME, new TChar(PyrrhoStart.cfg.hp.host));
#endif
            r.Add(Sqlx.TRANSACTIONS_COMMITTED, Get(Sqlx.TRANSACTIONS_COMMITTED));
            r.Add(Sqlx.TRANSACTIONS_ROLLED_BACK, Get(Sqlx.TRANSACTIONS_ROLLED_BACK));
            return r;
        }
        /// <summary>
        /// Find the frontmost database for the current transaction that defines a given type
        /// </summary>
        /// <param name="sqlDataType">a type</param>
        /// <returns>The (participant) database or null</returns>
        internal override Database DbFor(SqlDataType sqlDataType)
        {
            for (int i = 0; i < databases.Count; i++)
            {
                var db = databases[i];
                if (db.pb == null)
                    continue;
                if (sqlDataType.name == null)
                {
                    if (db.pb.types.Contains(sqlDataType))
                        return db;
                }
                else if (db._Role.names.Contains(sqlDataType.name))
                {
                    var cq = db._Role.names[sqlDataType.name];
                    if (cq.HasValue)
                        return db;
                }
            }
            return null;
        }
        internal override string Rdc()
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var d = databaseList?.First(); d != null; d = d.Next())
            {
                var r = d.value().RdC();
                if (r == "")
                    continue;
                sb.Append(cm); sb.Append(r);
                cm = ",";
            }
            for (int j=0;j<etags.Count;j++)
                sb.Append(";"+etags[j]);
            return sb.ToString();
        }
        /// <summary>
        /// Commit this transaction
        /// </summary>
        internal override Connection Commit() 
        {
#if !EMBEDDED
            if (PyrrhoStart.TutorialMode)
            {
                var coo = databases[0].cd.details.Contains("Coordinator") ? "" : "Coordinating ";
                Console.WriteLine("(" + cid + ") "+coo+"Commit for tid=" + cid);
                foreach (var d in databases)
                    Console.WriteLine("  " + d);
            }
#endif
            parent.commits++;
            var parts = BTree<Ident,long?>.Empty; // 0.7 distributed transaction support
            try
            {
                int j = 0;
                while (j < databases.Count)
                {
                    //                Console.WriteLine("Commit Phase 1 loop: j=" + j + " < " + databases.Count);
                    var d = databases[j++] as Participant;
                    if (d == null)
                        continue;
#if !EMBEDDED && !LOCAL
                    if (PyrrhoStart.TutorialMode)
                    {
                        Console.Write("Phase 1 for " + d.name + "(" + d.did + ") Enter to start");
                        Console.ReadLine();
                    }
                    if ((!d.WorkToCommit) && !d.cd.roleChanged)
                    {
                        d.cd.Rollback();
                        continue;
                    }
#endif
                    long fromPos = d.WorkPos;
                    //             Console.WriteLine("Prepare + Commit1");
                    if (d.Prepare())
                    {
                        var c = d.Commit1(ref fromPos);
                        if (c != null)
                        {
#if !EMBEDDED && !LOCAL
                            if (PyrrhoStart.TutorialMode)
                                Console.WriteLine(".. failed");
#endif
                            if (d.profile != null)
                                parent.profile = d.profile.AddProfile(d as Participant, false);
                            d.Rollback();
                            parent.affected.Clear();
                            throw c;
                        }
                    }
                    var path = d.Master.name.ident;
#if !EMBEDDED
                    if (path == "_")
                        path += "@" + PyrrhoStart.cfg.hp.ToString();
#endif
                    var ppos = fromPos;
                    ATree<Ident,long?>.Add(ref parts, new Ident(path,0), ppos);
                }
                // Now do Commit1 again, with locking in alphabetical order of database name
                // as this helps to avoid deadlocks. 
                for (var pa = parts.First(); pa != null; pa = pa.Next())
                {
                    var s = pa.key().ident;
#if !EMBEDDED
                    if (s == "_@" + PyrrhoStart.cfg.hp.ToString()) // nb: parts is also used for remote dtcs
                        s = "_";
#endif
                    var d = GetDB(new Ident(s,0));
                    long fromPos = d.WorkPos;
                    d.LockConflicts();
#if !EMBEDDED
                    if (PyrrhoStart.TutorialMode)
                    {
                        Console.Write("Phase 1.5 for " + d.name + "(" + d.did + ") Enter to start");
                        Console.ReadLine();
                    }
#endif
                    d.Prepare();
                    var c = d.Commit1(ref fromPos);
                    if (c != null)
                    {
#if !EMBEDDED
                        if (PyrrhoStart.TutorialMode)
                            Console.WriteLine(".. failed");
#endif
                        if (d.profile != null)
                            parent.profile = d.profile.AddProfile(d as Participant, false);
                        d.ConflictsDone();
                        d.Rollback();
                        parent.affected.Clear();
                        throw c;
                    }
                }
                // Phase 2: Construct the possibly distributed transaction record, store the transaction details temporarily
                foreach (var d in databases)
                    if (d.ConflictsLocked && d.WorkToCommit)
                    {
#if !EMBEDDED
                        if (PyrrhoStart.TutorialMode)
                        {
                            Console.Write("Phase 2 for " + d.name + "(" + d.did + ") Enter to start");
                            Console.ReadLine();
                        }
#endif
                        d.Commit2(parts);
                    }
            }
            catch (Exception ex)
            {
                // Garbage collect the LocalTransactions by restoring the previous database entries
                // The databases array will be soon forgotten too
                for (int i = 0; i < databases.Count; i++)
                {
                    var lt = databases[i] as Participant;
                    if (lt == null)
                        continue;
                    ATree<Ident,Database>.Update(ref databaseList, lt.name, lt.database);
                    lt.Rollback();
                }
                throw ex;
            }
            // Phase 3: after once through this loop the Transaction must Commit
            // Write all the changes to physical media
            foreach (var d in databases)
            {
#if !EMBEDDED
                if (PyrrhoStart.TutorialMode)
                {
                    Console.Write("Phase 3 for " + d.name + "(" + d.did + ") Enter to start");
                    Console.ReadLine();
                }
#endif
                if (d.WorkToCommit && d.ConflictsLocked)
                        d.Commit3();
                    else
                        d.Commit();
            }
            // Garbage collect the LocalTransactions by restoring the previous database entries
            // The databases array will be forgotten
            // What is remembered are the records written to disk (and about to be Installed)
            lock (Database.databaselist)
            {
                foreach (var dd in databases)
                {
                    var d = dd as Participant;
                    if (d == null)
                        continue;
#if !EMBEDDED
                    if (PyrrhoStart.TutorialMode)
                    {
                        Console.Write("Phase 4 for " + d.name + "(" + d.did + ") Enter to start");
                        Console.ReadLine();
                    }
#endif
                    var db = d.Commit4(affected);
                    // update the Connection's databaseList entry
                    ATree<Ident,Database>.Add(ref parent.databaseList, db.name, db);
                    // update any change in role information
                    if (d.cd.roleChanged)
                        foreach (var cs in connectionList)
                            if (cs.name == d.cd.name)
                                cs.role = d.cd.role;
                    db.Commit();
                    d.ConflictsDone();
                    db.Dispose();
                }
            }
            parent.result = result;
#if !EMBEDDED
            if (PyrrhoStart.TutorialMode)
                Console.WriteLine("("+cid+") tid=" + cid + ": Committed");
#endif
            return parent;
        }
        /// <summary>
        /// Rollback the current transaction
        /// </summary>
        internal override Connection Rollback(DBException e)
        {
            if (e != null && e.signal == "40000")
                mark = Ident.Tree<Participant>.Empty;
            if (mark.Count != 0)
            {
                Restore(mark);
                return this;
            }
#if !EMBEDDED
            if (PyrrhoStart.TutorialMode || PyrrhoStart.DebugMode)
                Console.WriteLine("(" + cid + ") Rollback");
#endif
            parent.rollbacks++;
            if (e!=null)
                e.Add(Sqlx.TRANSACTION_ACTIVE, new TInt(0));
            if (databaseList.Count == 0)
                return parent;
            // Garbage collect the LocalTransactions by restoring the previous database entries
            // The databases array will be soon forgotten too
            for (int i = 0; i < databases.Count; i++)
            {
                var lt = databases[i] as Participant;
                if (lt == null)
                    continue;
                ATree<Ident,Database>.Update(ref databaseList, lt.name, lt.database);
                lt.Rollback();
            }
            Close();
            return parent;
        }
#if !LOCAL && !EMBEDDED
        internal override bool ReadConstraints(string[] rcs)
        {
            for (int i=1;i<rcs.Length;i++)
            {
                var ss = rcs[i].Split('|');
                if (ss.Length < 3)
                    return false;
                var db = GetDB(ss[0]);
                if (db == null)
                    continue;
                var rci = ReadConstraintInfo.From(db, rcs[i]);
                if (rci != null && !rci.Check(db))
                    return false;
            }
            return true;
        }
#endif
    }
    /// <summary>
    /// Save a parsing context while we do something else
    /// </summary>
    internal class SaveContext
    {
        /// <summary>
        /// the current execution status
        /// </summary>
		internal ExecuteStatus status;
        /// <summary>
        /// The authority
        /// </summary>
        internal Authority authority;
        /// <summary>
        /// Save the execution context: save the status
        /// </summary>
        /// <param name="t">the transaction</param>
        /// <param name="s">the new status</param>
		internal SaveContext(Connection cnx, ExecuteStatus s)
        {
            status = cnx.parse;
            var cx = cnx.context;
            authority = cnx._authority;
            cnx.parse = s;
        }
        /// <summary>
        /// Restore the transaction context
        /// </summary>
        /// <param name="t">the transction</param>
 		internal void Restore(Connection cnx)
        {
            cnx._authority = authority;
            cnx.parse = status;
        }
    }

    /// <summary>
    /// DropTransaction is used to deal with the consequential changes 
	/// following an ALTER TO or DROP RESTRICT/CASCADE operation
	/// Check and deal with dependent objects before the Drop/Rename occurs
    /// </summary>
	internal class DropTransaction : Transaction
	{
        /// <summary>
        /// constructor: a new drop transaction
        /// </summary>
        /// <param name="del">the object being dropped</param>
        public DropTransaction(Participant db, DBObject del)
            : base(db, del)
		{
			parse = ExecuteStatus.Drop;
		}
	}
    internal class ConstraintChecking
    {
        Participant db;
        TransitionRowSet trs;
        Table tb;
        ATree<long, OwnedSqlValue> tchecks;
        ATree<long, OwnedSqlValue> cchecks;
        ATree<long, ATree<long, OwnedSqlValue>> dchecks = BTree<long, ATree<long, OwnedSqlValue>>.Empty;
        public ConstraintChecking(Transaction tr,TransitionRowSet rs,Table t)
        {
            db = tr.db; trs = rs; tb = t;
            var b = trs.qry.Push(tr);
            try
            {
                tchecks = DoChecks(tr, tb.constraints);
                for (var tcp = db._Role.defs[t.defpos].props.First(); tcp != null; tcp = tcp.Next())
                    if (tcp.value().HasValue)
                    {
                        var tc = db.GetObject((long)tcp.value()) as TableColumn;
                        if (tc == null)
                            continue;
                        cchecks = DoChecks(tr, tc.constraints);
                        var dm = db.GetObject(tc.domaindefpos) as Domain;
                        var forcol = DoChecks(tr, dm.constraints);
                        if (forcol.Count > 0)
                            ATree<long, ATree<long, OwnedSqlValue>>.Add(ref dchecks, tc.defpos, forcol);
                    }
            }
            catch (Exception e) { throw e; }
            finally { trs.qry.Pop(tr,b); }
        }
        ATree<long,OwnedSqlValue> DoChecks(Transaction tr,ATree<long,bool> cs)
        {
            var r = BTree<long, OwnedSqlValue>.Empty;
            var needed = BTree<SqlValue, Ident>.Empty;
            for (var cp = cs.First();cp!= null;cp=cp.Next())
                if (db.GetObject(cp.key()) is Check ck)
                {
                    if (ck.search == null)
                        ck.search = new Parser(tr).ParseSqlValue(ck.source, SqlDataType.Content);
                    ATree<long, OwnedSqlValue>.Add(ref r, cp.key(), new OwnedSqlValue(ck.search, ck.definer, ck.owner));
                }
            return r;
        }
        public void Check()
        {
            var rw = trs.from.row as TransitionRowSet.TransitionRowBookmark;
            var was = rw.Get();
            rw.Set(new TRow(trs.from, trs.from.newRow));
            // handle column and domain checks
            for (var v = tchecks.First();v!= null;v=v.Next())
            {
                var au = trs.tr.PushRole(db, v.value().role, v.value().owner);
                if (!v.value().what.Matches(trs.tr,trs))
                {
                    var ck = db.GetObject(v.key()) as Check;
                    throw trs.tr.Exception("44002",
                        ck.SafeName(db),
                        tb.NameInSession(db), tb.NameInSession(db)).Mix()
                    .Add(Sqlx.CONSTRAINT_NAME, ck?.NameInOwner(db))
                    .Add(Sqlx.TABLE_NAME, tb.NameInOwner(db));
                }
                trs.tr.PopTo(au);
            }
            for (var v = cchecks.First();v!= null;v=v.Next())
            {
                var au = trs.tr.PushRole(db, v.value().role, v.value().owner);
                if (!v.value().what.Matches(trs.tr,trs))
                {
                    var ck = db.GetObject(v.key()) as Check;
                    var tc = db.GetObject(ck.checkobjpos) as TableColumn;
                    throw trs.tr.Exception("44003",
                        ck.NameInSession(db).ToString()??"",
                        tc.NameInSession(db).ToString()??"", tb.NameInSession(db).ToString()??"").Mix()
                    .Add(Sqlx.CONSTRAINT_NAME, ck.NameInOwner(db))
                    .Add(Sqlx.TABLE_NAME, tb.NameInOwner(db))
                    .Add(Sqlx.COLUMN_NAME, tc.NameInOwner(db));
                }
                trs.tr.PopTo(au);
            }
            for (var cp = dchecks.First();cp!= null;cp=cp.Next())
                for (var v = cp.value().First();v!= null;v=v.Next())
                {
                    var au = trs.tr.PushRole(db, v.value().role, v.value().owner);
                    if (!v.value().what.Matches(trs.tr,trs))
                    {
                        var ck = db.GetObject(v.key()) as Check;
                        var tc = db.GetObject(cp.key()) as TableColumn;
                        throw trs.tr.Exception("44001",
                        ck.NameInSession(db)?.ToString()??"",
                        tc.NameInSession(db), tb.NameInSession(db)).Mix()
                    .Add(Sqlx.CONSTRAINT_NAME, ck.NameInOwner(db))
                    .Add(Sqlx.TABLE_NAME, tb.NameInOwner(db))
                    .Add(Sqlx.COLUMN_NAME, tc.NameInOwner(db));
                    }
                    trs.tr.PopTo(au);
                }
            rw.Set(was);
        }
 
  
    }
    /// <summary>
    ///  better implementation of UNDO handler: copy the context stack as well as LocationTransaction states
    /// </summary>
    internal class ExecState
    {
        long topContext;
        Ident.Tree<Participant> mark;
        ATree<string, Activation> stack;

        internal ExecState(Connection t)
        {
            topContext = t.context.cxid;
            mark = t.Mark();
            stack = t.stack;
        }
        internal void Restore(Connection t)
        {
            var cx = t.context as Activation;
            while(cx!=null && cx.cxid != topContext)
                cx = cx.dynLink;
            if (cx == null)
                throw new PEException("Bad Restore");
            t.Restore(mark);
            t.stack = stack;
        }
    }
}