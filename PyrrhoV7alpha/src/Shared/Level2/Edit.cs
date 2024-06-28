using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Level5;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level2
{
	/// <summary>
	/// An Edit record is to request an ALTER DOMAIN
	/// </summary>
	internal class Edit : PDomain
	{
        internal long _defpos;
        public Domain prev = Domain.Null;
        internal long _prev;
        public override long defpos => _defpos;
        /// <summary>
        /// Constructor: an Edit request from the Parser
        /// </summary>
        /// <param name="old">The previous version of the Domain</param>
        /// <param name="nm">The (new) name</param>
        /// <param name="sd">The (new) structure definition</param>
        /// <param name="dt">The (new) Domain</param>
        /// <param name="pb">The local database</param>
        public Edit(Domain old, string nm, Domain dt,long pp,Context cx)
            : base(Type.Edit, nm, dt.kind, dt.prec, (byte)dt.scale, dt.charSet,
                  dt.culture.Name,dt.defaultString,
                  dt.super.First()?.key(),pp,cx)
        {
            if (cx.db != null)
                _defpos = cx.db.Find(old)?.defpos ?? throw new DBException("42000",nm);
            prev = old;
            _prev = prev.defpos;
        }
        /// <summary>
        /// Constructor: an Edit request from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public Edit(Reader rdr) : base(Type.Edit,rdr) {}
        protected Edit(Edit x, Writer wr) : base(x, wr)
        {
            _defpos = wr.cx.Fix(x._defpos);
            prev = (Domain)x.prev.Relocate(wr.cx);
            _prev = prev.defpos;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Edit(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Reclocation info for Positions</param>
        public override void Serialise(Writer wr)
		{
            wr.PutLong(_defpos);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise from the buffer
        /// </summary>
        /// <param name="buf">The buffer</param>
        public override void Deserialise(Reader rdr)
		{
			_defpos = rdr.GetLong();
            _prev = rdr.Prev(_defpos)??_defpos;
			base.Deserialise(rdr);
		}
        public override long Affects => _defpos;
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch (that.type)
            {
                case Type.Record4:
                case Type.Record3:
                case Type.Record2:
                case Type.Record:
                case Type.Update2:
                case Type.Update1:
                case Type.Update:
                    {
                        var t = (Record)that;
                        for (var cp = t.fields.PositionAt(0); cp != null; cp = cp.Next())
                         if (cx.db.objects[cp.key()] == prev)
                                return new DBException("40079", defpos, that, ct);
                        break;
                    }
                case Type.PDomain:
                case Type.PDomain1:
                case Type.Edit:
                case Type.EditType:
                case Type.PType:
                case Type.PType1:
                case Type.PNodeType:
                case Type.PEdgeType:
                    {
                        var t = (PDomain)that;
                        if (t.name==name)
                            return new DBException("40079", defpos, that, ct);
                        break;
                    }
                case Type.Drop:
                    if (((Drop)that).delpos == defpos)
                        return new DBException("40016", defpos, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that, ct);
        }
    }
    /// <summary>
    /// An Edit record is to request an ALTER UDType
    /// </summary>
    internal class EditType : PType
    {
        internal long _defpos;
        internal override long defpos => _defpos;
        public Domain prev = Domain.Null;
        public BTree<string, (int,long?)> hierCols = BTree<string,(int,long?)>.Empty;
        internal long _prev;
        /// <summary>
        /// Constructor: an Edit request from the Parser.
        /// Changes should propagate down to subtypes and up to supertype (TBD)
        /// </summary>
        /// <param name="nm">The (new) name</param>
        /// <param name="old">The previous version of the Domain</param>
        /// <param name="sd">The (new) structure definition</param>
        /// <param name="un">The UNDER domain if any</param>
        /// <param name="pp">The ppos for this log record</param>
        public EditType(string nm, UDType old, UDType sd, CTree<Domain,bool> un, long pp, Context cx)
            : base(Type.EditType, nm, sd, un, cx.db.nextStmt, pp, cx)
        {
            if (cx.db != null)
                _defpos = cx.db.Find(old)?.defpos ?? throw new DBException("42000","EditType");
            prev = old;
            hierCols = old.HierarchyCols(cx);
            _prev = prev.defpos;
        }
        /// <summary>
        /// Constructor: an Edit request from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public EditType(Reader rdr) : base(Type.EditType, rdr) 
        {  }
        protected EditType(EditType x, Writer wr) : base(x, wr)
        {
            _defpos = wr.cx.Fix(x._defpos);
            prev = (Domain)x.prev.Relocate(wr.cx);
            for (var b = x.under.First(); b != null; b = b.Next())
                under += ((Domain)b.key().Relocate(wr.cx), true);
            hierCols = x.hierCols;
            _prev = prev.defpos;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new EditType(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Reclocation info for Positions</param>
        public override void Serialise(Writer wr)
        {
            wr.PutLong(_defpos);
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise from the buffer
        /// </summary>
        /// <param name="buf">The buffer</param>
        public override void Deserialise(Reader rdr)
        {
            _defpos = rdr.GetLong();
            _prev = rdr.Prev(_defpos) ?? _defpos;
            prev = (Domain)(rdr.context._Ob(_prev)??Domain.Content);
            dataType = prev;
            hierCols = prev.HierarchyCols(rdr.context);
            base.Deserialise(rdr);
            dataType = ((Table)prev)._PathDomain(rdr.context);
            rdr.context.db += dataType;
        }
        public override long Affects => defpos;
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch (that.type)
            {
                case Type.Record4:
                case Type.Record3:
                case Type.Record2:
                case Type.Record:
                case Type.Update2:
                case Type.Update1:
                case Type.Update:
                    {
                        var t = (Record)that;
                        for (var cp = t.fields.PositionAt(0); cp != null; cp = cp.Next())
                            if (db.objects[cp.key()]== prev)
                                return new DBException("40079", defpos, that, ct);
                        break;
                    }
                case Type.PDomain:
                case Type.PDomain1:
                    {
                        var t = (PDomain)that;
                        if (t.name == name)
                            return new DBException("40079", defpos, that, ct);
                        break;
                    }
                case Type.Edit:
                case Type.EditType:
                case Type.PType:
                case Type.PType1:
                case Type.PNodeType:
                case Type.PEdgeType:
                    {
                        var t = (PType)that;
                        if (((UDType)t.dataType).subtypes.Contains(defpos))
                            return new DBException("40079", defpos, that, ct);
                        break;
                    }
                case Type.Drop:
                    if (((Drop)that).delpos == defpos)
                        return new DBException("40016", defpos, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that, ct);
        }
        internal override DBObject Install(Context cx)
        {
            var fix = CTree<long, bool>.Empty;
            var r = base.Install(cx);
            if (dataType is not Table st)
                throw new PEException("PE408205");
            // If there is a new under for our columns, update them
            if (cx.db.objects[_defpos] is Table tg)
                for (var b = tg.representation?.First(); b != null; b = b.Next())
                    if (cx.db.objects[b.key()] is TableColumn tc && tc.tabledefpos == _defpos)
                        for (var c = under.First(); c != null; c = c.Next())
                            if (c.key().defpos > tc.defpos)
                                cx.db += (tc + (TableColumn._Table, c.key().defpos));
            var oi = dataType.infos[cx.role.defpos] ?? new ObInfo(name, Grant.AllPrivileges);
            // To make things easier we consider the merging of columns in two stages,
            // first deal with where our new columns match columns in the hierarchy
            if (prev.infos[cx.role.defpos] is ObInfo pi)
                for (var b = oi.names.First(); b != null; b = b.Next())
                    if (b.value().Item2 is long np && !prev.representation.Contains(np) // new name
                        && hierCols[b.key()].Item2 is long ep  // in hierarchy
                        && np != ep)
                    {
                        var q = Math.Min(np, ep);
                        var nq = Math.Max(np, ep);
                        cx.MergeColumn(q, nq); // ShallowReplace does the work
                        fix += (dataType.defpos, true);
                    }
            // The second stage of merging columns considers the columns from a new under
            for (var ub = under.First(); ub != null; ub = ub.Next())
                if (ub.key() is UDType uD)
                {
                    var hc = uD.HierarchyCols(cx);
                    for (var b = prev.rowType.First(); b != null; b = b.Next())
                        if (b.value() is long np && cx.db.objects[np] is TableColumn tc
                            && tc.infos[cx.role.defpos] is ObInfo ci && ci.name is string n
                            && hc[n].Item2 is long ep  // in hierarchy
                            && np != ep)
                        {
                            var q = Math.Min(np, ep);
                            var nq = Math.Max(np, ep);
                            cx.MergeColumn(q, nq); // ShallowReplace does the work
                            fix += (uD.defpos, true);
                        }
                    // under and dataType may have changed
                    dataType = (UDType)(cx.db.objects[dataType.defpos] ?? Domain.TypeSpec);
                    if (uD is EdgeType gu && cx.db.objects[_defpos] is EdgeType ge)
                    {
                        var eo = cx.db.edgeEnds[gu.defpos] ?? BTree<long, BTree<long, long?>>.Empty;
                        var ee = cx.db.edgeEnds[ge.defpos] ?? BTree<long, BTree<long, long?>>.Empty;
                        cx.db += (Database.EdgeEnds, cx.db.edgeEnds + (uD.defpos, eo + ee));
                    }
                    oi = dataType.infos[cx.role.defpos] ?? oi;
                    var ps = ((UDType)dataType).HierarchyRepresentation(cx);
                    var rt = BList<long?>.Empty;
                    for (var b = ps.First(); b != null; b = b.Next())
                        rt += b.key();
                    var uP = cx.uids[uD.defpos] ?? uD.defpos; // just in case
                    var un = (UDType)(cx.db.objects[uP] ?? throw new DBException("PE40802"));
                    var ui = un.infos[cx.role.defpos] ?? new ObInfo(un.name, Grant.AllPrivileges);
                    var no = un.rowType == BList<long?>.Empty;
                    Level3.Index? xx = null;
                    // special case: if un is a nodetype without an ID column and we have an ID column
                    if (un is NodeType nu && nu.idCol < 0 && dataType is NodeType tn && tn.idCol > 0)
                    {
                        // this will be okay provided nu has no columns and no rows
                        if (nu.rowType.Count > 0 || nu.tableRows.Count > 0)
                            throw new DBException("42000").Add(Qlx.CREATE_GRAPH_TYPE_STATEMENT);
                        var nx = cx.db.objects[tn.idIx] as Level3.Index ?? throw new PEException("PE40405");
                        // we get nu to adopt the ID column of nt, and clone the ID index
                        nu += (Domain.RowType, new BList<long>(tn.idCol));
                        nu += (Domain.Representation,
                            new CTree<long, Domain>(tn.idCol, tn.representation[tn.idCol] ?? Domain.Position));
                        nu += (NodeType.IdCol, tn.idCol);
                        nu += (DBObject.Infos, nu.infos + (cx.role.defpos, ui + (ObInfo.Names, ui.names + oi.names)));
                        var xi = (Level3.Index)cx.Add(new Level3.Index(ppos + 1,
                            nx.mem + (Level3.Index.TableDefPos, un.defpos)));
                        nu += (NodeType.IdIx, xi.defpos);
                        cx.Add(nu);
                        cx.Add(xi);
                        xx = xi;
                        cx.db += nu;
                        cx.db += xi;
                        un = nu;
                    }
                    // special case: if un is an edge type without leaving/arriving indexes we clone those of st
                    if (un is EdgeType eu && eu.leaveIx < 0 && dataType is EdgeType te)
                    {
                        var lx = cx.db.objects[st.leaveIx] as Level3.Index ?? throw new DBException("PE40803");
                        var xl = (Level3.Index)cx.Add(new Level3.Index(ppos + 2,
                            lx.mem + (Level3.Index.TableDefPos, un.defpos)));
                        eu += (Table.Indexes, un.indexes + (xl.keys, new CTree<long, bool>(xl.defpos, true)));
                        if (!eu.representation.Contains(te.leaveCol)) 
                            eu += (Domain.RowType, eu.rowType + te.leaveCol);
                        eu += (Domain.Representation,eu.representation +
                                (te.leaveCol, te.representation[te.leaveCol] ?? Domain.Position));
                        eu += (EdgeType.LeaveCol, te.leaveCol);
                        eu += (EdgeType.LeaveIx, xl.defpos);
                        eu += (DBObject.Infos, eu.infos + (cx.role.defpos, ui + (ObInfo.Names, ui.names + oi.names)));
                        cx.Add(eu);
                        cx.Add(xl);
                        xx = xl;
                        cx.db += eu;
                        cx.db += xl;
                        un = eu;
                    }
                    if (un is EdgeType ev && ev.arriveIx < 0 && dataType is EdgeType tf)
                    {
                        var ax = cx.db.objects[st.arriveIx] as Level3.Index ?? throw new PEException("PE40804");
                        var xa = (Level3.Index)cx.Add(new Level3.Index(ppos + 3,
                            ax.mem + (Level3.Index.TableDefPos, un.defpos)));
                        ev += (Table.Indexes, un.indexes + (xa.keys, new CTree<long, bool>(xa.defpos, true)));
                        if (!ev.representation.Contains(tf.arriveCol))
                            ev += (Domain.RowType, ev.rowType + tf.arriveCol);
                        ev += (Domain.Representation, ev.representation +
                            (tf.arriveCol, tf.representation[tf.arriveCol] ?? Domain.Position));
                        ev += (EdgeType.ArriveCol, tf.arriveCol);
                        ev += (EdgeType.ArriveIx, xa.defpos);
                        ev += (DBObject.Infos, ev.infos + (cx.role.defpos, ui + (ObInfo.Names, ui.names + oi.names)));
                        cx.Add(ev);
                        cx.Add(xa);
                        xx = xa;
                        cx.db += ev;
                        cx.db += xa;
                        un = ev;
                    }
                    // otherwise we need to add tableRows to the new under
                    if (xx is null && un is NodeType nt && dataType is Table ns)
                    {
                        for (var b = ns.tableRows.First(); b != null; b = b.Next())
                            for (var xb = nt.indexes.First(); xb != null; xb = xb.Next())
                                for (var c = xb.value().First(); c != null; c = c.Next())
                                    if (cx.db.objects[c.key()] is Level3.Index x
                                        && x.MakeKey(b.value().vals) is CList<TypedValue> k)
                                    {
                                        x += (k, b.key());
                                        cx.db += (x.defpos, x);
                                    }
                        un += (Table.TableRows, nt.tableRows + ns.tableRows);
                        // record that we are a subType of Under
                        un += (Domain.Subtypes, uD.subtypes - ppos + (prev.defpos, true));
                        cx.Add(un);
                        cx.db += un;
                        dataType += (Domain.Under, under - uD + (un, true));
                    }
                }
            if (cx.db.objects[_defpos] is EdgeType th && th.leaveCol >= 0 && th.arriveCol >= 0 && under.First()?.key() is EdgeType u)
            {
                var ee1 = cx.db.edgeEnds[th.defpos] ?? BTree<long, BTree<long, long?>>.Empty;
                var ee2 = ee1[th.leaveCol] ?? BTree<long, long?>.Empty;
                cx.db += (u + (EdgeType.LeaveCol, th.leaveCol) + (EdgeType.ArriveCol, th.arriveCol) +
                    (Table.TableRows,u.tableRows + th.tableRows));
                cx.db += (Database.EdgeEnds, cx.db.edgeEnds +
                    (u.defpos, ee1 + (th.leaveCol, ee2 + (th.arriveCol, u.defpos))));
            }
            // record our new dataType
            cx.db += (dataType.defpos, dataType);
            return dataType;
        }
        public override string ToString()
        {
            var sb = new StringBuilder("EditType " + name + "[" + DBObject.Uid(prev.defpos) + "]");
            var cm = " Under: [";
            for (var b = under.First(); b != null; b = b.Next())
            { sb.Append(cm); cm = ","; sb.Append(b.key().name); }
            if (cm == ",")
                sb.Append(']');
            return sb.ToString();
        }
    }
}
