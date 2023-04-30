using System;
using System.ComponentModel.DataAnnotations;
using System.Xml;
using Pyrrho.Common;
using Pyrrho.Level1;
using Pyrrho.Level3;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

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
                  (dt as UDType)?.super,pp,cx)
        {
            if (cx.db != null)
            {
                if (!cx.db.types.Contains(old))
                    throw new DBException("42000",nm);
                _defpos = cx.db.types[old] ?? -1L;
            }
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
        /// <summary>
        /// Read Check: conflict if affected Physical is updated
        /// </summary>
        /// <param name="pos">the position</param>
        /// <returns>whether a conflict has occurred</returns>
		public override DBException? ReadCheck(long pos,Physical r,PTransaction ct)
		{
			return (pos==defpos)?new DBException("40009", pos,r,ct).Mix() :null;
		}
        public override long Affects => _defpos;
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch (that.type)
            {
                case Type.Record3:
                case Type.Record2:
                case Type.Record:
                case Type.Update1:
                case Type.Update:
                    {
                        var t = (Record)that;
                        for (var cp = t.fields.PositionAt(0); cp != null; cp = cp.Next())
                         if (db.objects[cp.key()] is DBObject c && c.domain == prev)
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
        readonly BTree<string, long?> hierCols;
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
        public EditType(Ident nm, UDType old, Domain sd, Domain? un, long pp, Context cx)
            : base(Type.EditType, nm, 
                  (UDType)(old.New(old.defpos,sd.mem + (Domain.Structure,old.structure))), un, pp, cx)
        {
            if (cx.db != null)
            {
                if (!cx.db.types.Contains(old))
                    throw new DBException("42000",nm.iix.dp);
                _defpos = cx.db.types[old] ?? -1L;
            }
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
        {
            hierCols = (prev as UDType)?.HierarchyCols(rdr.context)??BTree<string,long?>.Empty;
        }
        protected EditType(EditType x, Writer wr) : base(x, wr)
        {
            _defpos = wr.cx.Fix(x._defpos);
            prev = (Domain)x.prev.Relocate(wr.cx);
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
            base.Deserialise(rdr);
        }
        /// <summary>
        /// Read Check: conflict if affected Physical is updated
        /// </summary>
        /// <param name="pos">the position</param>
        /// <returns>whether a conflict has occurred</returns>
		public override DBException? ReadCheck(long pos, Physical r, PTransaction ct)
        {
            return (pos == defpos) ? new DBException("40009", pos, r, ct).Mix() : null;
        }
        public override long Affects => defpos;
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch (that.type)
            {
                case Type.Record3:
                case Type.Record2:
                case Type.Record:
                case Type.Update1:
                case Type.Update:
                    {
                        var t = (Record)that;
                        for (var cp = t.fields.PositionAt(0); cp != null; cp = cp.Next())
                            if (db.objects[cp.key()] is DBObject c && c.domain== prev)
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
        internal override DBObject Install(Context cx, long p)
        {
            var r = (Domain)base.Install(cx, p);
            cx.db -= ppos; // we are changing the old Domain, not adding a new one
            if (dataType is not NodeType st || cx.db.objects[st.structure] is not Table ts)
                throw new DBException("PE408205");
            // To make things easier we consider the merging of columns in two stages,
            // first deal with where our new columns match columns in the hierarchy
            if (dataType.infos[cx.role.defpos] is ObInfo oi)
                for (var b = oi.names.First(); b != null; b = b.Next())
                    if (b.value().Item2 is long np && !prev.representation.Contains(np) // new name
                        && hierCols[b.key()] is long sp && cx.db.objects[sp] is Table t // in hierarchy
                        && cx.db.objects[t.nodeType] is UDType s
                        && s.infos[cx.role.defpos] is ObInfo si && si.names[b.key()].Item2 is long ep
                        && np != ep)
                    {
                        var q = Math.Min(np, ep);
                        cx.MergeColumn(Math.Max(np, ep), q);
                        dataType = (UDType)(cx.db.objects[dataType.defpos] ?? throw new DBException("PE408200"));
                        dataType -= q;
                        dataType += (Domain.Display, dataType.Length);
                        ts += (Table.TableCols, ts.tableCols - q);
                    }
            under = (UDType?)cx.db.objects[under?.defpos ?? -1L];
            // The second stage of merging columns considers the columns from a new under
            if (under is UDType uD)
            {
                var hc = uD.HierarchyCols(cx);
                for (var b = prev.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long np && cx.db.objects[np] is TableColumn tc
                        && tc.infos[cx.role.defpos] is ObInfo ci && ci.name is string n
                        && hc[n] is long sp && cx.db.objects[sp] is Table t // in hierarchy
                        && cx.db.objects[t.nodeType] is UDType s
                        && s.infos[cx.role.defpos] is ObInfo si && si.names[n].Item2 is long ep
                        && np != ep)
                    {
                        var q = Math.Min(np, ep);
                        cx.MergeColumn(Math.Max(np, ep), q);
                        dataType = (UDType)(cx.db.objects[dataType.defpos] ?? throw new DBException("PE408203"));
                        dataType -= q;
                        dataType += (Domain.Display, dataType.Length);
                        ts += (Table.TableCols, ts.tableCols - q);
                    }
                // under and dataType may have changed
                under = (UDType)(cx.db.objects[under.defpos] ?? throw new DBException("PE408202"));
                // we need to add our tableRows to under 
                if (under is NodeType nt && cx.db.objects[nt.structure] is Table tu)
                {
                    for (var b = ts.tableRows.First(); b != null; b = b.Next())
                        for (var xb = tu.indexes.First(); xb != null; xb = xb.Next())
                            for (var c = xb.value().First(); c != null; c = c.Next())
                                if (cx.db.objects[c.key()] is Level3.Index x
                                    && x.MakeKey(b.value().vals) is CList<TypedValue> k)
                                {
                                    x += (k, b.key());
                                    cx.db += (x.defpos, x);
                                }
                    under = (UDType)(cx.db.objects[under.defpos] ?? throw new DBException("PE408204"));
                    // record that we are a subType of Under
                    under += (UDType.Subtypes, uD.subtypes - ppos + (prev.defpos, true));
                    cx.db += (under.defpos, under);
                    dataType += (UDType.Under, under);
                    tu += (Table.TableRows, tu.tableRows + ts.tableRows);
                    tu = tu + (DBObject._Domain, under) + (DBObject.LastChange, ppos);
                    cx.db += (tu.defpos, tu);
                }
            }
            // and record our new dataType
            cx.db += (dataType.defpos, dataType);
            ts = ts + (DBObject._Domain, dataType) + (DBObject.LastChange, ppos);
            cx.db += (ts.defpos, ts);
            return dataType;
        }
        public override string ToString()
        {
            return "EditType " + name + "[" + DBObject.Uid(prev.defpos) 
                + "] Under: " + DBObject.Uid(under?.defpos??-1L);  
        }
    }
}
