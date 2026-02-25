using Pyrrho.Level2;
using Pyrrho.Level4; // for rename/drop
using Pyrrho.Common;
using System.Text;
using static Pyrrho.Level2.PIndex;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2026
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level3
{
    /// <summary>
    /// This class corresponds to logical Index database objects.
    /// Indexes are database objects that are created by primary/unique key, and foreign keys.
    /// Indexes are not named in SQL.
    /// From Sep2025, a referencing table has an additional REF column for each foreign key 
    /// </summary>
    internal class Index : DBObject 
    {
        static long _uniq = 0;
        internal const long
            Adapter = -157, // long Procedure 
            Keys = -159, // Domain
            RefTable = -162, // long Domain (Table or RowSet)
            TableDefPos = -163, // long Table
            Tree = -164; // MTree
        /// <summary>
        /// The defining position for the table in the database.
        /// alas: if we cache table or valueType here we miss alterations, new TableColumns etc
        /// </summary>
        public long tabledefpos => (long)(mem[TableDefPos] ?? -1L);
        public MTree? rows => (MTree?)mem[Tree];
        public Domain keys => (Domain?)mem[Keys] ?? Domain.Null;
        /// <summary>
        /// for Foreign key, the referenced table
        /// </summary>
        public long reftabledefpos => (long)(mem[RefTable] ?? -1L);
        /// <summary>
        /// The adapter function
        /// </summary>
        public long adapter => (long)(mem[Adapter]??-1L);
        public Index(long dp, BTree<long, object> m) : base(dp, m) { }
        /// <summary>
        /// Constructor: a new Index 
        /// </summary>
        public Index(PIndex c, Context cx)
            : base(c.ppos, c.defpos, _IndexProps(c, cx)
                 + (TableDefPos, c.tabledefpos)
                  + (ObInfo.Name, c.name))
        { }
        static BTree<long, object> _IndexProps(PIndex c, Context cx)
        {
            var ro = cx.role;
            var r = new BTree<long, object>(Definer,ro.defpos);
            var rt = CTree<int,long>.Empty;
            var rs = CTree<long, Domain>.Empty;
            for (var b = c.columns.First(); b != null; b = b.Next())
                if (b.value() is long pos)
                {
                    if (pos == 0 && cx._Ob(c.tabledefpos) is Table tb &&
                        cx._Ob(tb.systemPS) is PeriodDef pd)
                        pos = pd.startCol;
                    rt += (b.key(),pos);
                    var cd = cx._Dom(pos);
                    if (cd is null || cd.kind == Qlx.Null)
                        throw new PEException("PE50201");
                    rs += (pos, cd);
                }
            var kd = new Domain(-1L, cx, Qlx.ROW, rs, rt, (int)rt.Count);
            r += (Keys, kd);
            var rows = new MTree(kd, TreeBehaviour.Allow, 0);
            r += (Tree, rows);
            return r;
        }
        public static Index operator +(Index x, (long, object) v)
        {
            return (Index)x.New(x.mem + v);
        }
        public static Index operator +(Index x,(CList<TypedValue>,long) y)
        {
            var (k, v) = y;
            if (x.rows == null)
                throw new PEException("PE3070");
            return x + (Tree, x.rows + (k,0,v));
        }
        public static Index operator -(Index x, CList<TypedValue> k)
        {
            if (x.rows is not MTree mt)
                throw new PEException("PE3071");
            MTree? nm = mt - k;
            return (nm==null)? new(x.defpos,x.mem-Tree) : x + (Tree, nm);
        }
        public static Index operator -(Index x, (CList<TypedValue>,long) y)
        {
            var (k,v) = y;
            if (x.rows is not MTree mt)
                throw new PEException("PE3072");
            MTree? nm = mt - (k,0,v);
            return (nm==null)? new(x.defpos,x.mem-Tree): x + (Tree, nm);
        }
        internal CList<TypedValue>? MakeKey(CTree<long,TypedValue> vs)
        {
            var r = CList<TypedValue>.Empty;
            for (var b = keys.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    if (vs[p] is not TypedValue v)
                        return null;
                    r += v;
                }
            return r;
        }
        internal CList<TypedValue>? MakeKey(CTree<long,TypedValue> vs,CTree<long, long> sIMap)
        {
            var r = CList<TypedValue>.Empty;
            for (var b = keys.rowType.First(); b != null; b = b.Next())
            {
                if (b.value() is long p && sIMap[p] is long q && vs[q] is TypedValue v)
                    r += v;
                else
                    return null;
            }
            return r;
        }
        /// <summary>
        /// Creator: an Index by modifying this from tableRows information
        /// </summary>
        /// <param name="db">The database</param>
        /// <returns>the new Index</returns>
        public Index Build(Context cx)
        {
            var rs = rows ?? throw new PEException("PE48128");
            if (cx._Ob(tabledefpos) is not Table tb)
                throw new PEException("PE47146");
                 // there is no primary index, so we do it from the tableRows information
                for (var pq = tb.tableRows.PositionAt(0); pq != null; pq = pq.Next())
                {
                    var rq = pq.value();
                    var m = rq.MakeKey(this);
                    if (HasNull(m))
                        m = rq.MakeKey(this, cx);
                    if (!HasNull(m)) {
                        if (rs.Contains(m))
                        {
                            throw new DBException("44002", "PRIMARY/UNIQUE").Mix()
                                  .Add(Qlx.TABLE_NAME, new TChar(tb.name ?? "??"))
                                  .Add(Qlx.CONSTRAINT_NAME, new TChar("PRIMARY/UNIQUE"));
                        }
                        rs += (m, 0, pq.key());
                    }
                }
            var r = this + (Tree, rs);
            cx.Add(r);
            return r;
        }
        static bool HasNull(CList<TypedValue> k)
        {
            for (var b = k.First(); b != null; b = b.Next())
                if (b.value() == TNull.Value)
                    return true;
            return false;
        }
        protected override void _Cascade(Context cx, Drop.DropAction a, BTree<long, TypedValue> u)
        {
            if (a == Level2.Drop.DropAction.Restrict && rows?.Count > 0)
                throw new DBException("23002",defpos);
            base._Cascade(cx, a, u);
        }
        internal override Database Drop(Database db, Database nd)
        {
            if (nd.objects[tabledefpos] is Table tb)
            {
                var xs = tb.indexes;
                for (var b = tb.indexes.First(); b != null; b = b.Next())
                    if (b.value().Contains(defpos))
                    {
                        if (b.value().Count == 1L)
                            xs -= b.key();
                        else if (b.key().Length!=0)
                            xs += (b.key(), b.value() - defpos);
                    }
                tb += (Table.Indexes, xs);
                nd += tb;
            }
     /*       if (nd.objects[reftabledefpos] is Table rt)
            {
                var xs = rt.rindexes;
                if (xs.Count == 1)
                    xs = CTree<long,bool>.Empty;
                else
                    xs -= tabledefpos;
                rt += (Table.RefIndexes, xs);
                nd += rt;
            }    */
            return base.Drop(db, nd);
        }
        internal Index AddRows(Table tb,Context cx)
        {
            var x = this;
            for (var b = tb.indexes[x.keys]?.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is Index sx && sx.rows is not null)
                    x += (Tree, sx.rows);
       //     for (var b = tb.super.First(); b != null; b = b.Next())
       //         if (b.key() is Table t)
       //             x = x.AddRows(t, cx);
            return x;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Index(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new Index(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx, m);
            var na = cx.Fix(adapter);
            if (na != adapter)
                r += (Adapter, na);
            var nk = keys.Fix(cx);
            if (nk != keys)
                r += (Keys, nk);
            var rt = cx.Fix(reftabledefpos);
            if (reftabledefpos != rt)
                r += (RefTable, rt);
            r -= Tree;  // Commit will enter the committed rows
            return r;
        }
        // ShallowReplace doen't seem very shallow here!
        internal override Basis ShallowReplace(Context cx, long was, long now)
        {
            var r = (Index)base.ShallowReplace(cx, was, now);
            var ks = (Domain)keys.ShallowReplace(cx,was,now);
            if (ks != keys)
            {
                r += (Keys, ks);
                if (rows is not null)
                {
                    var rs = new MTree(ks, rows.nullsAndDuplicates, 0);
                    for (var b = rows.First(); b != null; b = b.Next())
                    {
                        var nk = CList<TypedValue>.Empty;
                        var cb = ks.rowType.First();
                        for (var c = b.key().First(); c != null && cb != null; c = c.Next(), cb = cb.Next())
                            if (cb.value() is long cp && ks.representation[cp] is Domain cd
                                && c.value() is TypedValue cv)
                                nk += cd.Coerce(cx, cv);
                        if (b.Value() is long p)
                            rs += (nk, 0, p);
                        r += (Tree, rs);
                    }
                }
            }
            return r;
        }
        internal (int,int) Multiplicity()
        {
            if (rows == null)
                return (0, 0);
            var l = 0;
            var h = int.MaxValue;
            var f = true;
            for (var b=rows.First();b!=null;b=b.Next(),f=false)
            {
                var (x,y) = rows.Multiplicity(b._key, l, h);
                if (x < l || f)
                    l = x;
                if (y > h || f)
                    h = y;
            }
            return (l, h);
        }
        internal override void Note(Context cx, StringBuilder sb, string pre="/// ")
        {
            sb.Append(pre);
            var cm = "(";
            for (var b = keys.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(cx.NameFor(p));
                }
            sb.Append(")");
            sb.Append("\r\n");
        }
        /// <summary>
        /// A readable version of the Index
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" for "); sb.Append(Uid(tabledefpos));
            sb.Append(" count " + rows?.count);
            sb.Append(" Key:"); sb.Append(keys);
            sb.Append(" Rows:"); sb.Append(rows);
            return sb.ToString();
        }
    }
}
