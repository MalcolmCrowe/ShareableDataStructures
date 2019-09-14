using System;
using System.Globalization;
using System.Text;
using Pyrrho.Level2;
using Pyrrho.Common;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level3
{
    internal class Selector : SqlValue
    {
        internal const long
            Attribute = -279, // bool
            Axis = -280, // Sqlx
            Caption = -281, // string (C)
            ColumnPaths = -282, // BTree<long,bool>
            Seq = -285, // int  
            Table = -286, // long
            UpdateRule = -287, // BList<UpdateAssignment>
            UpdateSource = -288; // string
        public bool attribute => (bool)(mem[Attribute] ?? false);
        public string caption => (string)mem[Caption];
        public Sqlx axis => (Sqlx)(mem[Axis] ?? Sqlx.NO);
        /// <summary>
        /// The defining position of the associated Table or -1 if none
        /// </summary>
        public long tabledefpos=>(long)mem[Table];
        /// <summary>
        /// The position in the default select list
        /// </summary>
        public int seq=>(int)mem[Seq];
        /// <summary>
        /// The domain defined for this selector
        /// </summary>
        public Domain domain=>(Domain)mem[NominalType];
        public BList<UpdateAssignment> updateRule => 
            (BList<UpdateAssignment>)mem[UpdateRule]?? BList<UpdateAssignment>.Empty;
        public string updateSource => (string)mem[UpdateSource];
        /// <summary>
        /// The columnpaths defined in the schema for this selector
        /// </summary>
        public BTree<long, bool> columnPaths => 
            (BTree<long,bool>)(mem[ColumnPaths]??BTree<long, bool>.Empty);
        internal Selector(SqlValue s, int i) : this(s.name, s.defpos, s.nominalDataType, i) { }
        /// <summary>
        /// An ad-hoc selector for STATIC
        /// </summary>
        /// <param name="nm"></param>
        /// <param name="dt"></param>
        /// <param name="i"></param>
        internal Selector(string nm, long sg, Domain dt, int i)
            : base(sg,BTree<long, object>.Empty + (NominalType, dt) + (Seq, i)+(Name,nm))
        { }
        internal Selector(long dp, BTree<long, object> m) : base(dp, m) { }
        public static Selector operator+ (Selector s,(long,object)x)
        {
            return (Selector)s.New(s.mem+x);
        }
        public static Selector operator+ (Selector s,Level4.Metadata md)
        {
            var m = s.mem;
            if (md.Has(Sqlx.ATTRIBUTE)) m += (Attribute, true); else m -= Attribute;
            if (md.Has(Sqlx.X)) m += (Axis, Sqlx.X);
            else if (md.Has(Sqlx.Y)) m += (Axis, Sqlx.Y);
            else m -= Axis;
            if (md.description != "") m += (Description, md);
            return (Selector)s.New(m);
        }
        internal override bool AddNameToRole => false;
        internal override DBObject Relocate(long dp)
        {
            return new Selector(dp,mem);
        }
        /// <summary>
        /// Follow a given ColumnPath
        /// </summary>
        /// <param name="s">the bnext component of the path</param>
        /// <returns>the ColumnPath or null</returns>
        internal Selector FollowChain(Level4.Ident s)
        {
            if (s == null)
                return this;
            var rp = domain.names[s.ident];
            if (rp == null)
                return null;
            return FollowChain(s.sub);
        }
        internal override Metadata Meta()
        {
            var md = new Metadata();
            if (attribute)
                md.Add(Sqlx.ATTRIBUTE);
            md.Add(axis);
            return md;
        }
        internal override Query Resolve(Context cx, Query f)
        {
            if (nominalDataType != Domain.Null)
                return f;
            for (var b = f.rowType.columns.First(); b != null; b = b.Next())
            {
                var sc = b.value();
                if (name == sc.name)
                {
                    cx.Replace(this, sc);
                    return (Query)cx.obs[f.defpos];
                }
            }
            return f;
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = base.Replace(cx, so, sv);
            var ra = r.WithA(alias);
            return (ra != this)?(SqlValue)cx.Add(ra):ra;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(Attribute)) { sb.Append(" Attribute="); sb.Append(attribute); }
            if (mem.Contains(Caption)) { sb.Append(" Caption="); sb.Append(caption); }
            if (mem.Contains(ColumnPaths)) { sb.Append(" ColumnPaths="); sb.Append(columnPaths); }
            if (mem.Contains(NominalType)) { sb.Append(" DataType="); sb.Append(domain); }
            if (mem.Contains(Seq)) { sb.Append(" Seq="); sb.Append(seq); }
            if (mem.Contains(Table)) { sb.Append(" Table="); sb.Append(Uid(tabledefpos)); }
            if (mem.Contains(Axis)) { sb.Append(" "); sb.Append(axis); }
            if (mem.Contains(UpdateSource))
            {
                sb.Append(" UpdateSource="); sb.Append(updateSource);
                sb.Append(" Update:"); sb.Append(updateRule);
            }
            return sb.ToString();
        }

        internal override Basis New(BTree<long, object> m)
        {
            return new Selector(defpos,m);
        }
    }
    /// <summary>
    /// A Database object representing a table column
    /// </summary>
    internal class TableColumn : Selector
    {
        internal const long
            Checks = -289,  // BTree<long,Check>
            ColumnProperties = -290, // BTree<string,DBObject>
            Generated = -291, // PColumn.GenerationRule (C)
            GeneratedAs = -114, // string
            NotNull = -292; // true (C)
        /// <summary>
        /// A set of column constraints
        /// </summary>
        public BTree<long, Check> constraints => (BTree<long, Check>)mem[Checks];
        public BTree<string, DBObject> columnProperties => (BTree<string, DBObject>)mem[ColumnProperties];
        public bool notNull => (bool)(mem[NotNull] ?? false);
        public PColumn.GenerationRule generated =>
            (PColumn.GenerationRule)(mem[Generated] ?? PColumn.GenerationRule.No);
        /// <summary>
        /// Constructor: a new TableColumn 
        /// </summary>
        /// <param name="tb">The Table</param>
        /// <param name="c">The PColumn def</param>
        /// <param name="dt">the data type</param>
        public TableColumn(Table tb, PColumn c, Domain dt)
            : base(c.ppos,BTree<long,object>.Empty+(Table,tb.defpos)+(Seq,c.seq)
                  +(Definer,c.role.defpos)+(NominalType,dt)+(Generated,c.generated)
                  +(Domain.Default,dt.defaultValue)+(Name,c.name))
        {}
        protected TableColumn(long dp, BTree<long, object> m) : base(dp, m) { }
        public static TableColumn operator+(TableColumn s,(long,object)x)
        {
            return new TableColumn(s.defpos, s.mem + x);
        }
        internal override BTree<long, DBObject> Add(PMetadata p, Database db)
        {
            var m = mem;
            if (p.Has(Sqlx.CAPTION)) m += (Caption, p.description);
            if (p.Has(Sqlx.X)) m += (Axis, Sqlx.X);
            else if (p.Has(Sqlx.Y)) m += (Axis, Sqlx.Y);
            return new BTree<long, DBObject>(defpos, new TableColumn(defpos, m));
        }
        internal override DBObject Add(Check ck, Database db)
        {
            return new TableColumn(defpos,mem+(Checks,constraints+(ck.defpos,ck)));
        }
        internal override DBObject AddProperty(Check ck, Database db)
        {
            return new TableColumn(defpos,mem+(ColumnProperties,columnProperties+(ck.name,ck)));
        }
        /// <summary>
        /// Accessor: Check a new column notnull condition
        /// Normally fail if null values found
        /// </summary>
        /// <param name="tr">Transaction</param>
        /// <param name="reverse">If true fail if non-null values found</param>
        internal void ColumnCheck(Transaction tr, bool reverse)
        {
            var cx = new Context(tr);
            var tb = tr.role.objects[tabledefpos] as Table;
            if (tb == null)
                return;
            var n = tb.columns[seq].name;
            var fm = new From(tr.uid, tb);
            for (var rb = fm.RowSets(tr,cx).First(cx); 
                rb != null; rb = rb.Next(cx))
            {
                var v = rb.row[seq];
                var nullfound = v == null;
                if (nullfound ^ reverse)
                    throw new DBException(reverse ? "44005" : "44004", name, tb.name).ISO()
                        .Add(Sqlx.TABLE_NAME, new TChar(tb.name))
                        .Add(Sqlx.COLUMN_NAME, new TChar(name));
            }
        }
        /// <summary>
        /// Accessor: Check a new column check constraint
        /// </summary>
        /// <param name="tr">Transaction</param>
        /// <param name="c">The new Check constraint</param>
        /// <param name="signal">signal is 44003 for column check, 44001 for domain check</param>
        internal void ColumnCheck(Transaction tr, Check c, string signal)
        {
            var tb = tr.role.objects[tabledefpos] as Table;
            if (tb == null)
                return;
            var needed = BTree<SqlValue, long>.Empty;
            var cx = new Level4.Context(tr);
            Query nf = new From(tr.uid,tb).AddCondition(cx,c.search.Disjoin());
            nf = c.search.Conditions(tr, cx, nf, false, out _);
            if (nf.RowSets(tr,cx).First(cx) != null)
                throw new DBException(signal, c.name, this, tb).ISO()
                    .Add(Sqlx.CONSTRAINT_NAME, new TChar(c.name.ToString()))
                    .Add(Sqlx.COLUMN_NAME, new TChar(name))
                    .Add(Sqlx.TABLE_NAME, new TChar(tb.name));
        }
        /// <summary>
        /// a readable version of the table column
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(Attribute) && (bool)mem[Attribute]) sb.Append(" Attribute");
            if (mem.Contains(Checks) && constraints.Count>0)
            { sb.Append(" Checks:"); sb.Append(constraints); }
            if (mem.Contains(Generated) && generated != PColumn.GenerationRule.No)
            { sb.Append(" Generated="); sb.Append(generated); }
            if (mem.Contains(NotNull) && notNull) sb.Append(" Not Null");
            return sb.ToString();
        }
        /// <summary>
        /// We drop a column silently if the table is dropped.
        /// </summary>
        /// <param name="t">The drop/rename transaction</param>
        /// <returns>whether the object is dependent</returns>
        public override Sqlx Dependent(Transaction t,Level4.Context cx)
        {
            if (t.refObj.defpos == domain.defpos)
                return Sqlx.RESTRICT;
            return (t.refObj.defpos == tabledefpos) ? Sqlx.DROP : Sqlx.NO;
        }
    }
    /// <summary>
    /// This is a type of Selector that corresponds to subColumn that is specified in a constraint
    /// and so must be realised in the physical infrastructure. 
    /// </summary>
    internal class ColumnPath : Selector
    {
        internal const long
            Prev = -293; // Selector
        /// <summary>
        /// The prefix Selector
        /// </summary>
        public Selector prev => (Selector)mem[Prev];
        /// <summary>
        /// Constructor:
        /// </summary>
        /// <param name="db">the database</param>
        /// <param name="pp">the level 2 column path information</param>
        /// <param name="rs">the set of grantees</param>
        public ColumnPath(Database db, PColumnPath pp)
            : this(pp, (Selector)db.role.objects[pp.coldefpos])
        { }
        protected ColumnPath(PColumnPath pp,Selector pr)
            : base(pp.ppos,BTree<long,object>.Empty+(Prev,pr)
                  +(Table,pr.tabledefpos)+(Classification,pr.classification))
        { }
        protected ColumnPath(long dp, BTree<long, object> m) : base(dp, m)
        { }
        internal override Basis New(BTree<long, object> m)
        {
            return new ColumnPath(defpos,m);
        }
        /// <summary>
        /// Poke a value into a given document according to this ColumnPath
        /// </summary>
        /// <param name="d">The document</param>
        /// <param name="ss">The list of path components</param>
        /// <param name="i">An index into this path</param>
        /// <param name="v">the new value</param>
        /// <returns>the updated Document</returns>
        TypedValue Set(TDocument d, string[] ss, int i,  TypedValue v)
        {
            var s = ss[i];
            var nd = new TDocument();
            if (i < ss.Length - 1)
            {
                var tv = d[s];
                if (tv as TDocument != null)
                    v = Set(tv as TDocument, ss, i + 1, v);
            }
            return new TDocument(d, (s, v));
        }
        /// <summary>
        /// Drop/rename support: check for dependencies
        /// </summary>
        /// <param name="t">the drop/rename transaction</param>
        /// <returns>DROP or RESTRICT</returns>
        public override Sqlx Dependent(Transaction t,Level4.Context cx)
        {
            if (prev.defpos == t.refObj.defpos || prev.Dependent(t,cx)==Sqlx.RESTRICT)
                return Sqlx.RESTRICT;
            return base.Dependent(t,cx);
        }
    }
    /// <summary>
    /// This class (new in v7) computes the current state of the TableRow and stores it in the
    /// Table for the schemaRole.
    /// </summary>
    internal class TableRow : DBObject
    {
        internal readonly static long
            Fields = -397, // BTree<long,TypedValue>
            Prev = -398, // long
            PrevKeys = -399, // BTree<long,PRow>
            Time = -400; // long
        public long tabledefpos => (long)mem[Selector.Table];
        public long time => (long)mem[Time];
        public long prev => (long)(mem[Prev] ?? -1);
        public Domain rowType => (Domain)mem[SqlValue.NominalType];
        public BTree<long, TypedValue> fields => (BTree<long,TypedValue>)mem[Fields];
        // prevKeys is null for Insert and empty if no keys have changed
        public BTree<long, PRow> prevKeys =>
            (BTree<long, PRow>)mem[PrevKeys] ?? BTree<long, PRow>.Empty;
        public string provenance =>(string)mem[Domain.Provenance];
        public TableRow(Record rc, Database db) : this(rc, db, _Fields(rc, db)) { }
        public TableRow(Record rc,Database db,(BTree<long,TypedValue>,BTree<long,PRow>)x) 
            :base(rc.ppos,
                 (rc is Update up)?up._defpos:rc.defpos,
                 db.role.defpos,BTree<long,object>.Empty
                 +(Selector.Table,rc.tabledefpos)+(Prev,_Prev(rc))
                 +(SqlValue.NominalType,_Type(rc,db))+(Ppos,rc.ppos)
                 +(Domain.Provenance,rc.provenance)+(Time,rc.time)
                 +(Fields,x.Item1)+(PrevKeys,x.Item2))
        { }
        protected TableRow(long defpos, BTree<long, object> m) : base(defpos, m) { }
        public static TableRow operator+(TableRow r,(long,object)x)
        {
            return new TableRow(r.defpos, r.mem + x);
        }
        static Domain _Type(Record rc, Database db)
        {
            return (rc.subType >= 0) ? (Domain)db.role.objects[rc.subType]
                : ((Table)db.role.objects[rc.tabledefpos]);
        }
        static long _Prev(Record rc)
        {
            return (rc is Update up) ? up.prev : -1;
        }
        static (BTree<long,TypedValue>,BTree<long,PRow>) _Fields(Record rc,Database db)
        {
            var tb = (Table)db.schemaRole.objects[rc.tabledefpos];
            var fl = rc.fields;
            BTree<long, PRow> pk = null;
            if (rc is Update up)
            {
                // add unchanged fields
                var old = tb.tableRows[up._defpos];
                for (var b = old.fields.First(); b != null; b = b.Next())
                    if (!fl.Contains(b.key()))
                        fl += (b.key(), b.value());
                pk = BTree<long, PRow>.Empty;
                // keep a record of keys that now need to be removed from indexes
                // (now is not the time to update imdexes)
                for (var b = tb.indexes.First(); b != null; b = b.Next())
                {
                    var x = b.value();
                    var ok = MakeKey(x, old.fields);
                    var nk = MakeKey(x, fl);
                    for (var i = 0; i < x.cols.Count; i++)
                        if (ok[i].CompareTo(nk[i]) != 0)
                        {
                            pk += (x.defpos, ok);
                            break;
                        }
                }
            }
            return (fl,pk);
        }
        public static PRow MakeKey(Index x,BTree<long,TypedValue> fl)
        {
            PRow r = null;
            for (var i = (int)x.cols.Count - 1; i >= 0; i--)
            {
                var v = fl[x.cols[i].defpos];
                if (v==null)
                    return x.MakeAutoKey(fl);
                r = new PRow(v, r);
            }
            return r;
        }
        public PRow MakeKey(Index x)
        {
            PRow r = null;
            for (var i = (int)x.cols.Count - 1; i >= 0; i--)
                r = new PRow(fields[x.cols[i].defpos], r);
            return r;
        }
        public PRow MakeKey(long[] cols)
        {
            PRow r = null;
            for (var i = (int)cols.Length - 1; i >= 0; i--)
                r = new PRow(fields[cols[i]], r);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TableRow(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TableRow(dp,mem);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Table=");sb.Append(tabledefpos);
            sb.Append(" Domain=");sb.Append(rowType);
            sb.Append(" Time=");sb.Append(new DateTime(time));
            sb.Append(" Fields:");sb.Append(fields.ToString());
            if (prevKeys != null)
            { sb.Append(" PrevKeys:"); sb.Append(prevKeys.ToString()); }
            if (provenance!="")
            { sb.Append(" Provenance="); sb.Append(provenance);  }
            return sb.ToString();
        }
    }
}
