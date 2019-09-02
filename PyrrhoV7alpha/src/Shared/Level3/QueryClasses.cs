using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
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
    /// <summary>
    /// An OrderItem has a value and some ordering flags
    /// </summary>
    internal class OrderItem :Basis
    {
        internal const long
            AscDesc = -229, // Sqlx
            Nulls = -230, // Sqlx
            What = -231; // SqlValue
        /// <summary>
        /// What to order: NB: may be a row
        /// </summary>
        public SqlValue what=>(SqlValue)mem[What];
        /// <summary>
        /// ASC or DESC
        /// </summary>
        public Sqlx ascDesc => (Sqlx)(mem[AscDesc]??Sqlx.ASC);
        /// <summary>
        /// nulls FIRST or LAST
        /// </summary>
        public Sqlx nulls => (Sqlx)(mem[Nulls]??Sqlx.FIRST);
        /// <summary>
        /// Constructor: a window OrderItem from the Parser
        /// </summary>
        /// <param name="cx">The contextn</param>
        /// <param name="cn">The ident name</param>
        internal OrderItem(SqlValue w,Sqlx a,Sqlx n):base(BTree<long,object>.Empty
            +(What,w)+(AscDesc,a)+(Nulls,n))
        { }
        protected OrderItem(BTree<long, object> m) : base(m) { }
        public static OrderItem operator+(OrderItem o,(long,object)m)
        {
            return new OrderItem(o.mem + m);
        }
        public override string ToString()
        {
            var s = what.ToString();
            if (ascDesc == Sqlx.DESC)
                s += " DESC ";
            return s;
        }

        internal override Basis New(BTree<long, object> m)
        {
            return new OrderItem(m);
        }
    }
    internal class OrderSpec :Basis
    {
        internal const long
            Items = -232, // BList<OrderItem>
            _KeyType = -233; // Domain
        internal BList<OrderItem> items => 
            (BList<OrderItem>)mem[Items]?? BList<OrderItem>.Empty;
        internal Domain keyType => (Domain)mem[_KeyType]??Domain.Null;
        internal static readonly OrderSpec Empty = new OrderSpec();
        OrderSpec():base(BTree<long,object>.Empty) { }
        public OrderSpec(Domain k)
            :base(new BTree<long, object>(Items, _Items(k))) { }
        static BList<OrderItem> _Items(Domain k)
        {
            var ts = BList<OrderItem>.Empty;
            for (var b = k.columns.First(); b != null; b = b.Next())
                ts += (b.key(), new OrderItem(b.value(), Sqlx.ASC, Sqlx.NULLS));
            return ts;
        }
        protected OrderSpec(BTree<long, object> m) : base(m) { }
        public static OrderSpec operator+(OrderSpec o,(long,object)x)
        {
            return new OrderSpec(o.mem + x);
        }
        /// <summary>
        /// Check that two OrderSpecs for the same dataType have the same ordering.
        /// </summary>
        /// <param name="that"></param>
        /// <returns></returns>
        internal bool SameAs(Query q,OrderSpec that)
        {
            if (that == null)
                return false;
            if (items.Count != that.items.Count)
                return false;
            for (var i = 0; i < items.Count; i++)
                if (!items[i].what.MatchExpr(q,that.items[i].what))
                    return false;
            return true;
        }
        internal bool HasItem(SqlValue sv)
        {
            for (var b = items.First(); b != null; b = b.Next())
                if (b.value().what == sv)
                    return true;
            return false;
        }
        public Domain KeyType(Domain dt, int off = 0, int lim = -2)
        {
            if (lim == -2)
                lim = (int)items.Count;
            var n = lim - off;
            if (n <= 0)
                return Domain.Null;
            var cs = BList<Selector>.Empty;
            for (int j = 0; j < n; j++)
            {
                var s = items[j + off];
                var t = s.what.nominalDataType + (Domain.NullsFirst, s.nulls)
                    +(Domain.Descending,s.ascDesc);
                var sel = dt.names[s.what.alias ?? s.what.name];
                if (sel == null)
                    return null;
                cs += (j, sel);
            }
            return new Domain(cs);
        }
        public override string ToString()
        {
            var r = "";
            for (int i = 0; i < items.Count; i++)
                r += ((i > 0) ? "," : "(") + items[i].ToString();
            r += ")";
            return r;
        }

        internal override Basis New(BTree<long, object> m)
        {
            return new OrderSpec(m);
        }
    }
    /// <summary>
    /// Implement bounds for a window in a windowed table
    /// </summary>
    internal class WindowBound :Basis
    {
        internal const long
            Current = -234, // bool
            Distance = -235, //TypedValue
            Preceding = -236, // bool
            Unbounded = -237; // bool
        /// <summary>
        /// whether PRECEDING specified
        /// </summary>
        internal bool preceding => (bool)(mem[Preceding]??false);
        /// <summary>
        /// whether UNBOUNDED specified
        /// </summary>
        internal bool unbounded => (bool)(mem[Unbounded] ?? false);
        /// <summary>
        /// whether CURRENT included
        /// </summary>
        internal bool current => (bool)(mem[Current] ?? false);
        /// <summary>
        /// The distance specification
        /// </summary>
        internal TypedValue distance =>(TypedValue)mem[Distance]??TNull.Value;
        /// <summary>
        /// Constructor: bound is CURRENT ROW
        /// </summary>
        internal WindowBound() : base(BTree<long, object>.Empty) { }
        protected WindowBound(BTree<long, object> m) : base(m) { }
        public static WindowBound operator+(WindowBound w,(long,object)x)
        {
            return new WindowBound(w.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new WindowBound(m);
        }
    }
    /// <summary>
    /// A Window Specification from the parser
    /// </summary>
    internal class WindowSpecification :Basis
    {
        internal const long
            Exclude = -238,// Sqlx
            High = -239, //WindowBound
            Low = -240,// WindowBound
            Order = -241, // OrderSpec
            OrderWindow = -242, // string
            OrdType = -243, // Domain
            Partition = -244, // int
            PartitionType = -245, //Domain
            Units = -246, // Sqlx
            WQuery = -247; // Query
        /// <summary>
        /// The associated Query
        /// </summary>
        internal Query query => (Query)mem[WQuery];
        /// <summary>
        /// the name of the ordering window
        /// </summary>
        internal string orderWindow => (string)mem[OrderWindow];
        /// <summary>
        /// a specified ordering
        /// </summary>
        internal OrderSpec order => (OrderSpec)mem[Order];
        /// <summary>
        /// how many window partitioning order items have been specified
        /// </summary>
        internal int partition => (int)(mem[Partition]??0);
        /// <summary>
        /// The partitionType is the partition columns for the window
        /// </summary>
        internal Domain partitionType => (Domain)mem[PartitionType]??Domain.Null;
        /// <summary>
        /// The ordType includes ordering columns for the window if any
        /// </summary>
        internal Domain ordType => (Domain)mem[OrdType]??Domain.Null;
        /// <summary>
        /// ROW or RANGE if have window frame
        /// </summary>
        internal Sqlx units => (Sqlx)(mem[Units]??Sqlx.NO);
        /// <summary>
        /// low WindowBound if specified
        /// </summary>
        internal WindowBound low => (WindowBound)mem[Low];
        /// <summary>
        /// high WindowBound if specified
        /// </summary>
        internal WindowBound high => (WindowBound)mem[High];
        /// <summary>
        /// exclude CURRENT, TIES or OTHERS (NO if not specified)
        /// </summary>
        internal Sqlx exclude => (Sqlx)(mem[Exclude]??Sqlx.NO);
        /// <summary>
        /// Constructor: a window specification from the parser
        /// </summary>
        /// <param name="q"></param>
        internal WindowSpecification(Query q) : base(new BTree<long, object>(WQuery, q)) { }
        protected WindowSpecification(BTree<long, object> m) : base(m) { }
        public static WindowSpecification operator+(WindowSpecification w,(long,object)x)
        {
            return new WindowSpecification(w.mem + x);
        }
        /// <summary>
        /// Compare two WindowSpecifications for equivalance
        /// </summary>
        /// <param name="w">The other WindowSpecification</param>
        /// <returns>whether they are equivalent</returns>
        internal bool Equiv(WindowSpecification w)
        {
            if (name == w.name || name == w.orderWindow)
                return true;
            if (orderWindow != null && (orderWindow == w.name || orderWindow == w.orderWindow))
                return true;
            if (order != null || w.order != null)
            {
                if (order == null || w.order == null || order.items.Count!=w.order.items.Count)
                    return false;
                for (int i = 0; i < order.items.Count;i++)
                {
                    OrderItem ai = order.items[i];
                    OrderItem bi = w.order.items[i];
                    if (ai.nulls != bi.nulls)
                        return false;
                    if (ai.ascDesc != bi.ascDesc)
                        return false;
                    if (ai.what != bi.what) // probably need something more careful for comparing these SqlValues
                        return false;
                }
            }
            return partition == w.partition;
        }

        internal override Basis New(BTree<long, object> m)
        {
            return new WindowSpecification(m);
        }
        public static long MemoryLimit = 0;
    }
    internal class Grouping :DBObject
    {
        internal const long
            GroupKind = -248, //Sqlx
            Groups = -249, // BList<Grouping>
            Members = -250; // BList<SqlValue>
        /// <summary>
        /// GROUP, CUBE or ROLLUP
        /// </summary>
        internal Sqlx kind => (Sqlx)(mem[GroupKind]??Sqlx.GROUP);
        internal BList<Grouping> groups => 
            (BList<Grouping>)mem[Groups]?? BList<Grouping>.Empty;
        /// <summary>
        /// the names for this grouping.
        /// See SqlValue.IsNeeded for where the ref gp parameter is used.
        /// </summary>
        internal BTree<SqlValue,int> members => 
            (BTree<SqlValue,int>)mem[Members]??BTree<SqlValue,int>.Empty;
        internal Grouping(long dp,BTree<long,object>m=null):base(dp,m) { }
        public static Grouping operator+(Grouping g,(long,object) x)
        {
            return new Grouping(g.defpos,g.mem+x);
        }
        internal bool Has(SqlValue sv)
        {
            for (var b = members.First(); b != null; b = b.Next())
                if (b.key().defpos == sv.defpos)
                    return true;
            for (var b = groups.First(); b != null; b = b.Next())
                if (b.value().Has(sv))
                    return true;
            return false;
        }
        internal void Grouped(BTree<long, SqlValue> svs, ref bool gp)
        {
            for (var b = svs.First(); b != null; b = b.Next())
                Grouped(b.value(), ref gp);
        }
        internal void Grouped(SqlValue sv, ref bool gp)
        {
            if (members.Contains(sv))
                gp = true;
            for (var g = groups.First();g!=null;g=g.Next())
                g.value().Grouped(sv, ref gp);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(kind.ToString());
            var cm = " ";
            for (var b=members.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.value());
            }
            if (groups.Count != 0)
            {
                cm = "(";
                for (var b = groups.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(b.value());
                }
                sb.Append(")");
            }
            return sb.ToString();
        }

        internal override Basis New(BTree<long, object> m)
        {
            return new Grouping(defpos,m);
        }
    }
    /// <summary>
    /// Implement a GroupSpecfication
    /// </summary>
    internal class GroupSpecification : DBObject
    {
        internal const long
            DistinctGp = -251, // bool
            Sets = -252; // BList<Grouping>
        /// <summary>
        /// whether DISTINCT has been specified
        /// </summary>
        internal bool distinct => (bool)(mem[DistinctGp] ?? false);
        /// <summary>
        /// The specified grouping sets. We translate ROLLUP and CUBE into these
        /// </summary>
        internal BList<Grouping> sets =>
            (BList<Grouping>)mem[Sets] ?? BList<Grouping>.Empty;
        public GroupSpecification(long dp) :base(dp,BTree<long,object>.Empty) { }
        internal GroupSpecification(long dp,BTree<long, object> m) : base(dp,m) { }
        public static GroupSpecification operator+(GroupSpecification a,GroupSpecification gs)
        {
            var s = a.sets;
            for (var b = gs?.sets.First(); b != null; b = b.Next())
                s += b.value();
            return a + (Sets, s);
        }
        public static GroupSpecification operator+(GroupSpecification gs,(long,object)x)
        {
            return (GroupSpecification)gs.New(gs.mem + x);
        }
        public bool Has(SqlValue sv)
        {
            for (var b = sets.First(); b != null; b = b.Next())
                if (b.value().Has(sv))
                    return true;
            return false;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new GroupSpecification(defpos,m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(DistinctGp))
                sb.Append(" distinct");
            for (var b = sets.First(); b != null; b = b.Next())
                sb.Append(b.value());
            return sb.ToString();
        }
    }
    /// <summary>
    /// A Method Name for the parser
    /// </summary>
    internal class MethodName
    {
        /// <summary>
        /// The type of the method (static, constructor etc)
        /// </summary>
        public PMethod.MethodType methodType;
        /// <summary>
        /// the name of the method
        /// </summary>
        public Ident mname;
        /// <summary>
        /// the name of the parent type
        /// </summary>
        public Ident tname;
        /// <summary>
        /// the number of parameters of the method
        /// </summary>
        public int arity;
        public ProcParameter[] ins;
        /// <summary>
        /// The return type
        /// </summary>
        public long retpos;
        /// <summary>
        /// a string version of the signature
        /// </summary>
        public string signature;
    }
    internal class UpdateAssignment : Basis,IComparable
    {
        internal const long
            Vbl = -253, // SqlValue
            Val = -254; // SqlValue
        public SqlValue vbl=>(SqlValue)mem[Vbl];
        public SqlValue val=>(SqlValue)mem[Val];
        public UpdateAssignment(SqlValue vb, SqlValue vl) : base(BTree<long, object>.Empty
            + (Vbl, vb) + (Val, vl))
        { }
        protected UpdateAssignment(BTree<long, object> m) : base(m) { }
        public static UpdateAssignment operator+ (UpdateAssignment u,(long, object)x)
        {
            return new UpdateAssignment(u.mem + x);
        }
        public void SetupValues(Transaction tr, Context cx,Query q)
        {
            val._Setup(tr,cx,q, vbl.nominalDataType);
        }
        public TypedValue Eval(Transaction tr,Context cx)
        {
            var v = val.Eval(tr,cx);
            if (vbl is SqlValueExpr st && st.kind==Sqlx.LBRACK &&
                cx.row[st.left.defpos] is TArray ta && st.right.Eval(tr,cx) is TInt ti && ti.ToInt().HasValue)
            {
                ta[ti.ToInt().Value] = v;
                return ta;
            }
            return v;
        }
        internal override Basis New(BTree<long, object> m)
        {
            throw new NotImplementedException();
        }

        public int CompareTo(object obj)
        {
            var that = (UpdateAssignment)obj;
            return vbl.CompareTo(that.vbl);
        }
    }
}

