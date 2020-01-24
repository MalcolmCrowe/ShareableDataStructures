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
    internal class OrderSpec :Basis
    {
        internal const long
            Items = -217; // BList<SqlValue>
        internal BList<SqlValue> items => (BList<SqlValue>)mem[Items]??BList<SqlValue>.Empty;
        internal static readonly OrderSpec Empty = new OrderSpec();
        OrderSpec():base(BTree<long,object>.Empty) { }
        public OrderSpec(BList<SqlValue> k):base((Items,k)) { }
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
            var its = items;
            var tis = that.items;
            if (its.Count != tis.Count)
                return false;
            for (var i = 0; i < its.Count; i++)
                if (!((SqlValue)its[i]).MatchExpr(q,(SqlValue)tis[i]))
                    return false;
            return true;
        }
        internal bool HasItem(SqlValue sv)
        {
            for (var b = items.First(); b != null; b = b.Next())
                if (b.value() == sv)
                    return true;
            return false;
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
            Current = -218, // bool
            Distance = -219, // TypedValue
            Preceding = -220, // bool
            Unbounded = -221; // bool
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
        public override string ToString()
        {
            if (mem == BTree<long, object>.Empty)
                return "current row";
            var sb = new StringBuilder();
            if (unbounded)
                sb.Append("unbounded");
            else
                sb.Append(distance);
            if (preceding)
                sb.Append(" preceding");
            else
                sb.Append(" following");
            return sb.ToString();
        }
    }
    /// <summary>
    /// A Window Specification from the parser
    /// </summary>
    internal class WindowSpecification : DBObject
    {
        internal const long
            Exclude = -222,// Sqlx
            High = -223, //WindowBound
            Low = -224,// WindowBound
            Order = -225, // OrderSpec
            OrderWindow = -226, // string
            Partition = -228, // int
            PartitionType = -229, // ObInfo
            Units = -230, // Sqlx
            WQuery = -231; // Query
        public string name => (string)mem[Name];
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
        /// The partitionType is the partition columns for the window/
        /// NB this a Domain, not an ObInfo as we treat the TRow as a single value for once
        /// </summary>
        internal ObInfo partitionType => (ObInfo)mem[PartitionType];
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
        internal Sqlx exclude => (Sqlx)(mem[Exclude]??Sqlx.Null);
        /// <summary>
        /// Constructor: a window specification from the parser
        /// </summary>
        /// <param name="q"></param>
        internal WindowSpecification(long lp) : base(lp,BTree<long, object>.Empty) { }
        protected WindowSpecification(long dp,BTree<long, object> m) : base(dp,m) { }
        public static WindowSpecification operator+(WindowSpecification w,(long,object)x)
        {
            return (WindowSpecification)w.New(w.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new WindowSpecification(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            throw new NotImplementedException();
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
                var cols = order?.items;
                var wcols = w.order?.items;
                if (order == null || w.order == null || cols.Count!=wcols.Count)
                    return false;
                for (int i = 0; i < order.items.Count; i++)
                    if (!((SqlValue)cols[i]).MatchExpr(query, (SqlValue)wcols[i]))
                        return false;
            }
            return partition == w.partition;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new WindowSpecification(defpos,m);
        }
        public static long MemoryLimit = 0;
    }
    internal class Grouping :DBObject
    {
        internal const long
            GroupKind = -232, //Sqlx
            Groups = -233, // BList<Grouping>
            Members = -234; // BTree<SqlValue,int>
        /// <summary>
        /// GROUP, CUBE or ROLLUP
        /// </summary>
        public override Sqlx kind => (Sqlx)(mem[GroupKind]??Sqlx.GROUP);
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
        internal override DBObject Relocate(long dp)
        {
            return new Grouping(dp, mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = this;
            var d = wr.Fix(defpos);
            if (d != defpos)
                r = (Grouping)Relocate(d);
            var gs = BList<Grouping>.Empty;
            var ch = false;
            for (var b=groups.First();b!=null;b=b.Next())
            {
                var g = (Grouping)b.value().Relocate(wr);
                ch = ch || g != b.value();
                gs += g;
            }
            if (ch)
                r += (Groups, gs);
            ch = false;
            var ms = BTree<SqlValue,int>.Empty;
            for (var b = members.First(); b != null; b = b.Next())
            {
                var m = (SqlValue)b.key().Relocate(wr);
                ch = ch || m != b.key();
                ms += (m,b.value());
            }
            if (ch)
                r += (Members, ms);
            return r;
        }
    }
    /// <summary>
    /// Implement a GroupSpecfication
    /// </summary>
    internal class GroupSpecification : DBObject
    {
        internal const long
            DistinctGp = -235, // bool
            Sets = -236; // BList<Grouping>
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
        internal override DBObject Relocate(long dp)
        {
            return new GroupSpecification(dp, mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = this;
            var d = wr.Fix(defpos);
            if (d != defpos)
                r = (GroupSpecification)Relocate(d);
            var gs = BList<Grouping>.Empty;
            var ch = false;
            for (var b=sets.First();b!=null;b=b.Next())
            {
                var g = (Grouping)b.value().Relocate(wr);
                ch = ch || (g != b.value());
                gs += g;
            }
            if (ch)
                r += (Sets, gs);
            return r;
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
    internal class UpdateAssignment : Basis,IComparable
    {
        internal const long
            Val = -237, // SqlValue 
            Vbl = -238; // SqlValue
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
        internal override Basis New(BTree<long, object> m)
        {
            throw new NotImplementedException();
        }
        internal UpdateAssignment Replace(Context cx,DBObject was,DBObject now)
        {
            var va = (SqlValue)val.Replace(cx, was, now);
            var vb = (SqlValue)vbl.Replace(cx, was, now);
            return new UpdateAssignment(vb, va);
        }
        internal UpdateAssignment Frame(Context cx)
        {
            var va = (SqlValue)val.Frame(cx);
            var vb = (SqlValue)vbl.Frame(cx);
            return new UpdateAssignment(vb, va);
        }
        public int CompareTo(object obj)
        {
            var that = (UpdateAssignment)obj;
            return vbl.CompareTo(that.vbl);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Vbl: ");sb.Append(vbl);
            sb.Append(" Val: "); sb.Append(val);
            return sb.ToString();
        }
    }
}

