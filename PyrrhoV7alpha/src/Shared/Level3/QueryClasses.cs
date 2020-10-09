using System;
using System.Security.Authentication.ExtendedProtection;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

namespace Pyrrho.Level3
{
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

        internal override void Scan(Context cx)
        {
            distance?.Scan(cx);
        }
        internal override Basis Fix(Context cx)
        {
            return this + (Distance, distance?.Fix(cx));
        }
        internal override Basis _Relocate(Writer wr)
        {
            return this+(Distance, distance?.Relocate(wr));
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
            Order = -225, // CList<long>
            OrderWindow = -226, // string
            Partition = -228, // int
            PartitionType = -229, // BList<long>
            Units = -230, // Sqlx
            WQuery = -231; // long Query
        public string name => (string)mem[Name];
        /// <summary>
        /// The associated Query
        /// </summary>
        internal long query => (long)(mem[WQuery]??-1L);
        /// <summary>
        /// the name of the ordering window
        /// </summary>
        internal string orderWindow => (string)mem[OrderWindow];
        /// <summary>
        /// a specified ordering
        /// </summary>
        internal CList<long> order => (CList<long>)mem[Order];
        /// <summary>
        /// how many window partitioning order items have been specified
        /// </summary>
        internal int partition => (int)(mem[Partition]??0);
        /// <summary>
        /// The partitionType is the partition columns for the window/
        /// NB this a Domain, not an ObInfo as we treat the TRow as a single value for once
        /// </summary>
        internal BList<long> partitionType => (BList<long>)mem[PartitionType];
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
        internal override Basis New(BTree<long, object> m)
        {
            return new WindowSpecification(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new WindowSpecification(dp, mem);
        }
        internal override void Scan(Context cx)
        {
            cx.ObUnheap(defpos);
            high?.Scan(cx);
            low?.Scan(cx);
            cx.Scan(order);
            cx.Scan(partitionType);
            cx.ObScanned(query);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (WindowSpecification)base._Relocate(wr);
            r += (High, high?._Relocate(wr));
            r += (Low, low?._Relocate(wr));
            r += (Order, wr.Fix(order));
            r += (PartitionType, wr.Fix(partitionType));
            r += (WQuery, wr.Fixed(query));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (WindowSpecification)base.Fix(cx);
            r += (High, high?.Fix(cx));
            r += (Low, low?.Fix(cx));
            r += (Order, cx.Fix(order));
            r += (PartitionType, cx.Fix(partitionType));
            r += (WQuery, cx.obuids[query]);
            return r;
        }
        /// <summary>
        /// Compare two WindowSpecifications for equivalance
        /// </summary>
        /// <param name="w">The other WindowSpecification</param>
        /// <returns>whether they are equivalent</returns>
        internal bool Equiv(Context cx,WindowSpecification w)
        {
            if (name == w.name || name == w.orderWindow)
                return true;
            if (orderWindow != null && (orderWindow == w.name || orderWindow == w.orderWindow))
                return true;
            if (order != null || w.order != null)
            {
                if (order?.Length!=w.order?.Length)
                    return false;
                var wb = w.order.First();
                for (var b=order?.First(); b!=null;b=b.Next(),wb=wb.Next())
                    if (!((SqlValue)cx.obs[b.value()]).MatchExpr(cx,(Query)cx.obs[query], 
                        (SqlValue)cx.obs[wb.value()]))
                        return false;
            }
            return partition == w.partition;
        }
    }
    internal class Grouping :DBObject
    {
        internal const long
            GroupKind = -232, //Sqlx
            Groups = -233, // BList<Grouping>
            Members = -234; // BList<long> SqlValue
        /// <summary>
        /// GROUP, CUBE or ROLLUP
        /// </summary>
        public Sqlx kind => (Sqlx)(mem[GroupKind]??Sqlx.GROUP);
        internal BList<Grouping> groups => 
            (BList<Grouping>)mem[Groups]?? BList<Grouping>.Empty;
        /// <summary>
        /// the names for this grouping.
        /// See SqlValue.IsNeeded for where the ref gp parameter is used.
        /// </summary>
        internal BTree<long,int> members => 
            (BTree<long,int>)mem[Members]??BTree<long,int>.Empty;
        internal Grouping(long dp,BTree<long,object>m=null)
            :base(dp,m??BTree<long,object>.Empty) { }
        public static Grouping operator+(Grouping g,(long,object) x)
        {
            return new Grouping(g.defpos,g.mem+x);
        }
        internal bool Has(SqlValue sv)
        {
            for (var b = members.First(); b != null; b = b.Next())
                if (b.key() == sv.defpos)
                    return true;
            for (var b = groups.First(); b != null; b = b.Next())
                if (b.value().Has(sv))
                    return true;
            return false;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(kind.ToString());
            sb.Append(" ");
            var cm = "(";
            for (var b=members.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.key())); sb.Append("=");
                sb.Append(b.value());
            }
            if (cm == ",")
                sb.Append(")");
            if (groups.Count != 0)
            {
                cm = "[";
                for (var b = groups.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(b.value());
                }
                sb.Append("]");
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
        internal override void Scan(Context cx)
        {
            cx.ObUnheap(defpos);
            cx.Scan(groups);
            cx.Scan(members);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (Grouping)base._Relocate(wr);
            r += (Groups, wr.Fix(groups));
            r += (Members, wr.Fix(members));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (Grouping)base.Fix(cx);
            r += (Groups, cx.Fix(groups));
            r += (Members, cx.Fix(members));
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
            Sets = -236; // BList<long> Grouping
        /// <summary>
        /// whether DISTINCT has been specified
        /// </summary>
        internal bool distinct => (bool)(mem[DistinctGp] ?? false);
        /// <summary>
        /// The specified grouping sets. We translate ROLLUP and CUBE into these
        /// </summary>
        internal BList<long> sets =>
            (BList<long>)mem[Sets] ?? BList<long>.Empty;
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
        public bool Has(Context cx,SqlValue sv)
        {
            for (var b = sets.First(); b != null; b = b.Next())
                if (((Grouping)cx.obs[b.value()]).Has(sv))
                    return true;
            return false;
        }
        internal override BTree<long, bool> Needs(Context cx)
        {
            return cx.Needs(BTree<long, bool>.Empty, sets);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new GroupSpecification(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new GroupSpecification(dp, mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (GroupSpecification)base._Relocate(wr);
            r += (Sets, wr.Fix(sets));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (GroupSpecification)base.Fix(cx);
            r += (Sets, cx.Fix(sets));
            return r;
        }
        internal override void Scan(Context cx)
        {
            cx.ObUnheap(defpos);
            cx.Scan(sets);
        }
        internal void Grouped(Context cx,BList<long> vals)
        {
            for (var b = vals?.First(); b != null; b = b.Next())
                ((SqlValue)cx.obs[b.value()]).Grouped(cx, this);
        }
        internal void Grouped(Context cx, BTree<long,bool> vals)
        {
            for (var b = vals?.First(); b != null; b = b.Next())
                ((SqlValue)cx.obs[b.key()]).Grouped(cx, this);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(DistinctGp) && distinct)
                sb.Append(" distinct");
            var cm = '(';
            for (var b = sets.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ',';
                sb.Append(Uid(b.value()));
            }
            if (cm == ',')
                sb.Append(')');
            return sb.ToString();
        }
    }
    internal class UpdateAssignment : Basis,IComparable
    {
        internal const long
            Val = -237, // long SqlValue
            Vbl = -238; // long SqlValue
        public long vbl=>(long)(mem[Vbl]??-1L);
        public long val=>(long)(mem[Val]??-1L);
        public UpdateAssignment(long vb, long vl) : base(BTree<long, object>.Empty
            + (Vbl, vb) + (Val, vl))
        { }
        protected UpdateAssignment(BTree<long, object> m) : base(m) { }
        public static UpdateAssignment operator+ (UpdateAssignment u,(long, object)x)
        {
            return new UpdateAssignment(u.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new UpdateAssignment(m);
        }
        internal override void Scan(Context cx)
        {
            cx.ObScanned(vbl);
            cx.ObScanned(val);
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = base._Relocate(wr);
            var va = (SqlValue)wr.Fixed(val);
            if (va.defpos != val)
                r += (Val, va.defpos);
            var vb = (SqlValue)wr.Fixed(vbl);
            if (vb.defpos != vbl)
                r += (Vbl, vb.defpos);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = base.Fix(cx);
            r += (Val, cx.obuids[val]);
            r += (Vbl, cx.obuids[vbl]);
            return r;
        }
        internal UpdateAssignment Replace(Context cx,DBObject was,DBObject now)
        {
            var va = cx.Replace(val, was, now);
            var vb = cx.Replace(vbl, was, now);
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
            sb.Append(" Vbl: ");sb.Append(DBObject.Uid(vbl));
            sb.Append(" Val: "); sb.Append(DBObject.Uid(val));
            return sb.ToString();
        }
    }
}

