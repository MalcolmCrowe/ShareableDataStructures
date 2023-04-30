using System;
using System.Security.Authentication.ExtendedProtection;
using System.Text;
using System.Threading;
using Pyrrho.Common;
using Pyrrho.Level2;
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

namespace Pyrrho.Level3
{
    /// <summary>
    /// Implement bounds for a window in a windowed table
    /// shareable as of 26 April 2021
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
        internal TypedValue distance =>(TypedValue?)mem[Distance]??TNull.Value;
        /// <summary>
        /// Constructor: bound is CURRENT ROW
        /// </summary>
        internal WindowBound() : base(BTree<long, object>.Empty) { }
        protected WindowBound(BTree<long, object> m) : base(m) { }
        public static WindowBound operator+(WindowBound w,(long,object)x)
        {
            var (dp, ob) = x;
            if (w.mem[dp] == ob)
                return w;
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
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            return m + (Distance, distance.Fix(cx));
        }
    }
    /// <summary>
    /// A Window Specification from the parser
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class WindowSpecification : DBObject
    {
        internal const long
            Exclude = -222,// Sqlx
            High = -223, //WindowBound
            Low = -224,// WindowBound
            Order = -225, // Domain (extends the partition type)
            OrderWindow = -226, // string
            PartitionType = -229, // Domain
            Units = -230, // Sqlx
            WQuery = -231; // long RowSet
        /// <summary>
        /// The associated RowSet
        /// </summary>
        internal long query => (long)(mem[WQuery]??-1L);
        /// <summary>
        /// the name of the ordering window
        /// </summary>
        internal string? orderWindow => (string?)mem[OrderWindow];
        /// <summary>
        /// a specified ordering, extending the partition if any
        /// </summary>
        internal Domain order => (Domain)(mem[Order]??Domain.Row);
        /// <summary>
        /// The partitionType is the partition columns for the window if any
        /// </summary>
        internal Domain partition => (Domain)(mem[PartitionType]??Domain.Row);
        /// <summary>
        /// ROW or RANGE if have window frame
        /// </summary>
        internal Sqlx units => (Sqlx)(mem[Units]??Sqlx.NO);
        /// <summary>
        /// low WindowBound if specified
        /// </summary>
        internal WindowBound? low => (WindowBound?)mem[Low];
        /// <summary>
        /// high WindowBound if specified
        /// </summary>
        internal WindowBound? high => (WindowBound?)mem[High];
        /// <summary>
        /// exclude CURRENT, TIES or OTHERS (NO if not specified)
        /// </summary>
        internal Sqlx exclude => (Sqlx)(mem[Exclude]??Sqlx.Null);
        /// <summary>
        /// Constructor: a window specification from the parser
        /// </summary>
        /// <param name="q"></param>
        internal WindowSpecification(long lp) : base(lp,BTree<long, object>.Empty) { }
        protected WindowSpecification(long dp, BTree<long, object> m)
            : base(dp, m) { }
        public static WindowSpecification operator+(WindowSpecification w,(long,object)x)
        {
            var (dp, ob) = x;
            if (w.mem[dp] == ob)
                return w;
            return (WindowSpecification)w.New(w.mem + x);
        }
        public static WindowSpecification operator +(WindowSpecification w, (Context,long, object) x)
        {
            var (cx, p, o) = x;
            return new WindowSpecification(w.defpos,w.mem + (p,o));
        }
        internal CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> qn)
        {
            for (var b = order.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p&& cx.obs[p] is SqlValue v)
                    qn = v.Needs(cx, r, qn);
            return qn;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new WindowSpecification(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new WindowSpecification(dp, m);
        }
        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (WindowSpecification)base._Replace(cx, so, sv);
            var np = partition.Replace(cx, so, sv);
            if (np != partition)
                r += (PartitionType, np);
            var no = order.Replace(cx, so, sv);
            if (no != order)
                r += (Order, no);
            return r;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nh = high?.Fix(cx);
            if (nh != high && nh is not null)
                r += (High, nh);
            var nl = low?.Fix(cx);
            if (nl != low && nl is not null)
                r += (Low, nl);
            var no = order.Fix(cx);
            if (no != order)
                r += (Order, no);
            var np = partition.Fix(cx);
            if (np!=partition)
                r += (PartitionType, np);
            var nq = cx.Fix(query);
            if (nq != query)
                r += (WQuery, nq);
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
            if (order == w.order)
                return true;
            return Context.Match(order.rowType,w.order.rowType);
        }
        internal override DBObject QParams(Context cx)
        {
            var r = base.QParams(cx).mem;
            var h = high?.distance;
            if (h is TQParam tq)
                h = cx.values[tq.qid.dp];
            if (h is not null && h != high?.distance)
                r += (High, h);
            var w = low?.distance;
            if (w is TQParam tr)
                w = cx.values[tr.qid.dp];
            if (w is not null && w != low?.distance)
                r += (Low, w);
            return new WindowSpecification(defpos,r);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(" Window ");
            sb.Append(Uid(defpos));
            if (query >= 0) { sb.Append(" Query "); sb.Append(Uid(query));  }
            if (orderWindow is not null) { sb.Append(" OWin "); sb.Append(orderWindow); }
            if (partition!=Domain.Row)
            {
                sb.Append(" Partition "); sb.Append(partition);
            }
            if (order!=Domain.Row)
            { 
                sb.Append(" Order "); sb.Append(order);
            }
            if (units != Sqlx.NO) { sb.Append(" Units "); sb.Append(units); }
            if (low is not null) { sb.Append(" Low "); sb.Append(low); }
            if (high != null) { sb.Append(" High "); sb.Append(high); }
            if (exclude != Sqlx.Null) { sb.Append(" Exclude "); sb.Append(exclude); }
            return sb.ToString();
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            BList<DBObject>ls;
            var ch = false;
            var mm = mem;
            if (partition!=Domain.Row)
            {
                (ls, m) = partition.Resolve(cx, f, m);
                if (ls[0] is Domain np && np!=partition)
                {
                    ch = true; 
                    mm += (PartitionType, np);
                }
            }
            if (order!=Domain.Row)
            {
                (ls, m) = order.Resolve(cx, f, m);
                if (ls[0] is Domain nd && nd != order)
                {
                    ch = true; 
                    mm += (Order, nd);
                }
            }
            if (cx.obs[query] is RowSet rs )
            {
                (ls, m) = rs.domain.Resolve(cx, f, m);
                if (ls[0] is Domain nr && nr.defpos != rs.domain.defpos)
                {
                    ch = true;
                    rs.Apply(new BTree<long, object>(_Domain, nr),cx);
                    mm += (WQuery, nr);
                }
            }
            return ch?(new BList<DBObject>((DBObject)New(mm)), m):(BList<DBObject>.Empty,m);
        }
    }
    // shareable as of 26 April 2021
    internal class Grouping :DBObject,IComparable
    {
        internal const long
            GroupKind = -232, //Sqlx
            Groups = -233, // CList<Grouping>
            Members = -234; // CTree<long,int> SqlValue
        /// <summary>
        /// GROUP, CUBE or ROLLUP
        /// </summary>
        public Sqlx kind => (Sqlx)(mem[GroupKind]??Sqlx.GROUP);
        internal CList<Grouping> groups => 
            (CList<Grouping>)(mem[Groups]?? CList<Grouping>.Empty);
        /// <summary>
        /// the names for this grouping.
        /// See SqlValue.IsNeeded for where the ref gp parameter is used.
        /// </summary>
        internal CTree<long,int> members => 
            (CTree<long,int>)(mem[Members]??CTree<long,int>.Empty);
        internal Domain keys =>
            (Domain)(mem[Index.Keys]??Domain.Null);
        internal Grouping(Context cx,BTree<long,object>?m= null)
            :base(cx.GetUid(),_Mem(cx,m))
        { }
        protected Grouping(long dp,BTree<long,object>m)
            :base(dp,m) { }
        static BTree<long,object> _Mem(Context cx,BTree<long,object>? m)
        {
            m ??= BTree<long, object>.Empty;
            if (m[Members] is CTree<long, int> ms)
            {
                long[] x = new long[ms.Count];
                for (var b = ms.First(); b != null; b = b.Next())
                    x[b.value()] = b.key();
                // what about groups??
                var bs = BList<DBObject>.Empty;
                for (var i = 0; i < x.Length; i++)
                    if (cx._Ob(x[i]) is DBObject ob)
                        bs += ob;
                var ks = (Domain)cx.Add(new Domain(cx.GetUid(), cx, Sqlx.ROW, bs, bs.Length));
                m += (Index.Keys, ks);
            }
            return m;
        }
        public static Grouping operator+(Grouping g,(long,object) x)
        {
            return new Grouping(g.defpos,g.mem+x);
        }
        internal bool Has(long s)
        {
            if (members.Contains(s))
                 return true;
            for (var b = groups.First(); b != null; b = b.Next())
                if (b.value().Has(s))
                    return true;
            return false;
        }
        internal bool Has(Context cx,string s)
        {
            for (var b=members.First();b is not null;b=b.Next())
                if (cx.obs[b.key()] is SqlValue v && v.name == s)
                    return true;
            for (var b = groups.First(); b != null; b = b.Next())
                if (b.value().Has(cx,s))
                    return true;
            return false;
        }
        internal override CTree<long,bool> Needs(Context cx)
        {
            var nd = CTree<long,bool>.Empty;
            for (var b = members.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v)
                nd += v.Needs(cx);
            for (var b = groups.First(); b != null; b = b.Next())
                nd += b.value().Needs(cx);
            return nd;
        }
        internal bool Known(Context cx, RestRowSet rrs)
        {
  /*          var cs = CTree<long, bool>.Empty;
            for (var b = rrs.remoteCols.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    cs += (p, true); */
            for (var b = groups.First(); b != null; b = b.Next())
                if (!b.value().Known(cx, rrs))
                    return false;
            //           for (var b = members.First(); b != null; b = b.Next())
            //               if (!cs.Contains(b.value()))
            //                   return false;
            for (var b = members.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue v && !v.KnownBy(cx, rrs))
                    return false;
            return true;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' ');
            sb.Append(kind.ToString());
            sb.Append(' ');
            var cm = "(";
            for (var b=members.First();b is not null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.key())); sb.Append('=');
                sb.Append(b.value());
            }
            if (cm == ",")
                sb.Append(')');
            if (groups.Count != 0)
            {
                cm = "[";
                for (var b = groups.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(b.value());
                }
                sb.Append(']');
            }
            return sb.ToString();
        }

        internal override Basis New(BTree<long, object> m)
        {
            return new Grouping(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new Grouping(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx, m);
            var ng = cx.FixBG(groups);
            if (ng!=groups)
            r += (Groups, ng);
            var nm = cx.Fix(members);
            if (nm!=members)
            r += (Members, nm);
            return r;
        }

        public int CompareTo(object? obj)
        {
            if (obj == null)
                return 1;
            var that = (Grouping)obj;
            var c = groups.CompareTo(that.groups);
            return (c != 0) ? c : members.CompareTo(that.members);
        }
    }
    /// <summary>
    /// Implement a GroupSpecfication
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class GroupSpecification : DBObject
    {
        internal const long
            DistinctGp = -235, // bool
            Sets = -236; // BList<long?> Grouping
        /// <summary>
        /// whether DISTINCT has been specified
        /// </summary>
        internal bool distinct => (bool)(mem[DistinctGp] ?? false);
        /// <summary>
        /// The specified grouping sets. We translate ROLLUP and CUBE into these
        /// </summary>
        internal BList<long?> sets =>
            (BList<long?>?)mem[Sets] ?? BList<long?>.Empty;
        internal GroupSpecification(long lp, Context cx, BTree<long, object> m) : base(lp,m) { }
        internal GroupSpecification(long lp, BTree<long, object> m) : base(lp, m) { }
        public static GroupSpecification operator+(GroupSpecification gs,(long,object)x)
        {
            var (dp, ob) = x;
            if (gs.mem[dp] == ob)
                return gs;
            return (GroupSpecification)gs.New(gs.mem + x);
        }
        public static GroupSpecification operator +(GroupSpecification gs, (Context,long, object) x)
        {
            var (cx, k, v) = x;
            var m = gs.mem + (k,v);
            return (GroupSpecification)cx.Add((GroupSpecification)gs.New(m));
        }
        public bool Has(Context cx,long s)
        {
            for (var b = sets.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is Grouping g && g.Has(s))
                    return true;
            return false;
        }
        internal override CTree<long, bool> Operands(Context cx)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = sets.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is Grouping g)
                    for (var c = g.keys.First(); c != null; c = c.Next())
                        if (cx.obs[p] is SqlValue v)
                            r += v.Operands(cx);
            return r;
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            return cx.Needs(CTree<long, bool>.Empty, sets);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new GroupSpecification(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new GroupSpecification(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx, m);
            var ns = cx.FixLl(sets);
            if (ns != sets)
                r += (Sets, ns);
            return r;
        }
        internal bool Grouped(Context cx,BList<long?> vals)
        {
            for (var b = vals?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v && !v.Grouped(cx, this))
                    return false;
            return true;
        }
        internal GroupSpecification Known(Context cx,RestRowSet rr)
        {
            var ss = BList<long?>.Empty;
            for (var b = sets.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is Grouping g && g.Known(cx, rr))
                    ss += p;
            return (ss == sets) ? this :
                (GroupSpecification)cx.Add(new GroupSpecification(cx.GetUid(), mem + (Sets, ss)));
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(DistinctGp) && distinct)
                sb.Append(" distinct");
            var cm = '(';
            for (var b = sets.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ',';
                    sb.Append(Uid(p));
                }
            if (cm == ',')
                sb.Append(')');
            return sb.ToString();
        }
    }
    // shareable as of 26 April 2021
    internal class UpdateAssignment : Basis,IComparable
    {
        internal const long
            Literal = -217, // TypedValue;
            Val = -237, // long SqlValue
            Vbl = -238; // long SqlValue
        public long vbl=>(long)(mem[Vbl]??-1L);
        public long val=>(long)(mem[Val]??-1L);
        public TypedValue lit => (TypedValue?)mem[Literal] ?? TNull.Value;
        public UpdateAssignment(long vb, long vl) : base(BTree<long, object>.Empty
            + (Vbl, vb) + (Val, vl))
        { }
        public UpdateAssignment(long vb, TypedValue vl) : base(BTree<long, object>.Empty
    + (Vbl, vb) + (Literal, vl))
        { }
        protected UpdateAssignment(BTree<long, object> m) : base(m) { }
        public static UpdateAssignment operator+ (UpdateAssignment u,(long, object)x)
        {
            var (dp, ob) = x;
            if (u.mem[dp] == ob)
                return u;
            return new UpdateAssignment(u.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new UpdateAssignment(m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var na = cx.Fix(val);
            if (na!=val)
            r += (Val, na);
            var nb = cx.Fix(vbl);
            if (nb!=vbl)
            r += (Vbl, nb);
            return r;
        }
        internal UpdateAssignment Replace(Context cx,DBObject was,DBObject now)
        {
            var va = cx.ObReplace(val, was, now);
            var vb = cx.ObReplace(vbl, was, now);
            return new UpdateAssignment(vb, va);
        }
        public TypedValue Eval(Context cx)
        {
            return cx.obs[val]?.Eval(cx) ?? lit;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Vbl: ");sb.Append(DBObject.Uid(vbl));
            if (val >= 0)
            { sb.Append(" Val: "); sb.Append(DBObject.Uid(val)); }
            else
            { sb.Append('='); sb.Append(lit); }
            return sb.ToString();
        }
        public int CompareTo(object? obj)
        {
            if (obj == null) return 1;
            var that = (UpdateAssignment)obj;
            int c = vbl.CompareTo(that.vbl);
            if (c != 0)
                return c;
            c = val.CompareTo(that.val);
            return (c != 0) ? c : lit.CompareTo(that.lit);
        }
    }
}

