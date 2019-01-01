using System.Text;

namespace Shareable
{
    internal class SIndex :SDbObject
    {
        public readonly long table;
        public readonly bool primary;
        public readonly long references;
        public readonly SList<long> cols;
        public readonly SMTree rows;
        /// <summary>
        /// A primary or unique index
        /// </summary>
        /// <param name="t"></param>
        /// <param name="c"></param>
        public SIndex(STransaction tr,long t,bool p,SList<long> c) : base(Types.SIndex,tr)
        {
            table = t;
            primary = p;
            cols = c;
            rows = new SMTree(Info((STable)tr.objects.Lookup(table),cols));
        }
        internal SIndex(SIndex x,long t,SList<long> c) : base(x)
        {
            table = t; cols = c;
            primary = x.primary;
            rows = x.rows;
        }
        SIndex(SDatabase d,AStream f): base(Types.SIndex,f)
        {
            table = f.GetLong();
            var n = f.ReadByte();
            var c = new long[n];
            for (var i = 0; i < n; i++)
                c[i] = f.GetInt();
            cols = SList<long>.New(c);
            rows = new SMTree(Info((STable)d.objects.Lookup(table), cols));
        }
        public SIndex(STransaction tr,SIndex x,AStream f):base(x,f)
        {
            table = tr.Fix(x.table);
            long[] c = new long[x.cols.Length];
            var i = 0;
            for (var b = x.cols.First(); b != null; b = b.Next())
                c[i++] = tr.Fix(b.Value);
            cols = SList<long>.New(c);
            rows = x.rows;
        }
        public override STransaction Commit(STransaction tr, int c, AStream f)
        {
            return base.Commit(tr, f);
        }
        SList<TreeInfo> Info(STable tb,SList<long> cols)
        {
            if (cols == null)
                return SList<TreeInfo>.Empty;
            return Info(tb, cols.next).InsertAt(new TreeInfo(tb.cols.Lookup(cols.element).name, 'D', 'D'),0);
        }
        public override string ToString()
        {
            var sb = new StringBuilder( "Index "+uid+" ["+table+"] (");
            var cm ="";
            for (var b = cols.First();b!=null;b=b.Next())
            {
                sb.Append(cm);cm = ",";
                sb.Append(""+b.Value);
            }
            sb.Append(")");
            return sb.ToString();
        }
    }
}
