using System;
using System.Text;

namespace Shareable
{
    public class SQuery : SDbObject
    {
        public readonly SList<Serialisable> cpos;
        public readonly SDict<string, Serialisable> names;
        public readonly SDict<long, SSelector> cols;
        public SQuery(Types t, long u) : base(t, u)
        {
            cols = SDict<long, SSelector>.Empty;
            cpos = SList<Serialisable>.Empty;
            names = SDict<string, Serialisable>.Empty;
        }
        public SQuery(Types t, STransaction tr) : base(t, tr)
        {
            cols = SDict<long, SSelector>.Empty;
            cpos = SList<Serialisable>.Empty;
            names = SDict<string, Serialisable>.Empty;
        }
        public SQuery(SQuery q) : base(q)
        {
            cols = q.cols;
            cpos = q.cpos;
            names = q.names;
        }
        public SQuery(SQuery q,SDict<long,SSelector>co,SList<Serialisable>cp,SDict<string,Serialisable>cn) :base(q)
        {
            cols = co;
            cpos = cp;
            names = cn;
        }
        /// <summary>
        /// This constructor is used at the point where a client Query has reached the server and we are
        /// replacing string-only selectors with actual SColumns. However, the Serialisables in the given
        /// select-list are not necessarily simple column names, so we build up the transformed query one 
        /// column at a time.
        /// </summary>
        /// <param name="t"></param>
        /// <param name="c"></param>
        /// <param name="nms"></param>
        protected SQuery(Types t,SList<Serialisable> c,SDict<string,Serialisable>nms) : base(t,-1)
        {
            var q = new SQuery(t, -1);
            var n = c.Length;
            for (var i = 0; i < n; i++, c=c.next)
                q = c.element.Lookup(nms).Analyse(q, i, nms);
            cols = q.cols;
            cpos = q.cpos;
            names = q.names;
        }
        protected SQuery(Types t, Reader f) : base(t, f)
        {
            cols = SDict<long, SSelector>.Empty;
            cpos = SList<Serialisable>.Empty;
            names = SDict<string, Serialisable>.Empty;
        }
        protected SQuery(SQuery q, AStream f) : base(q, f)
        {
            cols = q.cols;
            cpos = q.cpos;
            names = q.names;
        }
        /// <summary>
        /// Construct the Rowset for the given SDatabase (may have changed since SQuery was built)
        /// </summary>
        /// <param name="db">The current state of the database or transaction</param>
        /// <returns></returns>
        public virtual RowSet RowSet(SDatabase db)
        {
            throw new NotImplementedException();
        }
        public override void Append(StringBuilder sb)
        {
            base.Append(sb);
        }
        public override string ToString()
        {
            return "SQuery";
        }
    }
    public class SSearch : SQuery
    {
        public readonly SQuery sce;
        public readonly Serialisable alias;
        public readonly SList<Serialisable> where;
        public SSearch(Reader f):base(Types.SSearch,f)
        {
            sce = f._Get(null) as SQuery ?? throw new Exception("Query expected");
            alias = f._Get(null);
            var w = SList<Serialisable>.Empty;
            var n = f.GetInt();
            for (var i=0;i<n;i++)
                w = w.InsertAt(f._Get(null),i);
            where = w;
        }
        public SSearch(SQuery s,Serialisable a, SList<Serialisable> w)
            :base(Types.SSearch,-1)
        {
            sce = s;
            alias = a;
            where = w;
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            sce.Put(f);
            alias.Put(f);
            f.PutInt(where.Length);
            for (var b=where.First();b!=null;b=b.Next())
                b.Value.Put(f);
        }
        public new static SSearch Get(Reader f)
        {
            return new SSearch(f);
        }
        public override RowSet RowSet(SDatabase db)
        {
            return new SearchRowSet(db, this);
        }
        public override void Append(StringBuilder sb)
        {
            sce.Append(sb);
            sb.Append(" where ");
            var cm = "";
            for (var b=where.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = " and ";
                b.Value.Append(sb); 
            }
        }
    }
    public class SSelectStatement : SQuery
    {
        public readonly SQuery qry;
        public readonly SList<SOrder> order;
        public SSelectStatement(SList<Serialisable> c, SQuery q, SList<SOrder> or) 
            : base(Types.SSelect, c, q.names)
        { qry = q; order = or; }
        public static SSelectStatement Get(SDatabase db,Reader f)
        {
            var n = f.GetInt();
            var c = SList<Serialisable>.Empty;
            for (var i = 0; i < n; i++)
                c = c.InsertAt(f._Get(null) as SSelector, i);
            n = f.GetInt();
            var q = f._Get(db) as SQuery;
            var o = SList<SOrder>.Empty;
            for (var i = 0; i < n; i++)
                o = o.InsertAt(f._Get(null) as SOrder, i);
            return new SSelectStatement(c,q,o);
        }
        public override void Put(StreamBase f)
        {
            base.Put(f);
            f.PutInt(cpos.Length);
            for (var b = cols.First(); b != null; b = b.Next())
                b.Value.val.Put(f);
            qry.Put(f);
        }
        public override void Append(StringBuilder sb)
        {
            base.Append(sb);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Select ");
            var cm = "";
            for (var b = cols.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.Value);
            }
            sb.Append(' ');
            return sb.ToString();
        }

        public override RowSet RowSet(SDatabase db)
        {
            var r = new SelectRowSet(db,this);
            return r;
        }
    }
    public class SOrder : Serialisable
    {
        public readonly Serialisable corr;
        public readonly SColumn col;
        public readonly bool desc;
        public SOrder(SDatabase db,Reader f) :base(Types.SOrder)
        {
            corr = f._Get(null);
            col = f._Get(db) as SColumn;
            desc = f.ReadByte() == 1;
        }
    }
}
