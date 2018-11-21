using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shareable
{
    public abstract class SQuery : SDbObject
    {
        public readonly SList<SSelector> cpos;
        public readonly SDict<string, SSelector> names;
        public readonly SDict<long, SSelector> cols;
        public SQuery(Types t, long u) : base(t, u)
        {
            cols = SDict<long, SSelector>.Empty;
            cpos = SList<SSelector>.Empty;
            names = SDict<string, SSelector>.Empty;
        }
        public SQuery(Types t, STransaction tr) : base(t, tr)
        {
            cols = SDict<long, SSelector>.Empty;
            cpos = SList<SSelector>.Empty;
            names = SDict<string, SSelector>.Empty;
        }
        public SQuery(SQuery q) : base(q)
        {
            cols = q.cols;
            cpos = q.cpos;
            names = q.names;
        }
        protected SQuery(SQuery q, SDict<long, SSelector> c, SList<SSelector> p, SDict<string, SSelector> n) : base(q)
        {
            cpos = p;
            cols = c;
            names = n;
        }
        protected SQuery(Types t, Reader f) : base(t, f)
        {
            cols = SDict<long, SSelector>.Empty;
            cpos = SList<SSelector>.Empty;
            names = SDict<string, SSelector>.Empty;
        }
        protected SQuery(SQuery q, AStream f) : base(q, f)
        {
            cols = q.cols;
            cpos = q.cpos;
            names = q.names;
        }
        /// <summary>
        /// Queries come to us with client-local SDbObjects instead of STransaction SDbObjects. 
        /// We need to look them up
        /// </summary>
        /// <param name="db">A database or transaction</param>
        /// <returns>A version of this with correct references for db</returns>
        public abstract SQuery Lookup(SDatabase db);
        /// <summary>
        /// Construct the Rowset for the given SDatabase (may have changed since SQuery was built)
        /// </summary>
        /// <param name="db">The current state of the database or transaction</param>
        /// <returns></returns>
        public abstract RowSet RowSet(SDatabase db);
    }
    public class SSearch : SQuery
    {
        public readonly SQuery sce;
        public readonly SDict<SSelector,Serialisable> where;
        public SSearch(SDatabase db, Reader f):base(Types.SSearch,f)
        {
            sce = f._Get(db) as SQuery ?? throw new Exception("Query expected");
            var w = SDict<SSelector, Serialisable>.Empty;
            var n = f.GetInt();
            for (var i=0;i<n;i++)
            {
                var k = f._Get(db) as SSelector ?? throw new Exception("Selector expected");
                k = k.Lookup(sce);
                w = w.Add(k, f._Get(db));
            }
            where = w;
        }
        public SSearch(SSearch s,SQuery q,SDict<SSelector,Serialisable> w)
            :base(s)
        {
            sce = q;
            where = w;
        }
        public override void Put(StreamBase f)
        {
            f.WriteByte((byte)type);
            sce.Put(f);
            f.PutInt(where.Length);
            for (var b=where.First();b!=null;b=b.Next())
            {
                b.Value.key.Put(f);
                b.Value.val.Put(f);
            }
        }
        public override SQuery Lookup(SDatabase db)
        {
            var s = sce.Lookup(db);
            var w = SDict<SSelector, Serialisable>.Empty;
            for (var b = where.First(); b != null; b = b.Next())
                w = w.Add(b.Value.key.Lookup(s), b.Value.val);
            return new SSearch(this,s,w);
        }
        public override RowSet RowSet(SDatabase db)
        {
            return new SearchRowSet(db, this);
        }
    }
}
