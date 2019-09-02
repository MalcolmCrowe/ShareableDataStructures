using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level4
{
    /// <summary>
    /// This class implements a virtual table that consists of insertion, update and delete actions on a table.
    /// Assumed it is in the first database in the connection string
    /// </summary>
	internal class LogTableSelect: SystemRowSet
	{
        public readonly Table tb;
        /// <summary>
        /// Constructor: Build a rowset of log information about table records
        /// </summary>
        /// <param name="q">The From</param>
        public LogTableSelect(Transaction tr,Context cx,Table q)
            : base(tr,cx,q)
        {
            tb = q;
        }
        public override RowBookmark First(Context _cx)
        {
            for (var bk = new LogTableSelectBookmark(_cx,this,0,(null,5)); bk!=null;
                bk=(LogTableSelectBookmark)bk.Next(_cx))
            {
                if (bk._pt!=null)
                    return bk;
            }
            return null;
        }
        public override RowBookmark PositionAt(Context _cx,PRow key)
        {
            return base.PositionAt(_cx,key);
        }
    }
    /// <summary>
    /// The enumerator for the logtableselect rowset
    /// </summary>
    internal class LogTableSelectBookmark : SystemRowSet.LogSystemBookmark
    {
        /// <summary>
        /// The transaction record
        /// </summary>
        internal readonly PTransaction _pt;
        internal readonly Physical _ph;
        internal readonly long _next;
        internal readonly LogTableSelect _lts;
        public override TRow row => LogTableSelectEntry(_ph);

        public override TRow key => LogTableSelectEntry(_ph);

        /// <summary>
        /// Constructor: for the log table select rowset
        /// </summary>
        /// <param name="f"></param>
        public LogTableSelectBookmark(Context _cx,LogTableSelect rs,int pos,(Physical,long) p,
            PTransaction t=null,long nx=0) :base(_cx,rs,pos,p)
        {
            _pt = t;
            _ph = p.Item1;
            _lts = rs;
            _next = nx;
        }
        /// <summary>
        /// Build an entry for the logtable enumerator
        /// </summary>
        /// <param name="ph">the physical of interest</param>
        /// <returns>the link for the enumerator current value</returns>
        public TRow LogTableSelectEntry(Physical ph)
        {
            Physical pr = ph;
            string s = "";
            long df = -1;
            switch (ph.type)
            {
                case Physical.Type.Record: s = "Insert"; df = ph.ppos; break;
                case Physical.Type.Update: s = "Update";
                    Update u = (Update)ph;
                    df = u.defpos;
                    break;
                case Physical.Type.Delete:
                    Delete d = (Delete)ph;
                    pr = db.GetD(d.delRow.defpos);
                    s = "Delete";
                    df = d.delRow.defpos;
                    break;
            }
            var rc = pr as Record;
            var rw = new TypedValue[_lts.rowType.Length];
            rw[0] = new TInt(ph.ppos);
            rw[1] = new TChar(s);
            rw[2] = new TInt(df);
            rw[3] = new TInt(_pt.ppos);
            rw[4] = new TInt(_pt.time);
            for (var c = rc.fields.First(); c != null; c = c.Next())
                if (c.value() != null)
                {
                    var sl = _rs.rowType.names["" + c.key()];
                    rw[sl.seq] = new TChar(c.value().ToString());
                }
            return new TRow(_rs.rowType, rw);
        }
        /// <summary>
        /// The current value
        /// </summary>
        public override TRow CurrentValue()
        {
            return LogTableSelectEntry(ph);
        }
        /// <summary>
        /// Move to the next entry in the table log
        /// </summary>
        /// <returns>whether there is a next entry</returns>
        public override RowBookmark Next(Context _cx)
        {
            var ph1 = _ph;
            var ppos1 = _ph.ppos;
            var next = _next;
            var pt1 = _pt;
            var from = res.qry as Table;
            for (; ; )
            {
                if (ppos1 == db.loadpos)
                    return null;
                var end = ppos1;
                ph1 = db.Get(ref next);
                if (ph1 == null)
                    return null;
                switch (ph1.type)
                {
                    case Physical.Type.PTransaction:
                        {
                            pt1 = (PTransaction)ph1;
                            continue;
                        }
                    case Physical.Type.Record:
                        {
                            Record r = (Record)ph1;
                            if (r.tabledefpos != from.defpos)
                                continue;
                            break;
                        }
                    case Physical.Type.Update:
                        {
                            Update u = (Update)ph;
                            if (u.tabledefpos != from.defpos)
                                continue;
                            break;
                        }
                    case Physical.Type.Delete:
                        {
                            Delete d = (Delete)ph;
                            Physical pr = db.GetD(d.delRow.defpos);
                            Record r = (Record)pr;
                            if (r.tabledefpos != from.defpos)
                                continue;
                            break;
                        }
                    default: continue;
                }
                return new LogTableSelectBookmark(_cx,_lts, _pos+1, (ph1,nextpos), pt1,next);
            }
        }
    }
}
