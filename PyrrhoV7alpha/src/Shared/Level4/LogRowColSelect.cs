using System;
using System.Collections.Generic;
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
    /// This class implements a virtual table that consists of information extracted from the log, relevant to a particular cell in a base table.
    /// It is assumed that we just have one database in the connection string.
    /// </summary>
    internal class LogRowColSelect : SystemRowSet
    {
        /// <summary>
        /// The ident
        /// </summary>
        public TableColumn tcol;
        /// <summary>
        /// The defining position of the row
        /// </summary>
        public long rowpos;
        /// <summary>
        /// Constructor: Build a rowset of log information about a table cell
        /// </summary>
        /// <param name="q">The query</param>
        /// <param name="q">The row position</param>
        /// <param name="c">the ident</param>
        /// <param name="t">the table</param>
        /// <param name="e">An enumerator for the Log</param>
        public LogRowColSelect(Transaction tr,Context cx,From q, long d, TableColumn c, Table t) 
            :base(tr,cx,q)
        {
            tcol = c;
            rowpos = d;
        }
        public override RowBookmark First(Context _cx)
        {
            return new LogRowColSelectBookmark(_cx,this);
        }
    }
    /// <summary>
    /// The log enumerator for the log row col select table
    /// </summary>
    internal class LogRowColSelectBookmark :RowBookmark
    {
        internal readonly LogRowColSelect _rcs;
        internal readonly Database db;
        /// <summary>
        /// The starting position in the log
        /// </summary>
        readonly long _ppos;
        /// <summary>
        /// The previous position in the log
        /// </summary>
        readonly long _oldpos;
        /// <summary>
        /// A physical
        /// </summary>
        readonly Physical _ph;
        /// <summary>
        /// the value found
        /// </summary>
        readonly TRow _val;
        /// <summary>
        /// The starting transaction
        /// </summary>
        readonly PTransaction _pst;
        /// <summary>
        /// the ending transaction
        /// </summary>
        readonly PTransaction _pet;
        readonly bool _del;
        /// <summary>
        /// Constructor: the enumerator for the logrowcolselect enumerator
        /// </summary>
        internal LogRowColSelectBookmark(Context _cx,LogRowColSelect r,int pos=0,long ppos=0,long oldpos=0,Physical ph=null,
            TRow val=null,PTransaction pst=null,PTransaction pet=null,bool del=false) 
            :base(_cx,r,pos,ph.ppos)
        {
            _rcs = r;
            _oldpos = oldpos;
            _ph = ph;
            _val = val;
            _pst = pst;
            _pet = pet;
            _del = del;
        }

        public override TRow row => _val;

        public override TRow key => _val;

        /// <summary>
        /// Move to the next logrowcol select log entry
        /// </summary>
        /// <returns>whether there is a next entry</returns>
        public override RowBookmark Next(Context _cx)
        {
            var ppos = _ppos;
            var oldpos = _oldpos;
            var ph = _ph;
            var val = _val;
            var pst = _pst;
            var pet = _pet;
            var del = _del;
            if (ppos == db.loadpos || del)
                return null;
            pst = pet;
            pet = null;
            oldpos = ph.ppos;
            Record rec = ph as Record;
            if (rec == null)
                return null;
            for (; ; )
            {
                if (ppos == db.loadpos)
                {
                    pet = null;
                    goto done;
                }
                ph = db.Get(ref ppos);
                switch (ph.type)
                {
                    case Physical.Type.PTransaction:
                        {
                            pet = (PTransaction)ph;
                            break;
                        }
                    case Physical.Type.Update:
                        {
                            var u = (Record)ph;
                            if (u.defpos == _rcs.rowpos && 
                                u.fields.Contains(_rcs.tcol.defpos) && u.fields[_rcs.tcol.defpos] != val)
                                    goto done;
                            break;
                        }
                    case Physical.Type.Delete:
                        {
                            Delete d = (Delete)ph;
                            if (d.delpos == _rcs.rowpos)
                            {
                                del = true;
                                goto done;
                            }
                            break;
                        }
                }
                done:
                var ret = new LogRowColSelectBookmark(_cx,_rcs, _pos + 1, ppos, oldpos, ph, val, pst, pet, del);
                if (!del)
                {
                    TypedValue ep = null;
                    TypedValue et = null;
                    if (pet != null)
                    {
                        ep = new TInt(pet.ppos);
                        et = new TDateTime(new DateTime(pet.time));
                    }
                    val = new TRow(_rcs.rowType,
                        new TInt(oldpos), val, new TInt(pst.ppos),
                        new TDateTime(new DateTime(pst.time)), ep, et);
                }
                return ret;
            }
        }
        internal override TableRow Rec()
        {
            throw new NotImplementedException();
        }
    }
}
