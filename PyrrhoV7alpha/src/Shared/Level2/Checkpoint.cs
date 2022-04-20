using Pyrrho.Level3;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.
namespace Pyrrho.Level2
{
    /// <summary>
    /// A checkpoint record contains no obs. It establishes a synchronisation point for a mobile client.
    /// Note - as of Nov 2018 no version of the server creates this record.
    /// It is preserved here for compatibility with existing databases.
    /// </summary>
    internal class Checkpoint : Physical
    {
        /// <summary>
        /// Constructor: a Checkpoint found in the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The position in the buffer</param>
        public Checkpoint(Reader rdr) : base(Type.Checkpoint, rdr) { }
        protected Checkpoint(Checkpoint x, Writer wr) : base(x, wr) { }
        protected override Physical Relocate(Writer wr)
        {
            return new Checkpoint(this, wr);
        }
        internal override void Install(Context cx, long p)
        {
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
        }
        /// <summary>
        /// A readable version of the Checkpoint
        /// </summary>
        /// <returns>The string representation</returns>
        public override string ToString()
        {
            return "Checkpoint";
        }
        public override long Dependent(Writer wr, Transaction tr)
        {
            return -1;
        }
    }
}
