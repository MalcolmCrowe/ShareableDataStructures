using System;
using Pyrrho.Level3;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level2
{
    /// <summary>
    /// The EndOfFile marker discourages tampering with obs files,
    /// by recording a hash value of the contents. The marker is overwritten
    /// by the next transaction.
    /// </summary>
	internal class EndOfFile : Physical
	{
        public override long Dependent(Writer wr, Transaction tr)
        {
            return -1;
        }
        /// <summary>
        /// Constructor: an end of file marker from the buffer
        /// </summary>
        /// <param name="bp">the buffer</param>
        /// <param name="pos">the defining position</param>
		public EndOfFile(Reader rdr) : base(Type.EndOfFile,rdr) {}
        /// <summary>
        /// A readable version of the Physical
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            return "End of File: ";
        }

        protected override Physical Relocate(Writer wr)
        {
            throw new NotImplementedException();
        }

        internal override DBObject? Install(Level4.Context cx, long p)
        {
            return null;
        }
    }
}

