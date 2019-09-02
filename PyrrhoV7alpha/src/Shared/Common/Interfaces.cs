using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pyrrho.Common
{
    /// <summary>
    /// All data in the database is strongly typed
    /// </summary>
    public interface ITypedValue
    {
        int _CompareTo(object obj);
        bool IsNull {get;}
    }
    /// <summary>
    /// All rowsets are traversed using IBookmarks.
    /// A traversal list or tree provides a First(..) method 
    /// returning an IBookmark for the row (or null if the object is empty)
    /// Next() gives the next bookmark in the traversal or null if exhausted
    /// </summary>
    /// <typeparam name="V">The row type</typeparam>
    public interface IBookmark<V>
    {
        IBookmark<V> Next(); // null if none
        V Value();
        long Position();
    }
    /// <summary>
    /// An Exception class for reporting internal errors :(
    /// </summary>
    public class PEException : Exception // Pyrrho Engine failure
    {
        public PEException(string mess)
            : base(mess)
        {
            Console.WriteLine("PE Exception: " + mess);
        }
    }
    /// <summary>
    /// An Exception class conforming to the SQL standard
    /// </summary>
    public class DatabaseError : Exception
    {
        public DatabaseError(string sig, string mess)
            : base(sig)
        {
        }
    }
}
