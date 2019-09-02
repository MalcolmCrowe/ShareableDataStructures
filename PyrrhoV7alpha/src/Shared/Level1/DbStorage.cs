using System;
using System.IO;
using System.Collections;
using Pyrrho.Common;
using System.Net.Sockets;
using System.Threading;
using System.Text;
using PyrrhoBase;
using Pyrrho.Level2;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code 
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland
namespace Pyrrho.Level1
{
	/// <summary>
	/// Storage management for Pyrrho
	/// Storage object for a logical file on disk
	/// 
	/// The logical file has a maximum extension of long.MaxValue bytes
	/// The file may be split into physical pieces with extra digits added before the file extension
	/// e.g. file.1.pfl for the first extension of file whose first piece is file.pfl
	/// 
	/// At present each physical piece has a maximum size of 32GB = 0x8 0000 0000
	/// and all pieces except the last will have exactly this length
    /// 
    /// Since v5.0 we need to be able to transact file creation (e.g. partition reconfiguration).
	/// </summary>
    internal class DbStorage : IDbStorage
    {
        /// <summary>
        /// Constructor: a new DbStorage
        /// </summary>
        /// <param name="fname">a logical file name</param>
        public DbStorage(string fname) // e.g. "file.pfl"
        {
            int ix = fname.LastIndexOf(".");
            basename = fname;
            if (ix > 0 && DbData.ext==fname.Substring(ix))
                basename = fname.Substring(0, ix);
        }
        protected DbStorage()
        { }
        // logical file properties
        /// <summary>
        /// The basename (without the .osp extension)
        /// </summary>
        public string basename = "";
        public override string Basename
        {
            get { return basename; }
        }
        public bool Eof { get { return Position == Length; } }
        // instrumentation
        public long reads = 0L;
        public long writes = 0L;
        public long flushes = 0L;
        // deferred creation stuff
        Initialisation initialisation = null;
        internal bool creating = false;
        // physical file manipulation stuff
        /// <summary>
        /// The stream uses an array of filestreams to implement it
        /// </summary>
        public FileStream[] files = null;
        /// <summary>
        /// The current stream
        /// </summary>
        FileStream cur = null;
        /// <summary>
        /// Index of the current stream
        /// </summary>
        int curIndex = -1;
        /// <summary>
        /// Server configurable: the number of bits for segmenting physical files
        /// By default is 35 (gives 32GB segments)
        /// </summary>
        public static int SegmentationBits
        {
            get { return maxSizeBits; }
            set
            {
                maxSizeBits = value;
                maxSize = 1L << maxSizeBits;
            }
        }
        static long maxSize = 0x800000000; // of an individual physical file
        static int maxSizeBits = 35;
        /// <summary>
        /// Open a given database file
        /// </summary>
        /// <param name="mode">Read or ReadWrite</param>
        /// <returns>true if at least one files was opened</returns>
        public override bool Open() // the first Open with ReadWrite will succeed; later Read will work
        {
            if (initialisation != null)
                return true;
            int n = 0;
#if SILVERLIGHT
            while (store.FileExists(NameFor(n)))
#else
            while (File.Exists(NameFor(n)))
#endif
                n++;
            files = new FileStream[n];
            for (int j = 0; j < n; j++)
            {
#if (SILVERLIGHT)
                files[j] = store.OpenFile(NameFor(j), FileMode.Open);
#else
                var sname = NameFor(j);
                try
                {
                    files[j] = new FileStream(sname, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                }
                catch (IOException)
                {
                    throw new DBException("3D000", NameFor(j)).ISO();
                }
#endif
                if (j < n - 1 && files[j].Length != maxSize)
                    throw new DBException("3D006", NameFor(j)).Mix();
            }
            if (n > 0)
            {
                curIndex = 0;
                cur = files[0];
                return true;
            }
            return false;
        }
        /// <summary>
        /// Create a new database stream in the current set
        /// </summary>
        public virtual void Create()
        {
            initialisation = new Initialisation(this);
            creating = true;
        }
        /// <summary>
        /// Compute the name for the jth file
        /// </summary>
        /// <param name="j">the index of the file</param>
        /// <returns>the name for this file</returns>
        string NameFor(int j)
        {
            string s;
            if (j == 0)
                s= PyrrhoStart.path+basename + DbData.ext;
            else
                s= PyrrhoStart.path+basename + "." + j + DbData.ext;
            return s;
        }
        /// <summary>
        /// Configure the stream to be readable
        /// </summary>
        public override bool CanRead { get { return true; } }
        /// <summary>
        /// configure the stream to be seekable
        /// </summary>
        public override bool CanSeek { get { return true; } }
        /// <summary>
        /// configure the stream to be writable
        /// </summary>
        public override bool CanWrite { get { return true; } }
        /// <summary>
        /// The total length of the multifile stream
        /// </summary>
        public override long Length
        {
            get
            {
                if (initialisation != null)
                    return initialisation.length;
                int n = files.Length - 1;
                if (n < 0)
                    return 0;
                long r = (n << maxSizeBits) + files[n].Length;
                return r;
            }
        }
        /// <summary>
        /// Disallow setting the length
        /// </summary>
        /// <param name="n"></param>
        public override void SetLength(long n)
        {
            throw new PEException("PE30");
        }
        /// <summary>
        /// Compute the current position in the multifile stream
        /// </summary>
        public override long Position
        {
            get {
                if (initialisation != null)
                    return initialisation.position;
                return (curIndex << maxSizeBits) + cur.Position; }
            set { Seek(value, SeekOrigin.Begin); }
        }
        /// <summary>
        /// Close the stream
        /// </summary>
        public override void Close()
        {
 //           Console.WriteLine("Closing " + NameFor(0));
            if (files == null)
                return;
            if (initialisation != null)
            { // discard everything
                initialisation = null;
                creating = false;
                files = null;
                return;
            }
            flushes++;
            if (cur!=null)
                cur.Flush();
            for (int j = 0; j < files.Length; j++)
                files[j].Close();
            files = null;
        }
        /// <summary>
        /// Flush data to the stream
        /// </summary>
        public override void Flush()
        {
            if (initialisation != null)
                return;
            flushes++;
            cur.Flush();
            if (PyrrhoStart.TutorialMode||PyrrhoStart.CheckMode)
                Console.WriteLine("DbStorage.Flush complete for "+NameFor(0)+" len "+cur.Position);
        }
        /// <summary>
        /// Move to the next file
        /// </summary>
        void NextFile()
        {
            cur = files[++curIndex];
            cur.Seek(0L, SeekOrigin.Begin);
        }
        /// <summary>
        /// Read data into a buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        /// <param name="off">the offset</param>
        /// <param name="count">the number of bytes to read</param>
        /// <returns>the number actually read</returns>
        public override int Read(byte[] buf, int off, int count)
        {
            if (initialisation != null)
                return initialisation.Read(buf, off, count);
            long p = Position;
            long ln = Length;
            if (p == ln)
                return -1;
            if (p + count > ln)
                count = (int)(ln - p);
            int r = 0;
            long d = cur.Position + count - maxSize;
            if (d > 0)
            {
                r = DoRead(buf, off, (int)(count - d));
                NextFile();
                r += DoRead(buf, (int)(off + count - d), (int)d);
            }
            else
                r = DoRead(buf, off, count);
            if (cur.Position == maxSize)
                NextFile();
            p += r;
            return r;
        }
        /// <summary>
        /// read implementation
        /// </summary>
        /// <param name="buf">an array</param>
        /// <param name="off">an offset</param>
        /// <param name="count">number of bytes to read</param>
        /// <returns>number actually read</returns>
        int DoRead(byte[] buf, int off, int count)
        {
            int r = 0;
            while (count - r > 0)
            {
                int n = cur.Read(buf, off + r, count - r);
                reads++;
                if (n == 0)
                    throw new PEException("PE31");
                r += n;
            }
            return r;
        }
        /// <summary>
        /// Seek in the multifile
        /// </summary>
        /// <param name="off">the offset desired</param>
        /// <param name="so">the seek mode</param>
        /// <returns>the old position</returns>
        public override long Seek(long off, SeekOrigin so)
        {
//            PyrrhoServer.Trace("DbStorage Seek " + off + so);
            if (initialisation != null)
                return initialisation.Seek(off, so);
            switch (so)
            {
                case SeekOrigin.Begin:
                    curIndex = (int)(off >> maxSizeBits);
                    cur = files[curIndex];
                    return ((long)(curIndex) << maxSizeBits) +
                        cur.Seek(off & (maxSize - 1), SeekOrigin.Begin);
                case SeekOrigin.Current:
                    return Seek(Position + off, SeekOrigin.Begin);
                case SeekOrigin.End:
                    return Seek(Length + off, SeekOrigin.Begin);
            }
            return 0;  // not reached
        }
        /// <summary>
        /// Create the next file in the sequence
        /// </summary>
        void CreateNextFile()
        {
            int n = files.Length;
            FileStream[] ns = new FileStream[n + 1];
            for (int j = 0; j < n; j++)
                ns[j] = files[j];
            files = ns;
            curIndex = n;
            cur = new FileStream(NameFor(n), FileMode.Create, FileAccess.ReadWrite, FileShare.None);
            files[n] = cur;
        }
        /// <summary>
        /// Write to the multifile stream
        /// </summary>
        /// <param name="buf">the buffer</param>
        /// <param name="off">the offset</param>
        /// <param name="count">the count</param>
        public override void Write(byte[] buf, int off, int count)
        {
            if (initialisation != null)
            {
                initialisation.Write(buf, off, count);
                return;
            }
            long p = cur.Position;
            long d = p + count - maxSize;
            if (d > 0)
            {
                cur.Write(buf, off, (int)(count - d));
                writes++;
                CreateNextFile();
                cur.Write(buf, (int)(off + count - d), (int)d);
            }
            else
                cur.Write(buf, off, count);
            writes++;
            if (cur.Position - p != count)
                throw new PEException("PE32");
        }
        public override int ReadByte()
        {
            throw new PEException("PE33");
        }
        public override void WriteByte(byte b)
        {
            throw new PEException("PE34");
        }
        internal virtual void Abort()
        { }

        public override void Commit()
        {
            if (initialisation == null)
                return;
            var init = initialisation;
            creating = false;
            initialisation = null; // allow real writes and seeks
#if (SILVERLIGHT)
            FileStream fs = store.CreateFile(NameFor(0));
#else
            var n = NameFor(0);        
            FileStream fs = new FileStream(n, FileMode.Create, FileAccess.ReadWrite, FileShare.None);
#endif
            files = new FileStream[1];
            files[0] = fs;
            cur = fs;
            curIndex = 0;
            // now use the local methods
            for (var e=init.writes.First();e!= null;e=e.Next())
            {
                Seek(e.key(), SeekOrigin.Begin);
                Write(e.value(), 0, e.value().Length);
            }
            Flush();
        }

        public virtual bool Remote { get {return false;} }

        internal class Initialisation
        {
            public DbStorage store;
            public long position = 0;
            public long length = 0;
            public ATree<long, byte[]> writes = BTree<long, byte[]>.Empty;
            public Initialisation(DbStorage s) { store = s; }

            internal int Read(byte[] buf, int off, int count)
            {
                long x = 0;
                byte[] b = null;
                for (var e =writes.First();e!= null;e=e.Next())
                    if (e.key() <= position && (e.key() + e.value().Length) > position)
                    {
                        x = e.key();
                        b = e.value();
                        break;
                    }
                if (b == null)
                    return 0;
                var p = (int)(position-x);
                var r = b.Length-p;
                if (count>r)
                    count = r;
                for (int j = 0; j < count; j++)
                    buf[off + j] = b[p + j];
                return count;
            }

            internal long Seek(long off, SeekOrigin so)
            {
                var r = position;
                switch (so)
                {
                    case SeekOrigin.Begin: position = off; break;
                    case SeekOrigin.Current: position += off; break;
                    case SeekOrigin.End: position = length + off; break;
                }
                return r;
            }

            internal void Write(byte[] buf, int off, int count)
            {
                var n = count-off;
                byte[] b = new byte[n];
                for(int j = 0;j<n;j++)
                    b[j] = buf[j+off];
                ATree<long, byte[]>.Add(ref writes, position, b);
                position += n;
                length += n;
            }
        }
    }

}
