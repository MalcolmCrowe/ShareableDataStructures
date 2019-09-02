using System;
using System.IO;
using Pyrrho.Common;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

// OPEN SOURCE EDITIONS
namespace Pyrrho.Level1
{
	/// <summary>
	/// This is the main data file operations manager for Pyrrho
	/// A database is defined precisely by the contents of a single
	/// logical file managed by this module.
	/// 
	/// By convention the file extension is .pfl.
	/// The first few bytes contain a version identifier so that this
	/// level can decide if it is a valid file.
	/// 
	/// </summary>
	public class DbData
	{
        public const string ext = ".osp";
        /// <summary>
        /// The name of the database
        /// </summary>
        public string name;
        /// <summary>
        /// DataFile has a small write buffer of 2K bytes.
        /// 2019 allows for extra AsyncStream bytes and headers for server-server traffic.
        /// </summary>
		protected const int bufSize = 2019;
		protected byte[] buffer;
        long basepos;
        int off, count;
        public bool readOnly = false;
        public FileStream file;
        /// <summary>
        /// Constructor: a new DataFile for the file
        /// </summary>
        /// <param name="r">the file</param>
        internal DbData(string n)
        {
            name = n;
            file = new FileStream(n, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            StartReading(0);
        }
        void StartReading(long p)
        {
            buffer = new byte[bufSize];
            basepos = p;
            off = 0;
            lock (file)
            {
                file.Seek(p, SeekOrigin.Begin);
                count = file.Read(buffer, 0, bufSize);
            }
        }
        /// <summary>
        /// The start of the write buffer
        /// </summary>
		protected long bufStart;
        /// <summary>
        /// The current position in the write buffer
        /// </summary>
		protected int bufPos;
        /// <summary>
        /// Read into a buffer
        /// </summary>
        /// <param name="buf">The buffer</param>
        /// <param name="offset">The position in the buffer</param>
        /// <param name="count">The number of bytes to read</param>
        /// <returns>the number of bytes read</returns>
        public int Read(byte[] buf, int offset, int count) // LOCKED
        {
            return file.Read(buf, offset, count);
        }
        /// <summary>
        /// Writing a byte uses the buffer defined in this class
        /// </summary>
        /// <param name="b"></param>
		public void WriteByte(byte b) // LOCKED
		{
            if (bufPos >= bufSize)
            {
                file.Write(buffer, 0, bufSize);
                bufStart += bufSize;
                bufPos = 0;
            }
            buffer[bufPos++] = b;
		}
        /// <summary>
        /// Write from an array
        /// </summary>
        /// <param name="buf">An array of bytes</param>
        /// <param name="off">Starting point in the array</param>
        /// <param name="count">The number of bytes to write</param>
		public void Write(byte[] buf,int off,int count) // LOCKED
		{
            if (bufPos + count > bufSize)
            {
                file.Write(buffer, 0, bufPos);
                bufStart += bufPos;
                bufPos = 0;
            }
            if (count > bufSize)
            {
                file.Write(buf, off, count);
                bufPos = 0;
                bufStart += count;
            }
            else
                for (int j = 0; j < count; j++)
                {
                    byte b = buf[j + off];
                    buffer[bufPos++] = b;
                }
		}
        /// <summary>
        /// The length of the datafile
        /// </summary>
		public long Length 
		{
            get
            {
                 return file.Length + bufPos;
            }		
		}
        /// <summary>
        /// Must only be called during write (when locked)
        /// </summary>
		public long Position // LOCKED
		{ 
			get 
			{
				return bufStart+bufPos;
			}
		}
        /// <summary>
        /// Close the DataFile
        /// </summary>
		public void Close() // LOCKED
		{
       //     Check();
			Flush();
			file.Close();
		}
        /// <summary>
        /// StartCommit position the write buffer at the end of the file
        /// </summary>
        /// <param name="frompos">the new highwatermark</param>
        /// <returns>the new position</returns>
		public virtual long StartCommit(long fromPos) // LOCKED
        {
            Seek(fromPos, SeekOrigin.Begin);
            bufStart = fromPos;
            bufPos = 0;
            return bufStart;
        }
        /// <summary>
        /// Peek at the next couple of bytes if we can
        /// </summary>
        /// <param name="i">0 for next byte in the buffer, 1 for the one after</param>
        /// <returns>-1 if we can't, otherwise the byte</returns>
        internal int Peek(int i)
        {
            if (off + i >= Count)
                return -1; // we can't peek
            return buffer[off + i];
        }
        protected virtual bool EoF()
        {
            return off >= Count;
        }
        /// <summary>
        /// Create a new Physical record from the buffer
        /// </summary>
        /// <returns>a Physical</returns>
        internal Physical Create()
        {
            if (EoF())
                return null;
            long m = basepos + off;
            Physical.Type tp = (Physical.Type)GetByte();
            Physical p;
            switch (tp)
            {
                default: throw new PEException("PE35");
                case Physical.Type.Alter: p = new Alter(this, m); break;
                case Physical.Type.Alter2: p = new Alter2(this, m); break;
                case Physical.Type.Alter3: p = new Alter3(this, m); break;
                case Physical.Type.AlterRowIri: p = new AlterRowIri(this, m); break;
                case Physical.Type.Change: p = new Change(this, m); break;
                case Physical.Type.Checkpoint: p = new Checkpoint(this, m); break;
                case Physical.Type.Curated: p = new Curated(this, m); break;
                case Physical.Type.Delete: p = new Delete(this, m); break;
                case Physical.Type.Drop: p = new Drop(this, m); break;
                case Physical.Type.Edit: p = new Edit(this, m); break;
                case Physical.Type.EndOfFile: // ignore
                    p = new EndOfFile(this, m); break;
                case Physical.Type.Grant: p = new Grant(this, m); break;
                case Physical.Type.Metadata: p = new PMetadata(this, m); break;
                case Physical.Type.Modify: p = new Modify(this, m); break;
                case Physical.Type.Namespace: p = new Namespace(this, m); break;
                case Physical.Type.Ordering: p = new Ordering(this, m); break;
#if !(LOCAL || EMBEDDED)
                case Physical.Type.Partitioned: p = new Partitioned(this, m); break;
#endif
                case Physical.Type.PCheck: p = new PCheck(this, m); break;
                case Physical.Type.PCheck2: p = new PCheck2(this, m); break;
                case Physical.Type.PColumn: p = new PColumn(this, m); break;
                case Physical.Type.PColumn2: p = new PColumn2(this, m); break;
                case Physical.Type.PColumn3: p = new PColumn3(this, m); break;
                case Physical.Type.PDateType: p = new PDateType(this, m); break;
                case Physical.Type.PDomain: p = new PDomain(this, m); break;
                case Physical.Type.PDomain1: p = new PDomain1(this, m); break;
                case Physical.Type.PeriodDef: p = new PPeriodDef(this, m); break;
                case Physical.Type.PImportTransaction: p = new PImportTransaction(this, m); break;
                case Physical.Type.PIndex: p = new PIndex(this, m); break;
                case Physical.Type.PIndex1: p = new PIndex1(this, m); break;
                case Physical.Type.PMethod: p = new PMethod(tp, this, m); break;
                case Physical.Type.PMethod2: p = new PMethod(tp, this, m); break;
                case Physical.Type.PProcedure: p = new PProcedure(tp, this, m); break;
                case Physical.Type.PProcedure2: p = new PProcedure(tp, this, m); break;
                case Physical.Type.PRole: p = new PRole(this, m); break;
                case Physical.Type.PRole1: p = new PRole(this, m); break;
                case Physical.Type.PTable: p = new PTable(this, m); break;
                case Physical.Type.PTable1: p = new PTable1(this, m); break;
                case Physical.Type.PTransaction: p = new PTransaction(this, m); break;
                case Physical.Type.PTransaction2: p = new PTransaction2(this, m); break;
                case Physical.Type.PTrigger: p = new PTrigger(this, m); break;
                case Physical.Type.PType: p = new PType(this, m); break;
                case Physical.Type.PType1: p = new PType1(this, m); break;
                case Physical.Type.PUser: p = new PUser(this, m); break;
                case Physical.Type.PView: p = new PView(this, m); break;
                case Physical.Type.PView1: p = new PView1(this, m); break; //obsolete
                case Physical.Type.RestView: p = new PRestView(this, m); break;
                case Physical.Type.RestView1: p = new PRestView1(this, m); break;
                case Physical.Type.Record: p = new Record(this, m); break;
                case Physical.Type.Record1: p = new Record1(this, m); break;
                case Physical.Type.Record2: p = new Record2(this, m); break;
                case Physical.Type.Reference: p = new Reference(this, m); break;
                case Physical.Type.Revoke: p = new Revoke(this, m); break;
                case Physical.Type.Update: p = new Update(this, m); break;
                case Physical.Type.Versioning: p = new Versioning(this, m); break;
#if !(LOCAL || EMBEDDED)
                case Physical.Type.Partition: p = new Partition(this, m); break;
#endif
                case Physical.Type.Reference1: p = new Reference1(this, m); break;
                case Physical.Type.ColumnPath: p = new PColumnPath(this, m); break;
                case Physical.Type.Metadata2: p = new PMetadata2(this, m); break;
                case Physical.Type.PIndex2: p = new PIndex2(this, m); break;
                case Physical.Type.DeleteReference1: p = new DeleteReference1(this, m); break;
                case Physical.Type.Authenticate: p = new Authenticate(this, m); break;
                case Physical.Type.TriggeredAction: p = new TriggeredAction(this, m); break;
                case Physical.Type.Metadata3: p = new PMetadata3(this, m); break;
                case Physical.Type.RestView2: p = new PRestView2(this, m); break;
                case Physical.Type.Audit: p = new Audit(this, m); break;
                case Physical.Type.Classify: p = new Classify(this, m); break;
                case Physical.Type.Clearance: p = new Clearance(this, m); break;
                case Physical.Type.Enforcement: p = new Enforcement(this, m); break;
                case Physical.Type.Record3: p = new Record3(this, m); break;
                case Physical.Type.Update1: p = new Update1(this, m); break;
            }
            p.Deserialise(this);
            p.CheckDate();
            return p;
        }
        /// <summary>
        /// Get an Int32 from the buffer
        /// Used only for the EndOfFile marker
        /// </summary>
        /// <returns>an int</returns>
        internal int GetInt32()
        {
            int v = 0;
            for (int j = 0; j < 4; j++)
                v = (v << 8) + GetByte();
            return v;
        }
        /// <summary>
        /// Get an Integer from the buffer
        /// </summary>
        /// <returns>an Integer</returns>
        public object GetInteger()
        {
            int n = GetByte();
            byte[] b = new byte[n];
            for (int j = 0; j < n; j++)
                b[j] = (byte)GetByte();
            Integer r = new Integer(b);
            if (n <= 8)
                return (long)r;
            return r;
        }
        /// <summary>
        /// Get an Integer from the buffer
        /// </summary>
        /// <returns>an Integer</returns>
        public Integer GetInteger0()
        {
            int n = GetByte();
            byte[] b = new byte[n];
            for (int j = 0; j < n; j++)
                b[j] = (byte)GetByte();
            return new Integer(b);
        }
        /// <summary>
        /// Get an int from the buffer
        /// Used when we know the data will fit in 4 bytes
        /// </summary>
        /// <returns>an int</returns>
        public int GetInt()
        {
            return (int)(long)GetInteger();
        }
        /// <summary>
        /// Get a long from the buffer
        /// Used when we know the data will fit in 8 bytes
        /// </summary>
        /// <returns>a long</returns>
        public long GetLong()
        {
            return (long)GetInteger();
        }
        /// <summary>
        /// Get a Numeric from the buffer
        /// </summary>
        /// <returns>a new Numeric</returns>
        public Common.Numeric GetDecimal()
        {
            Integer m = GetInteger0();
            return new Common.Numeric(m, GetInt());
        }
        /// <summary>
        /// Get a Real from the buffer
        /// </summary>
        /// <returns>a new real</returns>
        public double GetDouble()
        {
            return GetDecimal();
        }
        /// <summary>
        /// Get a unicode string from the buffer
        /// </summary>
        /// <returns>a new string</returns>
        public string GetString()
        {
            int n = GetInt();
            byte[] cs = new byte[n];
            for (int j = 0; j < n; j++)
                cs[j] = (byte)GetByte();
            return Encoding.UTF8.GetString(cs, 0, cs.Length);
        }
        public Ident GetIdent(long pos, Ident.IDType t = Ident.IDType.NoInput)
        {
            var s = GetString();
            return (s == "") ? null : new Ident(s, t, 0, pos);
        }
        /// <summary>
        /// Get a dateTime from the buffer
        /// </summary>
        /// <returns>a new datetime</returns>
        public DateTime GetDateTime()
        {
            return new DateTime(GetLong());
        }
        /// <summary>
        /// Get an Interval from the buffer
        /// </summary>
        /// <returns>a new Interval</returns>
        public Interval GetInterval()
        {
            var ym = GetByte();
            if (ym == 1)
            {
                var years = GetInt();
                var months = GetInt();
                return new Interval(years, months);
            }
            else
                return new Interval(GetLong());
        }
        /// <summary>
        /// Attempt some backward compatibility
        /// </summary>
        /// <returns></returns>
        public Interval GetInterval0()
        {
            var years = GetInt();
            var months = GetInt();
            var r = new Interval(years, months)
            { ticks = GetLong() };
            if (r.years == 0 && r.months == 0)
                r.yearmonth = false; // low marks for this!
            return r;
        }
        /// <summary>
        /// Get an array of bytes from the buffer
        /// </summary>
        /// <returns>the new byte array</returns>
        public byte[] GetBytes()
        {
            int n = GetInt();
            byte[] b = new byte[n];
            for (int j = 0; j < n; j++)
                b[j] = (byte)GetByte();
            return b;
        }
        /// <summary>
        /// Flush the buffer
        /// </summary>
        public override void Flush() 
        {
            if (bufPos > 0)
            {
                file.Write(buffer, 0, bufPos);
                file.Flush();
                bufStart += bufPos;
            }
            bufPos = 0;
        }
    }

 }
