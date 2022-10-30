using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System;
using System.IO;
using System.Text;
using System.Threading;
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
    public abstract class IOBase
    {
        public class Buffer
        {
            public const int Size = 1024;
            public long start;
            public byte[] buf;
            public int len;
            public int pos;
            public Buffer()
            {
                buf = new byte[Size];
                pos = 0;
            }
            public Buffer(long s, int n)
            {
                buf = new byte[Size];
                pos = 0;
                start = s;
                len = n;
            }
        }
        public Buffer buf = new Buffer();
        public virtual int GetBuf(long s)
        {
            throw new NotImplementedException();
        }
        public virtual void PutBuf()
        {
            throw new NotImplementedException();
        }
        public virtual int ReadByte()
        {
            throw new NotImplementedException();
        }
        public virtual void WriteByte(byte value)
        {
            throw new NotImplementedException();
        }
    }
    public class Writer : IOBase
    {
        public Stream file; // shared with Reader(s)
        public long seg = -1;    // The SSegment uid for the start of a Commit once roles are defined
                                 //       internal BTree<long, long> uids = BTree<long, long>.Empty; // used for movement of DbObjects
                                 //      internal BTree<long, RowSet> rss = BTree<long, RowSet>.Empty; // ditto RowSets
                                 // fixups: unknownolduid -> referer -> how->bool
        public long segment;  // the most recent PTransaction/PTriggeredAction written
        public long oldStmt; // Where we are with Executables
                             //       public long srcPos,oldStmt,stmtPos; // for Fixing uids
        internal BList<Rvv> rvv = BList<Rvv>.Empty;
        BList<byte[]> prevBufs = BList<byte[]>.Empty;
        internal Context cx; // access the database we are writing to
        internal Writer(Context c, Stream f)
        {
            cx = c;
            file = f;
        }
        public long Length => file.Length + buf.pos;
        public override void PutBuf()
        {
            file.Seek(0, SeekOrigin.End);
            for (var b = prevBufs.First(); b != null; b = b.Next())
            {
                var bf = b.value();
                file.Write(bf, 0, Buffer.Size);
            }
            prevBufs = BList<byte[]>.Empty;
            file.Write(buf.buf, 0, buf.pos);
            buf.pos = 0;
        }
        public override void WriteByte(byte value)
        {
            if (buf.pos >= Buffer.Size)
            {
                prevBufs += buf.buf;
                buf.buf = new byte[Buffer.Size];
                buf.pos = 0;
            }
            buf.buf[buf.pos++] = value;
        }
        public void PutInt(int? n)
        {
            if (n == null)
                throw new PEException("Null PutInt");
            PutInteger(new Integer(n.Value));
        }
        internal void PutInteger(Integer b)
        {
            var m = b.Length;
            WriteByte((byte)m);
            for (int j = 0; j < m; j++)
                WriteByte(b[j]);
        }
        public void PutLong(long n)
        {
            PutInteger(new Integer(n));
        }
        public void PutString(string s)
        {
            var cs = Encoding.UTF8.GetBytes(s);
            PutInt(cs.Length);
            for (var i = 0; i < cs.Length; i++)
                WriteByte(cs[i]);
        }
        public void PutBytes(byte[] b)
        {
            PutInt(b.Length);
            for (var i = 0; i < b.Length; i++)
                WriteByte(b[i]);
        }
        internal Ident PutIdent(Ident id)
        {
            if (id == null || id.ident == "")
            {
                PutString("");
                return null;
            }
            var r = new Ident(id.ident, cx.Ix(Length));
            PutString(id.ident);
            return r;
        }

    }

    public class Reader : IOBase
    {
        internal Context context;
        internal Role role;
        internal User user;
        internal PTransaction trans = null;
        public long segment;
        FileStream file;
        public long limit;
        internal BTree<long, Physical.Type> log;
        public bool locked = false;
        internal Reader(Database db, long p)
        {
            context = new Context(db);
            context.parse = ExecuteStatus.Parse;
            file = db._File();
            log = db.log;
            limit = file.Length;
            GetBuf(p);
        }
        internal Reader(Context cx)
        {
            var db = cx.db;
            context = new Context(db);
            context.parse = ExecuteStatus.Parse;
            file = db._File();
            log = db.log;
            role = db.role;
            user = (User)db.objects[db.owner];
            limit = file.Length;
            GetBuf(db.loadpos);
        }
        internal Reader(Context cx, long p, PTransaction pt = null)
        {
            var db = cx.db;
            context = new Context(db);
            context.parse = ExecuteStatus.Parse;
            file = db._File();
            log = db.log;
            role = db.role;
            user = (User)db.objects[db.owner];
            limit = file.Length;
            trans = pt;
            GetBuf(p);
        } 
        internal Physical Create()
        {
            if (EoF())
                return null;
            Physical.Type tp = (Physical.Type)ReadByte();
            Physical p;
            switch (tp)
            {
                default: throw new PEException("PE35");
                case Physical.Type.Alter: p = new Alter(this); break;
                case Physical.Type.Alter2: p = new Alter2(this); break;
                case Physical.Type.Alter3: p = new Alter3(this); break;
                case Physical.Type.AlterRowIri: p = new AlterRowIri(this); break;
                case Physical.Type.Change: p = new Change(this); break;
                case Physical.Type.Checkpoint: p = new Checkpoint(this); break;
                case Physical.Type.Curated: p = new Curated(this); break;
                case Physical.Type.Delete: p = new Delete(this); break;
                case Physical.Type.Drop: p = new Drop(this); break;
                case Physical.Type.Edit: p = new Edit(this); break;
                case Physical.Type.EndOfFile:
                    p = new EndOfFile(this); break;
                case Physical.Type.Grant: p = new Grant(this); break;
                case Physical.Type.Metadata: p = new PMetadata(this); break;
                case Physical.Type.Modify: p = new Modify(this); break;
                case Physical.Type.Namespace: p = new Namespace(this); break;
                case Physical.Type.Ordering: p = new Ordering(this); break;
                case Physical.Type.PCheck: p = new PCheck(this); break;
                case Physical.Type.PCheck2: p = new PCheck2(this); break;
                case Physical.Type.PColumn: p = new PColumn(this); break;
                case Physical.Type.PColumn2: p = new PColumn2(this); break;
                case Physical.Type.PColumn3: p = new PColumn3(this); break;
                case Physical.Type.PDateType: p = new PDateType(this); break;
                case Physical.Type.PDomain: p = new PDomain(this); break;
                case Physical.Type.PDomain1: p = new PDomain1(this); break;
                case Physical.Type.PeriodDef: p = new PPeriodDef(this); break;
                case Physical.Type.PImportTransaction: p = new PImportTransaction(this); break;
                case Physical.Type.PIndex: p = new PIndex(this); break;
                case Physical.Type.PIndex1: p = new PIndex1(this); break;
                case Physical.Type.PMethod: p = new PMethod(tp, this); break;
                case Physical.Type.PMethod2: p = new PMethod(tp, this); break;
                case Physical.Type.PProcedure: p = new PProcedure(tp, this); break;
                case Physical.Type.PProcedure2: p = new PProcedure(tp, this); break;
                case Physical.Type.PRole: p = new PRole(this); break;
                case Physical.Type.PRole1: p = new PRole(this); break;
                case Physical.Type.PTable: p = new PTable(this); break;
                case Physical.Type.PTable1: p = new PTable1(this); break;
                case Physical.Type.PTransaction: p = new PTransaction(this); break;
                //         case Physical.Type.PTransaction2: p = new PTransaction2(this); break;
                case Physical.Type.PTrigger: p = new PTrigger(this); break;
                case Physical.Type.PType: p = new PType(this); break;
                case Physical.Type.PType1: p = new PType1(this); break;
                case Physical.Type.PUser: p = new PUser(this); break;
                case Physical.Type.PView: p = new PView(this); break;
                case Physical.Type.PView1: p = new PView1(this); break; //obsolete
                case Physical.Type.RestView: p = new PRestView(this); break;
                case Physical.Type.RestView1: p = new PRestView1(this); break;
                case Physical.Type.Record: p = new Record(this); break;
                case Physical.Type.Record1: p = new Record1(this); break;
                case Physical.Type.Record2: p = new Record2(this); break;
                //          case Physical.Type.Reference: p = new Reference(this); break;
                case Physical.Type.Revoke: p = new Revoke(this); break;
                case Physical.Type.Update: p = new Update(this); break;
                case Physical.Type.Versioning: p = new Versioning(this); break;
                //          case Physical.Type.Reference1: p = new Reference1(this); break;
                case Physical.Type.ColumnPath: p = new PColumnPath(this); break;
                case Physical.Type.Metadata2: p = new PMetadata2(this); break;
                case Physical.Type.PIndex2: p = new PIndex2(this); break;
                //          case Physical.Type.DeleteReference1: p = new DeleteReference1(this); break;
                case Physical.Type.Authenticate: p = new Authenticate(this); break;
                case Physical.Type.TriggeredAction: p = new TriggeredAction(this); break;
                case Physical.Type.Metadata3: p = new PMetadata3(this); break;
                case Physical.Type.RestView2: p = new PRestView2(this); break;
                case Physical.Type.Audit: p = new Audit(this); break;
                case Physical.Type.Classify: p = new Classify(this); break;
                case Physical.Type.Clearance: p = new Clearance(this); break;
                case Physical.Type.Enforcement: p = new Enforcement(this); break;
                case Physical.Type.Record3: p = new Record3(this); break;
                case Physical.Type.Update1: p = new Update1(this); break;
                case Physical.Type.Delete1: p = new Delete1(this); break;
                case Physical.Type.Drop1: p = new Drop1(this); break;
                case Physical.Type.RefAction: p = new RefAction(this); break;
        /*        case Physical.Type.VIndex: p = new VIndex(this); break; */
            }
            p.Deserialise(this);
            return p;
        }

        public override int GetBuf(long s)
        {
            int m = (limit == 0 || limit >= s + Buffer.Size) ? Buffer.Size : (int)(limit - s);
            bool taken = false;
            try
            {
                if (!locked)
                    Monitor.Enter(file, ref taken);
                file.Seek(s, SeekOrigin.Begin);
                buf.len = file.Read(buf.buf, 0, m);
                buf.pos = 0;
            }
            finally
            {
                if (taken)
                {
                    Monitor.Exit(file);
                    locked = false;
                }
            }
            buf.start = s;
            return buf.len;
        }
        public override int ReadByte()
        {
            if (Position >= limit)
                return -1;
            if (buf.pos == buf.len)
            {
                int n = GetBuf(buf.start + buf.len);
                if (n < 0)
                    return -1;
                buf.pos = 0;
            }
            return buf.buf[buf.pos++];
        }
        public long Position => buf.start + buf.pos;
        /// <summary>
        /// Get the name and Domain for a given TableColumn defpos and ppos
        /// </summary>
        /// <param name="log"></param>
        /// <param name="p"></param>
        /// <returns></returns>
        internal (string, Domain) GetColumnDomain(long cp, long p)
        {
            var tc = (TableColumn)context.db.objects[cp];
            var oi = tc.infos[context.role.defpos];
            var nm = oi.name;
            return (nm,context._Dom(tc));
        }
        internal Integer GetInteger()
        {
            var n = ReadByte();
            var cs = new byte[n];
            for (int j = 0; j < n; j++)
                cs[j] = (byte)ReadByte();
            return new Integer(cs);
        }
        public int GetInt()
        {
            return GetInteger();
        }
        internal int GetInt32()
        {
            var r = 0;
            for (var i = 0; i < 4; i++)
                r = (r << 8) + ReadByte();
            return r;
        }
        public long GetLong()
        {
            return GetInteger();
        }
        public string GetString()
        {
            int n = GetInt();
            byte[] cs = new byte[n];
            for (int j = 0; j < n; j++)
                cs[j] = (byte)ReadByte();
            return Encoding.UTF8.GetString(cs, 0, n);
        }
        internal Ident GetIdent()
        {
            var p = context.GetIid();
            var s = GetString();
            return (s == "") ? null : new Ident(s, p);
        }
        /// <summary>
        /// Get a Numeric from the buffer
        /// </summary>
        /// <returns>a new Numeric</returns>
        internal Common.Numeric GetDecimal()
        {
            Integer m = GetInteger();
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
        internal Interval GetInterval()
        {
            var ym = (byte)ReadByte();
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
        internal Interval GetInterval0()
        {
            var years = GetInt();
            var months = GetInt();
            var ticks = GetLong();
            var r = new Interval(years, months, ticks);
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
                b[j] = (byte)ReadByte();
            return b;
        }
        protected bool EoF()
        {
            return Position >= limit;
        }
        internal void Set(Physical ph)
        {
            ph.trans = trans?.ppos ?? 0;
            ph.time = trans?.pttime ?? 0;
        }
        internal void Segment(Physical ph)
        {
            segment = GetLong();
            ph.trans = segment;
        }
        internal DBObject GetObject(long pp)
        {
            return (DBObject)context.db.objects[pp];
        }
        internal  void Upd(PColumn3 pc)
        {
            if (pc.ups != "")
                try
                {
                    pc.upd = new Parser(context).ParseAssignments(pc.ups, pc.dataType);
                }
                catch (Exception)
                {
                    pc.upd = CTree<UpdateAssignment, bool>.Empty;
                }
        }
        internal long? Prev(long pv)
        {
            return (long?)context.db.objects[pv];
        }
        internal void Setup(PDomain pd)
        {
            TypedValue dv = TNull.Value;
            var ds = pd.domain.defaultString;
            var domain = pd.domain;
            if (ds.Length > 0
                && pd.domain.kind == Sqlx.CHAR && ds[0] != '\'')
                ds = "'" + ds + "'";
            if (ds != "")
                try
                {
                    dv = Domain.For(domain.kind).Parse(context.db.uid, ds, null);
                    domain += (Domain.Default, dv);
                }
                catch (Exception) { }
            pd.domain = domain;
            if (pd.domdefpos!=-1L)
                context.db += (pd.domdefpos, pd.domain, Position);
        }
        internal void Setup(Ordering od)
        {
            od.domain = (Domain)context.db.objects[od.domdefpos];
        }
        internal void Add(Physical ph)
        {
            ph.OnLoad(this);
            context.result = -1L;
            context.db.Add(context, ph, Position);
        }
        /// <summary>
        /// This important routine is part of the Commit sequence. It looks for
        /// physicals committed by concurrent transactions. Because transactions can be
        /// very long, we call this routine twice. The first time, the we do not lock
        /// the file, so we must accept that we may need to give up partway through the
        /// the last complete Physical (this is not a problem).
        /// The second time GetAll is called, the file will already be locked and we want to 
        /// restart from the last Physical boundary.This time if the record is incomplete
        /// we throw an exception.
        /// </summary>
        /// <returns>The list of concurrent physicals</returns>
        internal BList<Physical> GetAll()
        {
            var r = BList<Physical>.Empty;
            try
            {
                for (long p = Position; p < limit; p = Position) // will have moved on
                    r += Create();
            }
            catch (Exception)
            {
                if (locked)
                    throw new Exception("GetAll " + Position);
            }
            return r;
        }
    }


}

