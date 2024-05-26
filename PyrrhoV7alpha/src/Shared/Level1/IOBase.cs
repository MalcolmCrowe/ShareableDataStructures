using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Xml;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

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
        public Buffer buf = new ();
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
    class Box 
    {   
        public byte[] cont;
        public Box(byte[] c) { cont = c;  }
    }
    public class Writer : IOBase
    {
        public Stream file; // shared with Reader(s)
        public long seg = -1;    // The SSegment uid for the start of a Commit once roles are defined
                                 //       internal BTree<long, long?> uids = BTree<long, long?>.Empty; // used for movement of DbObjects
                                 //      internal BTree<long, RowSet> rss = BTree<long, RowSet>.Empty; // ditto RowSets
                                 // fixups: unknownolduid -> referer -> how->bool
        public long segment;  // the most recent PTransaction/PTriggeredAction written
        public long oldStmt; // Where we are with Executables
                             //       public long srcPos,oldStmt,stmtPos; // for Fixing uids
        internal BList<Rvv> rvv = BList<Rvv>.Empty;
        BList<Box> prevBufs = BList<Box>.Empty;
        internal Context cx; // access the database we are writing to
        internal Writer(Context c, Stream f)
        {
            cx = c;
            file = f;
        }
        public long Length => file.Length + prevBufs.Count*Buffer.Size + buf.pos;
        public override void PutBuf()
        {
            file.Seek(0, SeekOrigin.End);
            for (var b = prevBufs.First(); b != null; b = b.Next())
            {
                var bf = b.value().cont;
                if (bf is not null)
                    file.Write(bf, 0, Buffer.Size);
            }
            prevBufs = BList<Box>.Empty;
            file.Write(buf.buf, 0, buf.pos);
            buf.pos = 0;
        }
        public override void WriteByte(byte value)
        {
            if (buf.pos >= Buffer.Size)
            {
                prevBufs += new Box(buf.buf);
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
        internal Ident? PutIdent(Ident? id)
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
        internal User? user;
        internal PTransaction? trans = null;
        public long segment;
        readonly FileStream file;
        public long limit;
        internal BTree<long, Physical.Type> log = BTree<long,Physical.Type>.Empty;
        public bool locked = false;
        internal Reader(Database db, long p)
        {
            context = new Context(db)
            {
                parse = ExecuteStatus.Parse
            };
            file = db._File();
            role = db.role;
            user = (User?)db.objects[db.owner];
            log = db.log;
            limit = file.Length;
            GetBuf(p);
        }
        internal Reader(Context cx)
        {
            if (cx.db is not Database db)
                throw new PEException("PE1001");
            context = new Context(db)
            {
                parse = ExecuteStatus.Parse
            };
            file = db._File();
            log = db.log;
            role = db.role;
            user = (User?)db.objects[db.owner];
            limit = file.Length;
            GetBuf(db.length);
        }
        internal Reader(Context cx, long p, PTransaction? pt = null)
        {
            var db = cx.db;
            context = new Context(db)
            {
                parse = ExecuteStatus.Parse
            };
            file = db._File();
            log = db.log;
            role = db.role;
            user = (User?)db.objects[db.owner];
            limit = file.Length;
            trans = pt;
            GetBuf(p);
        } 
        internal Physical Create()
        {
            if (EoF())
                return new EndOfFile(this);
            Physical.Type tp = (Physical.Type)ReadByte();
            Physical p = tp switch
            {
                Physical.Type.Alter => new Alter(this),
                Physical.Type.Alter2 => new Alter2(this),
                Physical.Type.Alter3 => new Alter3(this),
                Physical.Type.AlterRowIri => new AlterRowIri(this),
                Physical.Type.Change => new Change(this),
                Physical.Type.Curated => new Curated(this),
                Physical.Type.Delete => new Delete(this),
                Physical.Type.Drop => new Drop(this),
                Physical.Type.Edit => new Edit(this),
                Physical.Type.EndOfFile => new EndOfFile(this),
                Physical.Type.Grant => new Grant(this),
                Physical.Type.Metadata => new PMetadata(this),
                Physical.Type.Modify => new Modify(this),
                Physical.Type.Namespace => new Namespace(this),
                Physical.Type.Ordering => new Ordering(this),
                Physical.Type.PCheck => new PCheck(this),
                Physical.Type.PCheck2 => new PCheck2(this),
                Physical.Type.PColumn => new PColumn(this),
                Physical.Type.PColumn2 => new PColumn2(this),
                Physical.Type.PColumn3 => new PColumn3(this),
                Physical.Type.PDateType => new PDateType(this),
                Physical.Type.PDomain => new PDomain(this),
                Physical.Type.PDomain1 => new PDomain1(this),
                Physical.Type.PeriodDef => new PPeriodDef(this),
                Physical.Type.PIndex => new PIndex(this),
                Physical.Type.PIndex1 => new PIndex1(this),
                Physical.Type.PMethod => new PMethod(tp, this),
                Physical.Type.PMethod2 => new PMethod(tp, this),
                Physical.Type.PProcedure => new PProcedure(tp, this),
                Physical.Type.PProcedure2 => new PProcedure(tp, this),
                Physical.Type.PRole => new PRole(this),
                Physical.Type.PRole1 => new PRole(this),
                Physical.Type.PTable => new PTable(this),
                Physical.Type.PTable1 => new PTable1(this),
                Physical.Type.PTransaction => new PTransaction(this),
                //         case Physical.Type.PTransaction2: p = new PTransaction2(this); break;
                Physical.Type.PTrigger => new PTrigger(this),
                Physical.Type.PType => new PType(this),
                Physical.Type.PType1 => new PType1(this),
                Physical.Type.PType2 => new PType2(this),
                Physical.Type.PUser => new PUser(this),
                Physical.Type.PView => new PView(this),
                Physical.Type.RestView => new PRestView(this),
                Physical.Type.RestView1 => new PRestView1(this),
                Physical.Type.Record => new Record(this),
                Physical.Type.Record2 => new Record2(this),
                //          case Physical.Type.Reference: p = new Reference(this); break;
                Physical.Type.Revoke => new Revoke(this),
                Physical.Type.Update => new Update(this),
                Physical.Type.Versioning => new Versioning(this),
                //          case Physical.Type.Reference1: p = new Reference1(this); break;
                Physical.Type.ColumnPath => new PColumnPath(this),
                Physical.Type.Metadata2 => new PMetadata2(this),
                Physical.Type.PIndex2 => new PIndex2(this),
                //          case Physical.Type.DeleteReference1: p = new DeleteReference1(this); break;
                Physical.Type.Authenticate => new Authenticate(this),
                Physical.Type.TriggeredAction => new TriggeredAction(this),
                Physical.Type.Metadata3 => new PMetadata3(this),
                Physical.Type.RestView2 => new PRestView2(this),
                Physical.Type.Audit => new Audit(this),
                Physical.Type.Classify => new Classify(this),
                Physical.Type.Clearance => new Clearance(this),
                Physical.Type.Enforcement => new Enforcement(this),
                Physical.Type.Record3 => new Record3(this),
                Physical.Type.Update1 => new Update1(this),
                Physical.Type.Delete1 => new Delete1(this),
                Physical.Type.Drop1 => new Drop1(this),
                Physical.Type.RefAction => new RefAction(this),
                Physical.Type.PNodeType => new PNodeType(this),
                Physical.Type.PEdgeType => new PEdgeType(this),
                Physical.Type.EditType => new EditType(this),
                Physical.Type.AlterIndex => new AlterIndex(this),
                Physical.Type.AlterEdgeType => new AlterEdgeType(this),
                Physical.Type.Record4 => new Record4(this),
                Physical.Type.Update2 => new Update2(this),
                Physical.Type.Delete2 => new Delete2(this),
                Physical.Type.PSchema => new PSchema(this),
                Physical.Type.PGraph => new PGraph(this),
                Physical.Type.PGraphType => new PGraphType(this),
                _ => throw new PEException("PE35"),
            };
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
        internal (string, Domain) GetColumnDomain(long cp)
        {
            var tc = (TableColumn?)context.db.objects[cp];
            if (tc == null)
                return ("??", Domain.Content);
            var nm = tc.NameFor(context);
            return (nm,tc.domain);
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
        internal Ident? GetIdent()
        {
            var p = context.GetIid();
            var s = GetString();
            return (s == "") ? null : new Ident(s, p);
        }
        /// <summary>
        /// Get a Numeric from the buffer
        /// </summary>
        /// <returns>a new Numeric</returns>
        internal Numeric GetDecimal()
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
        internal DBObject? GetObject(long pp)
        {
            return (DBObject?)context.db.objects[pp];
        }
        internal  void Upd(PColumn3 pc)
        {
            if (pc.dataType is not null && pc.ups != "")
                try
                {
                    pc.upd = new Parser(context).ParseAssignments(pc.ups);
                }
                catch (Exception)
                {
                    pc.upd = CTree<UpdateAssignment, bool>.Empty;
                }
        }
        internal long? Prev(long pv)
        {
            return ((DBObject?)context.db.objects[pv])?.defpos;
        }
        internal void Setup(PDomain pd)
        {
            var ds = pd.domain.defaultString;
            var domain = pd.domain;
            if (ds.Length > 0
                && pd.dataType.kind == Qlx.CHAR && ds[0] != '\'')
                ds = "'" + ds + "'";
            if (ds != "")
                try
                {
                    var dv = Domain.For(domain.kind).Parse(context.db.uid, ds, context);
                    domain += (Domain.Default, dv);
                }
                catch (Exception) { }
            pd.domain = domain;
            if (pd.domdefpos!=-1L)
                context.db += pd.domain;
        }
        internal void Setup(Ordering od)
        {
            od.domain = (Domain)(context.db.objects[od.domdefpos] ?? throw new PEException("PE3006"));
        }
        internal void Add(Physical ph)
        {
            ph.OnLoad(this);
            context.result = -1L;
            context.db.Add(context, ph);
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
        /// <returns>The tree of concurrent physicals</returns>
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

