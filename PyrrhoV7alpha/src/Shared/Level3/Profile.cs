using System;
using System.Collections.Generic;
using System.Xml;
using System.IO;
using Pyrrho.Level2;
using Pyrrho.Common;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

namespace Pyrrho.Level3
{
    /// <summary>
    /// A transaction profile: used to collect statistics about similar transactions
    /// </summary>
    class TransactionProfile
    {
        /// <summary>
        /// The set of tables affected in this profile
        /// </summary>
        public BTree<long,TableProfile> tables = BTree<long, TableProfile>.Empty;
        /// <summary>
        /// The transaction profile identity
        /// </summary>
        public int id;
        /// <summary>
        /// number of times this profile has occurred
        /// </summary>
        public int num = 1;
        /// <summary>
        /// whether this transaction profile changes the database schema
        /// </summary>
        public bool schema = false;
        /// <summary>
        /// how many transaction conflicts occurred.
        /// we do not count erroneous transactions as failures
        /// </summary>
        public int fails = 0;
        /// <summary>
        /// Constructor
        /// </summary>
        public TransactionProfile() { }
        /// <summary>
        /// Constructor: a transaction profile for a given database profile
        /// </summary>
        /// <param name="p">the database profile</param>
        public TransactionProfile(Profile p)
        {
            p.transactions.Add(this);
        }
        /// <summary>
        /// Return a table profile for a given table, creating a new one if needed
        /// </summary>
        /// <param name="tb">the table defpos</param>
        /// <returns>the table profile</returns>
        public TableProfile Get(long tb)
        {
            if (!tables.Contains(tb))
                tables+=(tb, new TableProfile(tb));
            return tables[tb];
        }
        /// <summary>
        /// Consider whether two transaction profiles can be merged
        /// </summary>
        /// <param name="t">A transaction profile</param>
        /// <returns>Whether it matched</returns>
        public bool Matches(TransactionProfile t)
        {
            if (tables.Count != t.tables.Count || schema!=t.schema)
                return false;
            var a = tables.First();
            var b = t.tables.First();
            for (;a!=null && b!=null ; a=a.Next(),b=b.Next())
            {
                if (a.key()!=b.key())
                    return false;
                if (!a.value().Matches(b.value()))
                    return false;
            }
            return a == null && b == null;
        }

 /*       internal void Show(StringBuilder s)
        {
            s.Append("TRANSACTION occurrences "+num + " of which "+fails+" failed\n\r");
            if (schema)
                s.Append("Alterations to database schema\n\r");
            foreach (Slot<long> t in tables)
                ((TableProfile)(t.Value)).Show(s);
        } */
        public override string ToString()
        {
   //         StringBuilder sb = new StringBuilder();
   //         Show(sb);
   //         return sb.ToString();
            return "Transaction Profile " + id;
        }
        /// <summary>
        /// Save this profile as an XML report
        /// </summary>
        /// <param name="db">The local database</param>
        /// <param name="w">An XML writer</param>
        internal void Save(Database db,XmlWriter w)
        {
            w.WriteStartElement("Transaction");
            w.WriteAttributeString("Id", id.ToString());
            w.WriteAttributeString("Occurrences", num.ToString());
            w.WriteAttributeString("Fails", fails.ToString());
            w.WriteAttributeString("Schema", schema.ToString());
            for (var t = tables.First();t!= null;t=t.Next())
                t.value().Save(db,w);
            w.WriteEndElement();
        }
        /// <summary>
        /// Load this profile from XML
        /// </summary>
        /// <param name="r">An XML reader</param>
        internal void Load(XmlReader r)
        {
            if (r.HasAttributes)
            {
                while(r.MoveToNextAttribute())
                    switch (r.Name)
                    {
                        case "Id": id = int.Parse(r.Value); break;
                        case "Occurrences": num = int.Parse(r.Value); break;
                        case "Fails": fails = int.Parse(r.Value); break;
                        case "Schema": schema = bool.Parse(r.Value); break;
                    }
                r.MoveToElement();
            }
            while (r.Read())
            {
                r.MoveToContent();
                if (r.Name == "Table")
                    new TableProfile(this,r);
                else
                    return;
            }
        }
    }
    /// <summary>
    /// Profile information for how a table has been affected in a transaction
    /// by schema changes, reading, deletiing or inserting/updating records
    /// </summary>
    class TableProfile
    {
        /// <summary>
        /// The defining position of the table
        /// </summary>
        public long tab;
        /// <summary>
        /// The number of deletions from this table in this transaction
        /// </summary>
        public long dels; 
        /// <summary>
        /// Whether this transaction changed the table schema
        /// </summary>
        public bool schema = false;
        /// <summary>
        /// Read constraint information for this profile (for checkUpdate)
        /// </summary>
        public BTree<long, bool> read = BTree<long, bool>.Empty; 
        /// <summary>
        /// Read constraint information for this profile (the checkSpecific Index)
        /// </summary>
        public long ckix = 0; 
        /// <summary>
        /// Read constraint information for this profile (the number of specific records)
        /// </summary>
        public int specific = 0; 
        /// <summary>
        /// Read constraint information for this profile (whether blockUpdate)
        /// </summary>
        public bool blocked = false; 
        /// <summary>
        /// A set of record profiles for this table profile (records affected)
        /// </summary>
        public List<RecordProfile> recs = new List<RecordProfile>(); 
        /// <summary>
        /// Constructor: a new TableProfile for a given table
        /// </summary>
        /// <param name="tb">The table defpos</param>
        public TableProfile(long tb)
        {
            tab = tb;
        }
        /// <summary>
        /// Constructor: a new TableProfile from the XML file
        /// </summary>
        /// <param name="tp">The TransactionProfile this belongs to</param>
        /// <param name="r">The XML reader</param>
        public TableProfile(TransactionProfile tp, XmlReader r)
        {
            Load(r);
            tp.tables+=(tab, this);
        }
        /// <summary>
        /// Add information about a row change: creating a new record profile if needed
        /// </summary>
        /// <param name="r">The level 2 record</param>
        public void Add(Record r)
        {
            foreach (var p in recs)
                if (p.Matches(r.fields))
                {
                    p.num++;
                    return;
                }
             recs.Add(new RecordProfile(r,recs.Count));
        }
        /// <summary>
        /// Find if two table profiles match and should be merged
        /// </summary>
        /// <param name="t">A table profile to test</param>
        /// <returns>whether they match</returns>
        public bool Matches(TableProfile t)
        {
            if (tab != t.tab || dels != t.dels || recs.Count != t.recs.Count || schema != t.schema)
                return false;
            for (int i = 0; i < recs.Count; i++)
                if (recs[i].num != t.recs[i].num || !recs[i].Matches(t.recs[i].fields))
                    return false;
            if (read.Count != t.read.Count)
                return false;
            var a = read.First();
            var b = t.read.First();
            for(;a!=null&&b!= null; a=a.Next(), b=b.Next())
                if (a.key() != b.key())
                    return false;
            if (ckix != t.ckix || specific != t.specific || blocked != t.blocked)
                return false;
            return true;
        }
        /// <summary>
        /// Save a table profile to the XML file. The database is used to get table names
        /// </summary>
        /// <param name="db">the Database</param>
        /// <param name="w">the XML writer</param>
        internal void Save(Database db,XmlWriter w)
        {
            Table tb = db.objects[tab] as Table;
            var ti = db.role.infos[tb.defpos] as ObInfo;
            w.WriteStartElement("Table");
            w.WriteAttributeString("Name", ti.name);
            w.WriteAttributeString("Pos", tab.ToString());
            w.WriteAttributeString("Schema", schema.ToString());
            w.WriteAttributeString("Dels", dels.ToString());
            w.WriteAttributeString("Ckix", ckix.ToString());
            w.WriteAttributeString("Specific", specific.ToString());
            w.WriteAttributeString("Blocked", blocked.ToString());
            for (var s = read.First();s!= null;s=s.Next())
            {
                TableColumn tc = db.objects[s.key()] as TableColumn;
                var i = 0;
                for (var b=ti.domain.rowType.First();b!=null;b=b.Next(),i++)
                    if (b.value() == s.key())
                        break;
                var ci = (ObInfo)db.role.infos[tc.defpos];
                w.WriteStartElement("Read");
                w.WriteAttributeString("ColPos", s.key().ToString());
                w.WriteAttributeString("ReadCol", ci.name);
                w.WriteEndElement();
            }
            foreach (var v in recs)
                v.Save(db,w);
            w.WriteEndElement();
        }
        /// <summary>
        ///  Load a table profile from the XML file
        /// </summary>
        /// <param name="r">The XML reader</param>
        private void Load(XmlReader r)
        {
            if (r.HasAttributes)
            {
                while(r.MoveToNextAttribute())
                    switch (r.Name)
                    {
                        case "Pos": tab = long.Parse(r.Value); break;
                            // don't bother with Name
                        case "Schema": schema = bool.Parse(r.Value); break;
                        case "Dels": dels = int.Parse(r.Value); break;
                        case "Ckix": ckix = long.Parse(r.Value); break;
                        case "Specific": specific = int.Parse(r.Value); break;
                        case "Blocked": blocked = bool.Parse(r.Value); break;
                    }
                r.MoveToElement();
            }
            if (!r.IsEmptyElement)
            {
                while (r.Read())
                {
                    r.MoveToContent();
                    switch (r.Name)
                    {
                        case "Read": new ColumnProfile(this, r); break;
                        case "Record": recs.Add(new RecordProfile(r)); break;
                        default: return;
                    }
                }
            }
        }
    }
    /// <summary>
    /// This class just manages a column identity in a profile (as defpos)
    /// </summary>
    class ColumnProfile
    {
        /// <summary>
        /// The defpos
        /// </summary>
        public long pos;
        /// <summary>
        /// Constructor: for a Read constraint column in a table profile
        /// </summary>
        /// <param name="tp">The table profile</param>
        /// <param name="r">The XML reader</param>
        public ColumnProfile(TableProfile tp,XmlReader r)
        {
            while (r.MoveToNextAttribute())
                switch (r.Name)
                {
                    case "ColPos": pos = long.Parse(r.Value); break;
                        // ignore ReadCol
                }
            r.MoveToElement();
            tp.read+=(pos, true);
        }
        /// <summary>
        /// Constructor: for a Record column in a record profile
        /// </summary>
        /// <param name="rp">The record profile</param>
        /// <param name="r">The XML reader</param>
        public ColumnProfile(RecordProfile rp, XmlReader r)
        {
            while (r.MoveToNextAttribute())
                switch (r.Name)
                {
                    case "ColPos": pos = long.Parse(r.Value); break;
                        // ignore RecCol
                }
            r.MoveToElement();
            rp.fields+=(pos, null);
        }
    }
    /// <summary>
    /// A record profile: a set of similar insertions or updates.
    /// Similar in the sense that (new) values are provided for a set of columns.
    /// </summary>
    class RecordProfile
    {
        /// <summary>
        /// The set of affected columns
        /// </summary>
        public BTree<long, TypedValue> fields;
        /// <summary>
        /// An identifier for this record profile
        /// </summary>
        public int id;
        /// <summary>
        /// the number of rows affected in this way in this transaction
        /// </summary>
        public long num;
        /// <summary>
        /// Constructor: a profile for a given Level 2 Record structure
        /// </summary>
        /// <param name="r">The Record</param>
        /// <param name="i">The record profile identity</param>
         public RecordProfile(Record r,int i)
        {
            fields = r.fields; num = 1; id = i;
        }
        /// <summary>
        /// Constructor: a profile being read from an XML file
        /// </summary>
        /// <param name="r">The XML reader</param>
        public RecordProfile(XmlReader r)
        {
            Load(r);
        }
        /// <summary>
        /// Determine whether a record profile matches a given set of columns
        /// </summary>
        /// <param name="f">The set of fields</param>
        /// <returns>Whether they match</returns>
        public bool Matches(BTree<long, TypedValue> f)
        {
            if (fields.Count != f.Count)
                return false;
            var a = f.First();
            var b = fields.PositionAt(0);
            for (;a!=null & b!=null;a=a.Next(),b=b.Next())
                if (a.key() != b.key())
                    return false;
            return true;
        }
 /*       internal void Show(StringBuilder s)
        {
            s.Append("Record occurrences = " + num);
            string comma = " [";
            foreach (Slot<long> e in rec.fields)
            {
                s.Append(comma); s.Append(e.Key);
                comma = ",";
            }
            s.Append("]\n\r");
        } */
        /// <summary>
        /// Save a rceord profile to an XML file.
        /// The database is used to get current column names
        /// </summary>
        /// <param name="db">The database</param>
        /// <param name="w">The XML writer</param>
        internal void Save(Database db,XmlWriter w)
        {
            w.WriteStartElement("Record");
            w.WriteAttributeString("Id", id.ToString());
            w.WriteAttributeString("Occurrences", num.ToString());
            for (var s = fields.PositionAt(0);s!= null;s=s.Next())
            {
                TableColumn tc = db.objects[s.key()] as TableColumn;
                var ci = db.role.infos[tc.defpos] as ObInfo;
                w.WriteStartElement("Field");
                w.WriteAttributeString("ColPos", s.key().ToString());
                w.WriteAttributeString("RecCol", ci.name);
                w.WriteEndElement();
            }
            w.WriteEndElement();
        }
        /// <summary>
        /// Load a record profile from an XML file
        /// </summary>
        /// <param name="r">The XML reader</param>
        private void Load(XmlReader r)
        {
            if (r.HasAttributes)
            {
                while(r.MoveToNextAttribute())
                    switch (r.Name)
                    {
                        case "Id": id = int.Parse(r.Value); break;
                        case "Occurrences": num = int.Parse(r.Value); break;
                    }
                r.MoveToElement();
            }
            fields = BTree<long,  TypedValue>.Empty;
            if (!r.IsEmptyElement)
            {
                while (r.Read())
                {
                    r.MoveToContent();
                    if (r.Name == "Field")
                        new ColumnProfile(this, r);
                    else return;
                }
            }
        }

    }
#if MONGO
    /// <summary>
    /// This class added for MongoDB explain and profile features
    /// </summary>
    internal class QueryProfile
    {
        internal enum Cursor { Basic, Btree, GeoSearch }
        internal Cursor cursor = Cursor.Basic;
        internal Index index = null;
        internal long nresult = 0;
        internal long nscannedobjects = 0;
        internal bool scanAndOrder = false;
        internal bool indexOnly = false;
        internal long millis = 0;
        internal TDocument max = null;
        internal TDocument min = null;
        internal TDocument sort = null;
        internal string server = 
#if EMBEDDED
            "local";
#else
            PyrrhoStart.cfg.hp.ToString();
#endif
        internal List<QueryProfile> clauses = new List<QueryProfile>();
        internal List<QueryProfile> shards = new List<QueryProfile>();
        internal string comment = "";
        internal bool? explain = null;
        internal string hint = null;
        internal long maxScan = -1L;
        internal bool? returnKey = null;
        internal bool? showDiskLoc = null;
        internal bool? snapshot = null;
        internal bool? natural = null;
    }
#endif
    /// <summary>
    /// A database profile consists of a set of non-matching transaction profiles
    /// and a set of query profiles
    /// </summary>
    class Profile
    {
        /// <summary>
        /// The profiles being maintained (databases can be profiled or not)
        /// </summary>
        public static List<Profile> profiles = new List<Profile>();
        /// <summary>
        /// The name of the database
        /// </summary>
        public long dbname;
        /// <summary>
        /// The set of transaction profiles for this database
        /// </summary>
        public List<TransactionProfile> transactions = new List<TransactionProfile>();
#if MONGO
        /// <summary>
        /// The set of query profiles for this database: accumulated from transactions
        /// </summary>
        public List<QueryProfile> queries = new List<QueryProfile>();
#endif
         /// <summary>
        /// Constructor: a new profile for a database
        /// </summary>
        /// <param name="dbn">the database name</param>
        public Profile(long dbn)
        {
            dbname = dbn;
            profiles.Add(this);
        }
        /// <summary>
        /// Add a transaction to the profile
        /// </summary>
        /// <param name="t">The local database</param>
        /// <param name="success">Whether the transaction has succeeded</param>
        /// <returns>The new or updated transaction profile</returns>
        public TransactionProfile AddProfile(Transaction t,bool success)
        {
            if (t == null)
                return null;
            TransactionProfile tp = new TransactionProfile();
            if (!success)
                tp.fails = 1;
            for (var b = t.physicals.First(); b != null; b = b.Next())
            {
                var p = b.value();
                switch (p.type)
                {
                    case Physical.Type.Record:
                        var r = p as Record;
                        tp.Get(r.tabledefpos).Add(r);
                        break;
                    case Physical.Type.Record1: goto case Physical.Type.Record;
                    case Physical.Type.Record2: goto case Physical.Type.Record;
                    case Physical.Type.Update: goto case Physical.Type.Record;
                    case Physical.Type.Delete:
                        tp.Get((p as Delete).tabledefpos).dels++;
                        break;
                    case Physical.Type.EndOfFile: break;
                    case Physical.Type.Alter:
                        tp.Get((p as Alter).table.defpos).schema = true;
                        break;
                    case Physical.Type.Change: goto case Physical.Type.Alter;
                    default:
                        tp.schema = true;
                        break;
                }
            }
 /*           for (var tb = t.rdC.First();tb!= null;tb=tb.Next())
            {
                var r = tb.value();
                if (r.check != null)
                    r.check.Profile(t,tp.Get(tb.key()));
            } */
            foreach(var pp in transactions)
                if (pp.Matches(tp))
                {
                    pp.num++;
                    if (!success)
                        pp.fails++;
                    return pp;
                }
            tp.id = transactions.Count;
            transactions.Add(tp);
            return tp;
        }
   /*     public void AddProfile(Physical p) // during Database Load
        {
            switch (p.type)
            {
                case Physical.Type.PTransaction:
                    EndTransactionProfile();
                    cur = new TransactionProfile();
                    break;
                case Physical.Type.PImportTransaction:
                    goto case Physical.Type.PTransaction;
                case Physical.Type.PTransaction2:
                    goto case Physical.Type.PTransaction;
                case Physical.Type.EndOfFile:
                    goto case Physical.Type.PTransaction;
                case Physical.Type.Record:
                    var r = p as Record;
                    if (cur != null && r != null)
                        cur.Get(r.tabledefpos).Add(r);
                    break;
                case Physical.Type.Record1: goto case Physical.Type.Record;
                case Physical.Type.Record2: goto case Physical.Type.Record;
                case Physical.Type.Update: goto case Physical.Type.Record;
                case Physical.Type.Delete:
                    var d = p as Delete;
                    if (cur != null && d != null)
                        cur.Get(((Record)p.database.Get(d.delpos)).tabledefpos).dels++;
                    break;
                default: cur = null;
                    break;
            }
        } */
 /*       public void EndTransactionProfile()
        {
            if (cur != null)
            {
                foreach (var pp in transactions)
                    if (pp.Matches(cur))
                    {
                        pp.num++;
                        return;
                    }
                transactions.Add(cur);
            }
        } */
 /*       public override string  ToString()
        {
            StringBuilder s = new StringBuilder();
            s.Append("PROFILE for " + dbname+"\n\r");
            foreach (var p in transactions)
                p.Show(s);
            return s.ToString();
        } */
#if !SILVERLIGHT && !WINDOWS_PHONE
        /// <summary>
        /// Save the profile data to an xml file.
        /// This is done if profiling is turned off.
        /// </summary>
        /// <param name="db">The database for which profiling is to be saved</param>
        public void Save(Database db)
        {
            XmlWriter w = XmlWriter.Create(dbname + ".xml");
            w.WriteStartElement("Profile");
            w.WriteAttributeString("Date", DateTime.Now.ToString());
            foreach (var t in transactions)
                t.Save(db,w);
            w.WriteEndElement();
            w.Close();
        }
        /// <summary>
        /// Load the profile data from a saved xml file (if such exists).
        /// </summary>
        public void Load()
        {
            if (!File.Exists(dbname + ".xml"))
                return;
            XmlReader r = new XmlTextReader(dbname + ".xml");
            r.MoveToContent();
            if (r.Name != "Profile")
                return;
            while (r.Read())
            {
                r.MoveToContent();
                if (r.Name == "Transaction")
                    new TransactionProfile(this).Load(r);
            }
            r.Close();
        }
#endif
    }
}
