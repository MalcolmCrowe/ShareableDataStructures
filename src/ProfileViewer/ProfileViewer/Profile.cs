using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Xml;
using System.IO;
using System.Data;
using System.Windows.Forms;
using Pyrrho;

namespace ProfileViewer
{
    class TransactionProfile
    {
        public Dictionary<long,TableProfile> tables = new Dictionary<long,TableProfile>();
        public int id;
        public int num = 1; // number of times this profile has occurred
        public bool schema = false;
        public int fails = 0;
        public TransactionProfile() { }
        public TransactionProfile Copy()
        {
            var r = new TransactionProfile();
            r.id = id;
            r.num = num;
            r.schema = schema;
            r.fails = fails;
            foreach (var t in tables)
                r.tables.Add(t.Key,t.Value.Copy());
            return r;
        }
        public TableProfile Get(long tb)
        {
            if (!tables.ContainsKey(tb))
                tables.Add(tb, new TableProfile(tb));
            return tables[tb];
        }
        public TransactionProfile Matches(TransactionProfile t)
        {
            if (tables.Count != t.tables.Count || schema!=t.schema)
                return null;
            Dictionary<long, TableProfile>.Enumerator a = tables.GetEnumerator();
            Dictionary<long, TableProfile>.Enumerator b = t.tables.GetEnumerator();
            Dictionary<long, TableProfile> c = new Dictionary<long, TableProfile>();
            while (true)
            {
                if ((!a.MoveNext()) || (!b.MoveNext()))
                    break;
                if (a.Current.Key!=b.Current.Key)
                    return null;
                var u = a.Current.Value.Matches(b.Current.Value);
                if (u == null)
                    return null;
                c.Add(a.Current.Key, u);
            }
            foreach (var cc in c)
                t.tables[cc.Key] = cc.Value;
            return t;
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

        internal void Save(XmlWriter w)
        {
            w.WriteStartElement("Transaction");
            w.WriteAttributeString("Id", id.ToString());
            w.WriteAttributeString("Occurrences", num.ToString());
            w.WriteAttributeString("Fails", fails.ToString());
            w.WriteAttributeString("Schema", schema.ToString());
            foreach (KeyValuePair<long,TableProfile> t in tables)
                t.Value.Save(w);
            w.WriteEndElement();
        }

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
            if (!r.IsEmptyElement)
            {
                while (r.Read())
                {
                    r.MoveToContent();
                    if (r.Name == "Table")
                        new TableProfile(this, r);
                    else
                        return;
                }
            }
        }
        public void Show(TreeNodeCollection ct)
        {
            var n = new TreeNode("Transaction " + id + (schema ? " Schema" : "") + " Tables " + tables.Count + " Occurred " + num + " Failed " + fails);
            foreach (var t in tables)
            {
                var tn = new TreeNode();
                long totr = 0;
                bool rwild = false;
                foreach (var rc in t.Value.recs)
                {
                    string s = " [", c = "";
                    foreach (var f in rc.fields)
                    { s += c + f.Value; c = ","; }
                    tn.Nodes.Add("Record Occurs " + (rc.wild ? "*" : rc.num.ToString()) + s + "]");
                    if (rc.wild)
                        rwild = true;
                    totr += rc.num;
                }
                foreach (var rd in t.Value.read)
                    tn.Nodes.Add("Read " + rd.Value);
                tn.Text = "Table " + t.Value.name + (t.Value.schema ? " Schema" : "");
                tn.Text += " Recs " + (rwild?"*":totr.ToString()) + " Dels " + (t.Value.wildDels?"*":t.Value.dels.ToString()) + " Spec " + (t.Value.wildSpecific?"*":t.Value.specific.ToString()) + " Blocked " + t.Value.blocked;
                n.Nodes.Add(tn);
            }
            ct.Add(n);
        }
    }
    class TableProfile
    {
        public long tab;
        public bool wildDels = false;
        public long dels; // number of deletions
        public bool schema = false;
        public Dictionary<long, string> read = new Dictionary<long,string>(); // checkUpdate
        public long ckix = 0; // checkSpecific;
        public bool wildSpecific = false;
        public int specific = 0; // checkSpecific;
        public bool blocked = false; // blockUpdate
        public List<RecordProfile> recs = new List<RecordProfile>(); // records read or written
        public string name;
        public TableProfile(long tb)
        {
            tab = tb;
        }
        public TableProfile(TransactionProfile tp, XmlReader r)
        {
            Load(r);
            tp.tables.Add(tab, this);
        }
        public TableProfile Copy()
        {
            var r = new TableProfile(tab);
            r.name = name;
            r.dels = dels;
            r.schema = schema;
            r.ckix = ckix;
            r.specific = specific;
            r.blocked = blocked;
            r.wildDels = wildDels;
            r.wildSpecific = wildSpecific;
            foreach (var rd in read)
                r.read.Add(rd.Key,rd.Value);
            foreach (var rc in recs)
                r.recs.Add(rc.Copy());
            return r;
        }
  /*      public void Add(Record r)
        {
            foreach (var p in recs)
                if (p.Matches(r.fields))
                {
                    p.num++;
                    return;
                }
             recs.Add(new RecordProfile(r,recs.Count));
        } */
        public TableProfile Matches(TableProfile t)
        {
            TableProfile tc = null;
            if (tab != t.tab || recs.Count != t.recs.Count || schema != t.schema)
                return null;
            if (dels != t.dels)
            {
                t.Copy(ref tc);
                tc.wildDels = true;
            }
            for (int i = 0; i < recs.Count; i++)
            {
                if (!recs[i].Matches(t.recs[i].fields))
                    return null;
                if (recs[i].num != t.recs[i].num)
                {
                    t.Copy(ref tc);
                    tc.recs[i].wild = true;
                }
            }
            if (read.Count != t.read.Count)
                return null;
            Dictionary<long, string>.Enumerator a = read.GetEnumerator();
            Dictionary<long, string>.Enumerator b = t.read.GetEnumerator();
            while (a.MoveNext() && b.MoveNext())
                if (a.Current.Key != b.Current.Key)
                    return null;
            if (ckix != t.ckix || blocked != t.blocked)
                return null;
            if (specific != t.specific && !t.wildSpecific)
            {
                t.Copy(ref tc);
                tc.wildSpecific = true;
            }
            if (tc != null)
                return tc;
            return t;
        }
        void Copy(ref TableProfile tc)
        {
            if (tc == null)
                tc = Copy();
        }

        internal void Save(XmlWriter w)
        {
            w.WriteStartElement("Table");
            w.WriteAttributeString("Pos", tab.ToString());
            w.WriteAttributeString("Schema", schema.ToString());
            w.WriteAttributeString("Dels", dels.ToString());
            w.WriteAttributeString("Ckix", ckix.ToString());
            w.WriteAttributeString("Specific", specific.ToString());
            w.WriteAttributeString("Blocked", blocked.ToString());
            foreach (KeyValuePair<long,string> s in read)
            {
                w.WriteStartElement("Read");
                w.WriteAttributeString("Pos", s.Key.ToString());
                w.WriteEndElement();
            }
            foreach (var v in recs)
                v.Save(w);
            w.WriteEndElement();
        }

        private void Load(XmlReader r)
        {
            if (r.HasAttributes)
            {
                while(r.MoveToNextAttribute())
                    switch (r.Name)
                    {
                        case "Pos": tab = long.Parse(r.Value); break;
                        case "Name": name = r.Value; break;
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
    class RecordProfile
    {
        public Dictionary<long,string> fields = new Dictionary<long,string>();
        public int id;
        public bool wild = false; // makes num a *
        public long num; // number of similar records
        public RecordProfile(XmlReader r)
        {
            Load(r);
        }

        public RecordProfile()
        {
        }
        public RecordProfile Copy()
        {
            var r = new RecordProfile();
            r.id = id;
            r.wild = wild;
            r.num = num;
            foreach (var f in fields)
                r.fields.Add(f.Key, f.Value);
            return r;
        }
        public bool Matches(Dictionary<long,string> f)
        {
            if (fields.Count != f.Count)
                return false;
            Dictionary<long,string>.Enumerator a = f.GetEnumerator();
            Dictionary<long, string>.Enumerator b = fields.GetEnumerator();
            for (; ; )
            {
                if ((!a.MoveNext()) || (!b.MoveNext()))
                    return true;
                if (a.Current.Key != b.Current.Key)
                    return false;
            }
        } 

        internal void Save(XmlWriter w)
        {
            w.WriteStartElement("Record");
            w.WriteAttributeString("Id", id.ToString());
            w.WriteAttributeString("Occurrences", num.ToString());
            foreach (KeyValuePair<long,string> s in fields)
            {
                w.WriteStartElement("Field");
                w.WriteAttributeString("ColPos", s.Key.ToString());
                w.WriteAttributeString("RecCol", s.Value);
                w.WriteEndElement();
            }
            w.WriteEndElement();
        }
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
            fields = new Dictionary<long, string>();
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
    class ColumnProfile
    {
        public long pos;
        public string name;
        public ColumnProfile(TableProfile tp, XmlReader r)
        {
            while (r.MoveToNextAttribute())
                switch (r.Name)
                {
                    case "ColPos": pos = long.Parse(r.Value); break;
                    case "ReadCol": name = r.Value; break;
                }
            r.MoveToElement();
            tp.read.Add(pos, name);
        }
        public ColumnProfile(RecordProfile rp, XmlReader r)
        {
            while (r.MoveToNextAttribute())
                switch (r.Name)
                {
                    case "ColPos": pos = long.Parse(r.Value); break;
                    case "RecCol": name = r.Value; break;
                }
            r.MoveToElement();
            rp.fields.Add(pos, name);
        }
    }
    class CombinedProfile
    {
        public List<TransactionProfile> transactions = new List<TransactionProfile>();
        public TransactionProfile scheme;
        public CombinedProfile(TransactionProfile t)
        {
            scheme = t.Copy();
            transactions.Add(t);
        }
        public void Save(XmlWriter w)
        {
            foreach (var p in transactions)
                p.Save(w);
        }
        public int Show(Form1 fm)
        {
            if (transactions.Count == 1)
                scheme.Show(fm.tree.Nodes);
            else
            {
                var c = new TreeNode("Profile Trans=" + scheme.num + " Fails=" + scheme.fails);
                var cp = new TreeNode("Pattern");
                scheme.Show(cp.Nodes);
                c.Nodes.Add(cp);
                var ct = new TreeNode("Transactions");
                c.Nodes.Add(ct);
                foreach (var p in transactions)
                    p.Show(ct.Nodes);
                fm.tree.Nodes.Add(c);
            }
            return scheme.num;
        }


        internal bool Matches(TransactionProfile t)
        {
            var s = t.Matches(scheme);
            if (s != null)
            {
                scheme = s;
                transactions.Add(t);
                return true;
            }
            return false;
        }
    }
    class Profile
    {
         public string dbname;
        public DateTime date;
        public List<CombinedProfile> combined = new List<CombinedProfile>();
        public Profile(string dbn)
        {
            dbname = dbn;
        }
         public void Save()
        {
            XmlWriter w = XmlWriter.Create(dbname + ".xml");
            w.WriteStartElement("Profile");
            w.WriteAttributeString("Date", DateTime.Now.ToString());
            foreach (var t in combined)
                t.Save(w);
            w.WriteEndElement();
            w.Close();
        }
        public void Load()
        {
            if (!File.Exists(dbname + ".xml"))
                return;
            XmlReader r = new XmlTextReader(dbname + ".xml");
            r.MoveToContent();
            if (r.Name != "Profile")
                return;
            r.MoveToNextAttribute();
            date = DateTime.Parse(r.Value);
            r.MoveToElement();
            while (r.Read())
            {
                r.MoveToContent();
                if (r.Name == "Transaction")
                {
                    var t = new TransactionProfile();
                    t.Load(r);
                    Combine(t);
                }
            }
            r.Close();
        }

        void Combine(TransactionProfile t)
        {
            var m = false;
            foreach (var c in combined)
            {
                if (c.Matches(t))
                {
                    m = true;
                    c.scheme.num += t.num;
                    c.scheme.fails += t.fails;
                    break;
                }
            }
            if (!m)
                combined.Add(new CombinedProfile(t));
        }
        public PyrrhoTable ExecuteTable(PyrrhoConnect db,string select)
        {
            var cmd = (PyrrhoCommand)db.CreateCommand();
            cmd.CommandText = select;
            var rdr = (PyrrhoReader)cmd.ExecuteReader();
            if (rdr == null)
                return null;
            var t = rdr.GetSchemaTable();
            rdr.Close();
            return (PyrrhoTable)t;
        }
        internal void Fetch()
        {
            var db = new PyrrhoConnect("Files=" + dbname);
            db.Open();
            var tprof = ExecuteTable(db,"table \"Profile$\"");
            var trdc = ExecuteTable(db,"table \"Profile$ReadConstraint\"");
            var trec = ExecuteTable(db,"table \"Profile$Record\"");
            var trccol = ExecuteTable(db,"table \"Profile$RecordColumn\"");
            var ttab = ExecuteTable(db,"table \"Profile$Table\"");
            foreach (var r in tprof.Rows)
            {
                var t = new TransactionProfile();
                t.id = (int)(long)r["Id"];
                t.num = (int)(long)r["Occurrences"];
                t.fails = (int)(long)r["Fails"];
                t.schema = (bool)r["Schema"];
                foreach (var tr in ttab.Rows)
                {
                    if (((int)(long)tr["Id"]) != t.id)
                        continue;
                    var tab = new TableProfile((long)tr["Pos"]);
                    t.tables.Add(tab.tab, tab);
                    tab.name = (string)tr["Table"];
                    tab.blocked = (bool)tr["BlockAny"];
                    tab.dels = (int)(long)tr["Dels"];
                    tab.specific = (int)(long)tr["ReadRecs"];
                    tab.schema = (bool)tr["Schema"];
                    foreach (var rc in trec.Rows)
                    {
                        if (((int)(long)rc["Id"]) != t.id)
                            continue;
                        if (((string)rc["Table"]) != tab.name)
                            continue;
                        var rec = new RecordProfile();
                        rec.id = (int)(long)rc["Rid"];
                        rec.num = (long)rc["Recs"];
                        rec.fields = new Dictionary<long, string>();
                        foreach (var col in trccol.Rows)
                        {
                            if (((int)(long)col["Id"]) != t.id)
                                continue;
                            if (((string)col["Table"]) != tab.name)
                                continue;
                            if (((int)(long)col["Rid"]) != rec.id)
                                continue;
                            rec.fields.Add((long)col["ColPos"], (string)col["RecCol"]);
                        }
                        tab.recs.Add(rec);
                    }
                    foreach (var rd in trdc.Rows)
                    {
                        if (((int)(long)rd["Id"]) != t.id)
                            continue;
                        if (((string)rd["Table"]) != tab.name)
                            continue;
                        tab.read.Add((long)rd["ColPos"], (string)rd["ReadCol"]);
                    }
                }
                Combine(t);
            }
            db.Close();
        }
    }
}
