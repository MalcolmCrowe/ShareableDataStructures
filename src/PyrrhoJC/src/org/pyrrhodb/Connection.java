/*
 * Connection.java
 *
 * Created on 25 November 2006, 18:22
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;
import java.lang.reflect.*;
import java.math.BigDecimal;
import java.net.*;
import java.io.*;
import java.util.*;

/**
 *
 * @author Malcolm
 */
public class Connection {
    PyrrhoInputStream inp;
    PyrrhoOutputStream out;
    Crypt crypt;
    boolean autoCommit = true;
    boolean transactionActive = false;
    /* values other than TRANSACTION_SERIALIZABLE are ignored */
    public static int TRANSACTION_NONE = 0;
    public static int TRANSACTION_READ_UNCOMMITTED = 1;
    public static int TRANSACTION_READ_COMMITTED = 2;
    public static int TRANSACTION_REPEATABLE_READ = 4;
    public static int TRANSACTION_SERIALIZABLE = 8;
    public PyrrhoReader rdr = null;
    List<SQLWarning> warnings = new ArrayList<SQLWarning>();
    List<Versioned> posted = new ArrayList<Versioned>();
    public HashMap<String,DataType> dataTypes = new HashMap<String,DataType>();
    public HashMap<String,Procedure>procedures = new HashMap<String,Procedure>();
    /** Creates a new instance of Connection 
     @param properties
     @throws java.io.IOException */
    public Connection(HashMap<String,String> properties) throws IOException {
        String hostName = properties.get("Host");
        if (hostName==null)
            hostName = "localhost";
        String ps = properties.get("Port");
        if (ps==null)
            ps = "5433";
        Socket sock = new Socket(hostName,Integer.parseInt(ps));
        out = new PyrrhoOutputStream(sock.getOutputStream());
        inp = new PyrrhoInputStream(sock.getInputStream());
        crypt = new Crypt(inp,out);
        crypt.sendConnectionString(properties);
        inp.read(); // acknowledgement
    }
    public DatabaseMetaData getMetaData()
    {
        return new DatabaseMetaData(this);
    }
    public void setAutoCommit(boolean b)
    {
        autoCommit = b;
    }
    public void setTransactionIsolation (int level)// throws DatabaseException
    {
 //       if (level!=TRANSACTION_SERIALIZABLE)
 //           throw new DatabaseException("25004");
    }
    public Statement createStatement()
    {
        return new Statement(this);
    }
    public PreparedStatement prepareStatement(String sql)
    {
        return new PreparedStatement(this,sql);
    }
    public CallableStatement prepareCall(String sql)
    {
        return new CallableStatement(this,sql);
    }
    void AcquireTransaction() throws IOException
    {
        if ((!autoCommit) && (!transactionActive))
        {
            Send((byte)6);
            out.flush();
            transactionActive = true;
        }
    }
    public void commit() throws IOException,DatabaseException
    {
        if (transactionActive)
        {
            Send((byte)7);
            out.flush();
            if (Receive()!=11)
                throw new DatabaseException("2E203");
        }
        transactionActive = false;
    }
    public void rollback() throws IOException,DatabaseException
    {
        if (transactionActive)
        {
            Send((byte)8);
            out.flush();
            if (Receive()!=11)
                throw new DatabaseException("2E203");
        }
        transactionActive = false;
    }
    public void close()
    {}
    Object[] getResults(String sql)
    {
        return null;
    }

    void PutInt(int n) throws IOException {
        byte[] b = new byte[4];
        b[0] = (byte)(n>>24);
        b[1] = (byte)(n>>16);
        b[2] = (byte)(n>>8);
        b[3] = (byte)n;
        out.write(b);
    }
    void PutLong(long n) throws IOException {
        byte[] b = new byte[8];
        b[0] = (byte)(n>>56);
        b[1] = (byte)(n>>48);
        b[2] = (byte)(n>>40);
        b[3] = (byte)(n>>32);
        b[4] = (byte)(n>>24);
        b[5] = (byte)(n>>16);
        b[6] = (byte)(n>>8);
        b[7] = (byte)n;
        out.write(b);
    }
    int GetInt() throws IOException {
        byte[] b = new byte[4];
        inp.read(b);
        int n=0;
        for (int j=0;j<4;j++)
            n = (n<<8) + (((int)b[j])&0xff);
        return n;
    }
    long GetLong() throws IOException {
        byte[] b = new byte[8];
        inp.read(b);
        long n=0;
        for (int j=0;j<8;j++)
            n = (n<<8) + (((int)b[j])&0xff);
        return n;
    }
    String GetString() throws IOException {
        int n = GetInt();
        byte[] b = new byte[n];
        if (n>0)
            inp.read(b);
        return new String(b);
    }
    byte[] GetBlob() throws IOException {
        int n = GetInt();
        byte[] b = new byte[n];
        inp.read(b);
        return b;
    }
    PyrrhoRow GetRow(String tn) throws IOException,DocumentException,DatabaseException {
        int n = GetInt();
        ArrayList<Column> cs = new ArrayList<Column>();
        CellValue[] data = new CellValue[n];
        Versioned v = new Versioned();
        for (int j=0;j<n;j++)
        {
            String cn = GetString();
            String dn = GetString();
            int fl = GetInt();
            Column c = new Column(this,cn,dn,fl);
            cs.add(c);
            data[j] = GetCell(c,v);
        }
        PyrrhoRow r = new PyrrhoRow(new ResultSetMetaData(cs,new int[0]),v);
        r.row = data;
        return r;
    }
    PyrrhoArray GetArray(Versioned rc) throws IOException,DocumentException,DatabaseException {
        int n = GetInt();
        String an = GetString();
        String dn = GetString();
        int fl = GetInt();
        Column col = new Column(this,"",dn,fl);
        PyrrhoArray a = new PyrrhoArray(an,col,n);
        for (int j=0;j<n;j++)
            a.data[j] = GetCell(col,rc);
        return a;
    }
    PyrrhoTable GetTable() throws IOException,DocumentException,DatabaseException
    {
        int n = GetInt();
        if (n==0)
            return null;
        String tn = GetString();
        PyrrhoTable dt = new PyrrhoTable();
        dt.schema = GetSchema(tn,n);
        int nr = GetInt();
        for (int j=0;j<nr;j++)
        {
            Versioned v = new Versioned();
            PyrrhoRow r = new PyrrhoRow(dt.schema,v);
            for (int i=0;i<n;i++)
                r.row[i] = GetCell(dt.schema.columns.get(i),v);
        }
        return dt;
    }
    java.util.Date GetDateTime() throws IOException
    {
        return new java.util.Date(GetLong()/10000);
    }
    TimeSpan GetTimeSpan() throws IOException
    {
        return new TimeSpan(GetLong());
    }
    Interval GetInterval() throws IOException {
        Interval iv = new Interval();
        iv.years = (int)GetLong();
        iv.months = (int)GetLong();
        iv.ticks = GetLong();
        return iv;
    }
    String[] GetStrings() throws IOException {
        int n = GetInt();
        String[] obs = new String[n];
        for (int j=0;j<n;j++)
            obs[j] = GetString();
        return obs;
    }
    SQLWarning getWarnings()
    {
        if (warnings.size()<=0) {
            return null;
        } else {
            return warnings.get(0);
        }
    }
    ResultSetMetaData GetSchema(String tn,int nc) throws IOException
    {
        ArrayList<Column> cols = new ArrayList<Column>();
        int nk = 0;
        for (int j=0;j<nc;j++)
        {
            String cn = GetString();
            for (int i=0;i<j;i++)
                if (cols.get(i).name.equals(cn))
                    cn = "Col"+j;
            String dn = GetString();
            Column col = new Column(this,cn,dn,GetInt());
            if (col.keyPos>=nk)
                nk = col.keyPos+1;
            cols.add(col);
        }
        int[] key = new int[nk];
        for (int j=0;j<nc;j++)
        {
            int k = cols.get(j).keyPos;
            if (k>=0)
                key[k] = j;
        }
        return new ResultSetMetaData(cols,key);
    }
    CellValue GetCell(Column col,Versioned rc) throws IOException,DocumentException,DatabaseException
    {
        CellValue r = new CellValue();
        r.subType = col.dataTypeName;
        int typ = col.dataType;
        int b = inp.read();
        if (b==3)
        {
            rc.version = GetString();
            b = inp.read();
        }
        if (b==4)
        {
            rc.readCheck = GetString();
            b = inp.read();
        }
        if (b==0)
            return r;
        if (b==2)
        {
            r.subType = GetString();
            typ = GetInt();
        }
        switch(typ)
        {
            case 0: return r;
            case 1: r.val = new Long(GetString()); break;
            case 2: r.val = new BigDecimal(GetString()); break;
            case 3: r.val = GetString(); break;
            case 4: r.val = GetDateTime(); break;
            case 5: { byte[]bb = GetBlob();
                if (r.subType.equals("DOCUMENT"))
                    r.val = new Document(bb,0,bb.length);
                else if (r.subType.equals("DOCARRAY"))
                    r.val = new DocArray(bb,0,bb.length);
                break; }
            case 6: r.val = GetRow("Table"); break;
            case 7: r.val = GetArray(rc); break;
            case 8: r.val = new Double(GetString()); break;
            case 9: r.val = GetInt()!=0; break;
            case 10: r.val = GetInterval(); break;
            case 11: r.val = GetTimeSpan(); break;
            case 12: r.val = GetRow(r.subType); break;
            case 13: r.val = new Date(GetDateTime()); break;
            case 14: r.val = GetTable(); break;
            default: throw new DatabaseException("2E204",new String[]{""+typ});
        }
        return r;
    }
    void GetInfo(DatabaseException e) throws IOException
    {
        while (inp.rpos<inp.rcount)
        {
            String k = GetString();
            String v = GetString();
            e.info.put(k,v);
        }
        e.info.put("CONDITION_NUMBER",e.sig);
        if (e.info.containsKey("REURNED_SQLSTATE"))
            e.info.put("RETURNED_SQLSTATE",e.sig);
        if (e.info.containsKey("MESSAGE_TEXT"))
        {
            String m = e.info.get("MESSAGE_TEXT");
            if (!e.info.containsKey("MESSAGE_LENGTH"))
                e.info.put("MESSAGE_LENGTH",""+m.length());
            if (!e.info.containsKey("MESSAGE_OCTECT_LENGTH"))
                e.info.put("MESSAGE_OCTET_LENGTH",
                        ""+m.getBytes(java.nio.charset.Charset.defaultCharset()).length);
        }
    }
    void Send(byte proto,String text) throws IOException
    {
        out.write(proto);
        PutString(text);
    }
    void Send(byte proto) throws IOException {
        out.write(proto);
    }
    int Receive() throws IOException, DatabaseException {
        int proto = inp.read();
        while (proto==68)
        {
            String sig = GetString();
            warnings.add(new SQLWarning(sig,GetStrings()));
            proto = inp.read();
        }
        DatabaseException e = null;
        switch(proto)
        {
            case -1: e = new DatabaseException("2E205"); break;
            case 12: e = new DatabaseException(GetString(),GetStrings()); break;
            case 16: e = new DatabaseException("2E206",GetStrings()); break;
            case 17: e = new DatabaseException(GetString()); break;
            case 19: e = new DatabaseException("0N002",GetStrings()); break;
            default: return proto;
        }
        GetInfo(e);
        inp.rcount = 0; inp.rpos=2;
        throw e;
    }
    private void PutString(String text) throws IOException
    {
        byte[] bytes = text.getBytes();
        int n = bytes.length;
        PutInt(n);
        out.write(bytes,0,n);
    }
    Thread Callbacks(String sigs) throws DatabaseException
    {
        throw new DatabaseException("2E206");
    }
    /*
    We assume the parameter is an Entity class (i.e. comes from
    a row in a base table or view)
    */
    public void Delete(Versioned ob) throws DatabaseException
    {
        try
        {
            Class tp = ob.getClass();
            Field[] fs = tp.getDeclaredFields();
            StringBuilder sb = new StringBuilder("delete from \"");
            sb.append(tp.getName());
            sb.append("\" where ");
            sb.append(tp.getName());
            sb.append(".check= '");
            sb.append(ob.version);
            String cmd = sb.toString();
            Send((byte)48);
            PutLong(SchemaKey(tp));
            PutString(cmd);
            int b = Receive();
            if (b!=11 || GetInt()==0)
                throw new DatabaseException("02000");
        }
        catch(DatabaseException e)
        {
            throw e;
        }
        catch(Exception e)
        {
            throw new DatabaseException("02000",new String[]{e.getMessage()});
        }
    }
    public Versioned[] FindAll(Class tp) throws DatabaseException
    {
        return Get(tp,"");
    }
    public Versioned[] FindWith(Class tp,String w) throws DatabaseException
    {
        return Get(tp,w);
    }
    public Versioned FindOne(Class tp,Object[] w) throws DatabaseException
    {
        StringBuilder sb = new StringBuilder();
        String comma = "";
        for (int i=0;i<w.length;i++)
        {
            sb.append(comma);
            comma = ",";
            AddString(sb,w[i]);
        }
        Versioned[] obs = Get(tp,sb.toString());
        if (obs==null || obs.length==0)
            return null;
        return obs[0];
    }
    Versioned[] Get(Class tp,String w) throws DatabaseException
    {
        try
        {
            Send((byte)33, "/"+tp.getName() +"/"+w);
            int p = Receive();
            if (p==13)
                return Get0(tp);
            return (Versioned[])Array.newInstance(tp, 0);
        }
        catch(DatabaseException e)
        {
            throw e;
        }
        catch(Exception e)
        {
            throw new DatabaseException("2E300",new String[]{tp.getName()});
        }
    }
    Versioned[] Get0(Class tp) throws DatabaseException
    {
        PyrrhoReader rdr = new PyrrhoReader(this,tp.getName());
        List<Versioned> list = new ArrayList();
        try
        {
            Constructor<?> ctor = tp.getConstructor(tp);
            while (rdr.next()) {
                Versioned ob = (Versioned)ctor.newInstance();
                for (int i=0;i<rdr.row.length;i++)
                {
                    String nm = rdr.schema.columns.get(i).name;
                    Field f = tp.getField(nm);
                    if (f==null)
                        throw new DatabaseException("2E302",new String[]{tp.getName(),nm});
                    if (f.getAnnotation(Exclude.class)!=null)
                        continue;
                    Object v = rdr.row[i].val;
                    if (v!=null && v!=DBNull.value)
                        f.set(ob, v);
                }
                if (ob instanceof Versioned)
                {
                    Versioned vo = (Versioned)ob;
                    vo.version = rdr.version.version;
                    vo.readCheck = rdr.version.readCheck;
                }
                list.add(ob);
            }
        } catch (DatabaseException e) {
            throw e;
        } catch (Exception e)
        {
            throw new DatabaseException("2E300",new String[]{tp.getName()});
        }
        finally {
            rdr.close();
        }
        return (Versioned[])list.toArray();
    }
    public void Put(Versioned ob) throws DatabaseException
    {
        Class tp = ob.getClass();
        Field[] fs = tp.getFields();
        StringBuilder sb = new StringBuilder("update \""+tp.getName()+"\"");
        StringBuilder sw = new StringBuilder();
        String comma = " set ";
        String where = " where ";
        try {
            for (int i = 0; i < fs.length; i++) {
                Field f = fs[i];
                if (f.getName().equals("check") || f.getName().equals("rowversion")) {
                    continue;
                }
                if (f.getAnnotation(Exclude.class) != null) {
                    continue;
                }
                Key ka = f.getAnnotation(Key.class);
                Object v = f.get(ob);
                if (ka != null) {
                    sw.append(where);
                    sw.append("\"" + f.getName() + "\"");
                    AddString(sw, v);
                    where = " and ";
                } else {
                    sb.append(comma);
                    sb.append("\"" + f.getName() + "\"");
                    AddString(sb, v);
                    comma = ", ";
                }
                if (ob.version != "") {
                    sw.append(where);
                    sw.append("check='");
                    sw.append(ob.version);
                    sw.append("'");
                }
            }
            Send((byte)46);
            PutLong(SchemaKey(tp));
            PutString(sb.toString()+sw.toString());
            int b = Receive();
            if (b==15)
                throw new DatabaseException("40001");
            if (b==13)
            {
                Fix(ob);
                b = Receive();
            }
            if (b!=11)
                throw new DatabaseException("2E203");
        }catch(Exception e)
        {
            throw new DatabaseException("2E203");
        }
    }
    public void Post(Versioned ob) throws DatabaseException
    {
        Class tp = ob.getClass();
        Field[] fs = tp.getFields();
        StringBuilder sb = new StringBuilder("insert into \""+tp.getName()+"\"");
        StringBuilder sc = new StringBuilder(") values ");
        List<Field> kf = new ArrayList<Field>();
        String comma = "(";
        try {
            for (int i = 0; i < fs.length; i++) {
                Field f = fs[i];
                if (f.getName().equals("check")) {
                    continue;
                }
                Object v = f.get(ob);
                if (v == null) {
                    continue;
                }
                if (f.getAnnotation(Exclude.class) != null) {
                    continue;
                }
                // ignore key fields for post
                if (f.getAnnotation(Key.class)!= null) {
                    continue;
                }
                sb.append(comma + "\"" + f.getName() + "\"");
                sc.append(comma);
                AddString(sc, v);
                comma = ",";
            }
            Send((byte)45);
            PutLong(SchemaKey(tp));
            PutString(sb.toString()+sc.toString()+")");
            posted.add(ob);
            int b = Receive();
            if (b==13)
            {
                Fix(ob);
                b = Receive();
            }
            if (b!=11)
                throw new DatabaseException("2E203");
        } catch(Exception e)
        {
            throw new DatabaseException("2E203",new String[]{tp.getName()});
        }
    }
    void AddString(StringBuilder sb, Object w)
    {
        if (w==null)
            sb.append("null");
        else if (w instanceof String)
        {
            String vs = (String) w;
            if (vs.length() > 0 && vs.charAt(0) != '\'') {
                sb.append("'");
                sb.append(vs.replace("'", "''"));
                sb.append("'");
            } else
                sb.append(vs);
        } else if (w instanceof byte[])
            hexits(sb,(byte[])w);
        else
            sb.append(w.toString());
    }
    void hexits(StringBuilder sb,byte[] v)
    {
        sb.append("X'");
        for (int i=0;i<v.length;i++)
            sb.append(Integer.toHexString(v[i]&0xFF));
        sb.append("'");
    }
    long SchemaKey(Class tp)
    {
        Schema ca = (Schema)tp.getAnnotation(Schema.class);
        if (ca!=null)
            return ca.key();
        return 0;
    }
    void Fix(Versioned ob)
    {
        try {
            int n = GetInt();
            if (n == 0) {
                return;
            }
            ResultSetMetaData sc = GetSchema(GetString(), n);
            int b = Receive(); // 14
            Class tp = ob.getClass();
            for (int i=0;i<n;i++)
            {
                Column c = sc.columns.get(i);
                Field f = tp.getDeclaredField(c.name);
                if (f==null)
                    throw new DatabaseException("2E302",new String[]{tp.getName(),c.name});
                f.set(ob,GetCell(c,ob).val);
            }
        } 
        catch(Exception e)
        {}  
    }
}
