/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 *
 * @author Malcolm
 */
public class SDbObject extends Serialisable {
    /// <summary>
    /// For database objects such as STable, we will want to record 
    /// a unique id based on the actual position in the transaction log,
    /// so the Get and Commit methods will capture the appropriate 
    /// file positions in AStream – this is why the Commit method 
    /// needs to create a new instance of the Serialisable. 
    /// The uid will initially belong to the Transaction. 
    /// Once committed the uid will become the position in the AStream file.
    /// </summary>

    public final long uid;
    public static final long maxAlias = -1000000;
    /// <summary>
    /// We will allow clients to define SColumns etc, with an impossible uid
    /// </summary>
    /// <param name="t"></param>

    protected SDbObject(int t) {
        super(t);
        uid = -1;
    }
    /// <summary>
    /// For system tables and columns, with negative uids
    /// </summary>
    /// <param name="t"></param>
    /// <param name="u"></param>

    public SDbObject(int t, long u) {
        super(t);
        uid = u;
    }

    /// <summary>
    /// For a new database object we set the transaction-based uid
    /// </summary>
    /// <param name="t"></param>
    /// <param name="tr"></param>
    protected SDbObject(int t, STransaction tr) {
        super(t);
        uid = tr.uid + 1;
    }
    /// <summary>
    /// A modified database obejct will keep its uid
    /// </summary>
    /// <param name="s"></param>

    protected SDbObject(SDbObject s) {
        super(s.type);
        uid = s.uid;
    }
    /// <summary>
    /// A database object got from the file will have
    /// its uid given by the position it is read from
    /// </summary>
    /// <param name="t"></param>
    /// <param name="f"></param>

    protected SDbObject(int t, Reader f) throws Exception {
        super(t);
        if (t == Types.SName)
           uid = f.GetLong();
        else  // a new object is being defined
           if (f instanceof SocketReader)
           {
               var u = f.GetLong();

               uid = ((STransaction)f.db).uid + 1;
               if (u != -1) // keep track of the client-side name
                   f.db = f.db.Add(uid, f.db.role.uids.get(u));
           }
           else // file position is uid
               uid = f.getPosition() - 1;
    }

    protected SDbObject(SDbObject s, AStream f) {
        super(s.type);
        if (s.uid < STransaction._uid) {
            throw new Error("Internal error - misplaced database object");
        }
        uid = f.pos();
        f.uids = f.uids.Add(s.uid, uid);
        f.WriteByte((byte) s.type);
    }
    @Override
    public boolean isValue() { return false;}
    public long getAffects() { return uid; }
    public static Serialisable Get(Reader f) throws Exception
    {
        return new SDbObject(Types.SName,f);
    }
    @Override
    public Serialisable UseAliases(SDatabase db,SDict<Long,Long> ta)
    {
        return (ta.Contains(uid)) ?
            new SDbObject(Types.SName, ta.get(uid)) : this;
    }
    @Override
    public Serialisable UpdateAliases(SDict<Long,String> uids)
    {
        return (uids.Contains(uid-1000000))?
            new SDbObject(Types.SName,uid-1000000):this;
    }
    public static Ident Prepare(Ident n,SDict<Long,Long>pt)
            throws Exception
    {
        if (n.uid>=1 || n.uid < maxAlias)
            return n;
        if (pt!=null && !pt.Contains(n.uid))
            throw new Exception("Could not find " + n.id);
        return new Ident(pt.get(n.uid),n.id);
    }
    public static long Prepare(long uid,SDict<Long,Long> pt)
            throws Exception
    {
        if (uid>=1 || uid < maxAlias)
            return uid;
        if (pt!=null && !pt.Contains(uid))
            throw new Exception("Could not find " + _Uid(uid));
        return pt.get(uid);
    }
    public static SSlot<Long,Long> Prepare(SSlot<Long,Long> s,
            STransaction tr,SDict<Long,Long> pt) throws Exception
    {
        return new SSlot(Prepare(s.key,pt),Prepare(s.val,pt));
    }
    @Override
    public Serialisable Prepare(STransaction tr,SDict<Long,Long> pt) throws Exception
    {
        if (uid<maxAlias || uid >= 0)
            return this;
        if (pt!=null && !pt.Contains(uid))
            throw new Exception("Could not find " + tr.Name(uid));
        return tr.objects.get(pt.get(uid));
    }
    @Override
    public void Put(StreamBase f)
    {
        super.Put(f);
        f.PutLong(uid);
    }
    void Check(Boolean committed) {
        if (committed != uid < STransaction._uid) {
            throw new Error("Internal error - Commited check fails");
        }
    }

    String Uid() {
        return _Uid(uid);
    }
    static String _Uid(long uid)
    {
        if (uid > STransaction._uid)
            return "'" + (uid - STransaction._uid);
        if (uid <0 && uid > -1000000)
            return "#" + (-uid);
        if (uid < -1000000 & uid > -0x70000000)
            return "$" + (-1000000-uid);
        if (uid <= -0x70000000)
            return "@" + (0x70000000 + uid);
        return "" + uid;
    }

    @Override
    public String toString() {
        return "SName" + _Uid(uid);
    }
}
