/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

import java.util.ArrayList;

/**
 * 
 * @author Malcolm
 */
public abstract class ReaderBase extends IOBase {
    public Context ctx = Context.Empty;
    public SDbObject context = SRole.Public; // set a function or object being defined
    public long lastAlias = SDbObject.maxAlias;
    public Serialisable req;
    public SDatabase db;   // a copy, updatable during Get, Load
    public long Position()
    { return buf.start + buf.pos; }
    public Bigint GetInteger() throws Exception
    {
        var n = ReadByte();
        var cs = new byte[n];
        for (int j = 0; j < n; j++)
            cs[j] = (byte)ReadByte();
        return new Bigint(cs);
    }
    public static long pe13;
    public int GetInt() throws Exception
    {
        pe13 = Position();
        return GetInteger().toInt();
    }
    public long GetLong() throws Exception
    {
        return GetInteger().toLong();
    }
    public String GetString() throws Exception
    {
        int n = GetInt();
        byte[] cs = new byte[n];
        for (int j = 0; j < n; j++)
        cs[j] = (byte)ReadByte();
        return new String(cs, 0, n, "UTF-8");
    }
    public Serialisable _Get() throws Exception
    {
        int tp = ReadByte();
        if (tp<0)
            return null;
        Serialisable s;
        switch (tp)
        {
            case Types.Serialisable: s = Serialisable.Get(this); break;
//               case Types.STimestamp: s = STimestamp.Get(this); break;
            case Types.SBigInt:
            case Types.SInteger: s = SInteger.Get(this); break;
            case Types.SNumeric: s = SNumeric.Get(this); break;
            case Types.SString: s = SString.Get(this); break;
            case Types.SDate: s = SDate.Get(this); break;
            case Types.STimeSpan: s = STimeSpan.Get(this); break;
            case Types.SBoolean: s = SBoolean.Get(this); break;
            case Types.STable: s = STable.Get(this); break;
            case Types.SRow: s = SRow.Get(this); break;
            case Types.SColumn: s = SColumn.Get(this); break;
            case Types.SRecord: s = SRecord.Get(this); break;
            case Types.SUpdate: s = SUpdate.Get(this); break;
            case Types.SDelete: s = SDelete.Get(this); break;
            case Types.SAlter: s = SAlter.Get(this); break;
            case Types.SDrop: s = SDrop.Get(this); break;
            case Types.SIndex: s = SIndex.Get(this); break;
            case Types.SCreateTable: s = SCreateTable.Get(this); break;
            case Types.SUpdateSearch: s = SUpdateSearch.Get(this); break;
            case Types.SDeleteSearch: s = SDeleteSearch.Get(this); break;
            case Types.SSearch: s = SSearch.Get(this); break;
            case Types.SSelect: s = SSelectStatement.Get(this); break;
            case Types.SValues: s = SValues.Get(this); break;
            case Types.SExpression: s = SExpression.Get(this); break;
            case Types.SFunction: s = SFunction.Get(this); break;
            case Types.SOrder: s = SOrder.Get(this); break;
            case Types.SInPredicate: s = SInPredicate.Get(this); break;
            case Types.SAlias: s = SAlias.Get(this); break;
            case Types.SSelector: s = SSelector.Get(this); break;
            case Types.SGroupQuery: s = SGroupQuery.Get(this); break;
            case Types.STableExp: s = SJoin.Get(this); break;
            case Types.SName: s = SDbObject.Get(this); break;
            case Types.SArg: s = new SArg(this); break;
            case Types.SDropIndex: s = new SDropIndex(this); break;
            default: s = Serialisable.Null; break;
        }
        return s;
    }
    public STable GetTable() throws Exception
    {
        var tb = new STable(Position() - 1);
        var nm = GetString();
        db = db.Install(tb, nm, Position()); // will have moved on
        return tb;
    }
    /// <summary>
    /// Called from Transaction.Commit()
    /// </summary>
    /// <param name="d"></param>
    /// <param name="pos"></param>
    /// <param name="max"></param>
    /// <returns></returns>
    public SDbObject[] GetAll(long max) throws Exception
    {
        var r = new ArrayList();
        while (Position() < max)
            r.add((SDbObject)_Get());
        return (SDbObject[])r.toArray(new SDbObject[0]);
    }
    public Serialisable Lookup(long pos)
    {
        return db.objects.get(pos);
    }
}

