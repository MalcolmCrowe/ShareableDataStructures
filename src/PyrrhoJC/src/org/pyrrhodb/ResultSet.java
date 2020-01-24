/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pyrrhodb;
import java.util.*;
/**
 *
 * @author Malcolm
 */
public class ResultSet {
    public Statement stmt;
    boolean nullVal = false;
    ResultSet(Statement s)
    {
        stmt = s;
        Connection c = stmt.conn;
        try {
            int nc = c.GetInt();
            if (nc==0)
                return;
            String nm = c.GetString();
            c.rdr = new PyrrhoReader(s,nm,nc);
            
        } catch (Exception e)
        {}
    }
    public ResultSetMetaData getResultSetMetaData()
    {
        return stmt.conn.rdr.schema;
    }
    public int findColumn(String columnLabel)
    {
        List<Column> cols = stmt.conn.rdr.schema.columns;
        for (int i=0;i<cols.size();i++)
        {
            Column c = cols.get(i);
            if (c.name.equals(columnLabel))
                return i+1;
        }
        return 0;
    }
    public boolean next()
    {
        return stmt.conn.rdr.next();
    }
    public Object getObject(int columnIndex)
    {
        Object o = null;
        try {
            o = stmt.conn.rdr.row[columnIndex-1].val;
        } catch (Exception e) {}
        nullVal = o==null;
        return o;
    }
    public Object getObject(String columnLabel)
    {
        return getObject(findColumn(columnLabel));
    }
    public int getInt(int columnIndex)
    {
        try {
            return (int)((Long)getObject(columnIndex)).longValue();
        }
        catch(Exception e) {}
        return 0;
    }
    public int getInt(String columnLabel)
    {
        return getInt(findColumn(columnLabel));
    }
    public short getShort(int columnIndex)
    {
        try {
            return(Short)getObject(columnIndex);
        }
        catch(Exception e) {} 
        return 0;
    }
    public short getShort(String columnLabel)
    {
        return getShort(findColumn(columnLabel));
    }
    public Numeric getNumeric(int columnIndex)
    {
        try {
            return(Numeric)getObject(columnIndex);
        }
        catch(Exception e) {} 
        return Numeric.zero;
    }
    public Numeric getNumeric(String columnLabel)
    {
        return getNumeric(findColumn(columnLabel));
    }
    public double getDouble(int columnIndex)
    {
        try {
            return(Double)getObject(columnIndex);
        }
        catch(Exception e) {} 
        return 0;
    }
    public double getDouble(String columnLabel)
    {
        return getDouble(findColumn(columnLabel));
    }
    public float getFloat(int columnIndex)
    {
        try {
            return(Float)getObject(columnIndex);
        }
        catch(Exception e) {} 
        return 0;
    }
    public float getFloat(String columnLabel)
    {
        return getFloat(findColumn(columnLabel));
    }
    public String getString(int columnIndex)
    {
        try {
            return(String)getObject(columnIndex);
        }
        catch(Exception e) {} 
        return "";
    }
    public String getString(String columnLabel)
    {
        return getString(findColumn(columnLabel));
    }
    public Date getDate(int columnIndex)
    {
        try {
            return(Date)getObject(columnIndex);
        }
        catch(Exception e) {} 
        return null;
    }
    public Date getDate(String columnLabel)
    {
        return getDate(findColumn(columnLabel));
    }
    public Time getTime(int columnIndex)
    {
        try {
            return(Time)getObject(columnIndex);
        }
        catch(Exception e) {} 
        return null;
    }
    public Time getTime(String columnLabel)
    {
        return getTime(findColumn(columnLabel));
    }
    public Timestamp getTimestamp(int columnIndex)
    {
        try {
            return(Timestamp)getObject(columnIndex);
        }
        catch(Exception e) {} 
        return null;
    }
    public Timestamp getTimestamp(String columnLabel)
    {
        return getTimestamp(findColumn(columnLabel));
    }
    public boolean wasNull()
    {
        return nullVal;
    }
    public void close()
    {
        stmt.conn.rdr.close();
    }
}
