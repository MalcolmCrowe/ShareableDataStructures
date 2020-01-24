/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pyrrhodb;
import java.util.ArrayList;
/**
 *
 * @author 66668214
 */
public class PyrrhoReader {
    public PyrrhoReader(Statement s,String tn,int nc)
    {
        stmt = s;
        try {
            schema = s.conn.GetSchema(tn,nc); 
            row = new CellValue[nc];
        } catch (Exception e){}
    }
    PyrrhoReader(Connection c,String tn)
    {
        stmt = null;
        try{
            int nc = c.GetInt();
            c.GetString();
            schema = c.GetSchema(tn,nc); 
            row = new CellValue[nc];
        } catch (Exception e){}
    }
    public Statement stmt;
    boolean active=true,bOF = true;
    ResultSetMetaData schema;
    CellValue[] row = null; // current row
    Versioned version = null;
    CellValue[] cells = null; // obtained from a single round trip
    ArrayList<Versioned> versions = null;
    int off = 0;
    boolean getCell(int cx)
    {
        if (cells!=null)
        {
            row[cx] = cells[off++];
            if (off == cells.length)
                cells = null;
            return true;
        }
        try {
            stmt.conn.Send((byte)16);
            stmt.conn.out.flush();
            int p = stmt.conn.Receive();
            if (p!=10)
                return false;
            int n = stmt.conn.GetInt();
            if (n==0)
                return false;
            cells = new CellValue[n];
            off = 0;
            int j = cx;
            for (int i=0;i<n;i++)
            {
                Column col = schema.columns.get(j);
                Versioned rc = new Versioned();
                cells[i] = stmt.conn.GetCell(col,rc);
                if (++j==schema.columns.size())
                    j = 0;
            }
            row[cx] = cells[off++];
        } catch (Exception e)
        {
            return false;
        }
        return true;
    }
    boolean next()
    {
        for (int j=0;j<schema.columns.size();j++)
            if (!getCell(j))
                return false;
        return true;
    }
    void close()
    {
        if (active)
            try {
                stmt.conn.Send((byte)5);
            } catch(Exception e) {}
        active = false;
    }
}
