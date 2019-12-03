/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pyrrhodb;
import java.lang.Exception;
/**
 *
 * @author Malcolm
 */
public class Statement {
    public Connection conn;
    private ResultSet resultSet = null;
    private int updateCount = -1;
    Statement(Connection c)
    {
        conn = c;
    }
    public void setCursorName(String name)
    {}
    public ResultSet executeQuery(String sql) throws Exception
    {
        conn.AcquireTransaction();
        conn.Send((byte)21,sql);
        conn.out.flush();
        int reply = conn.Receive();
        if (reply==13)
                resultSet = new ResultSet(this);
        return resultSet;
    }
    /*
    If there is no current resultSet this method may construct one,
    (e.g. for insert/POST). Test for this and close it before opening
    another. For safety, always use either execduteUpdate or executeQuery
    */
    public boolean execute(String sql) throws Exception
    {
        conn.AcquireTransaction();
        conn.Send((byte)55,sql);
        conn.out.flush();

        int reply = conn.Receive();
        if (reply==13)
            resultSet = new ResultSet(this);
        updateCount = conn.GetInt();
        return reply==13;
    }
    public int executeUpdate(String sql) throws Exception
    {
        conn.AcquireTransaction();
        conn.Send((byte)2,sql);
        conn.out.flush();
        int p = conn.Receive();
        if (p!=11)
            throw new DatabaseException("2E203");
        updateCount = conn.GetInt();
        return updateCount;
    }
    public int getUpdateCount()
    {
        return updateCount;
    }
    public ResultSet getResultSet()
    {
        return resultSet;
    }
    public SQLWarning getWarnings()
    {
        if (conn.warnings.size()==0)
            return null;
        return conn.warnings.remove(0); 
    }
    public void cancel()
    {
    }
    public void close()
    {
        if (conn.rdr!=null)
            conn.rdr.close();
    }
}
