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
public class PreparedStatement extends Statement {
    String template;
    ArrayList<String> parameters = new ArrayList<String>();
    PreparedStatement(Connection conn,String s)
    {
        super(conn);
        template = s;
    }
    void safeSet(int ix,String s)
    {
        if (ix<parameters.size())
            parameters.add(ix,s);
        else
        {
            while (ix>parameters.size())
                parameters.add("?");
            parameters.add(s);
        }
    }
    public void setInt(int parameterIndex,int x)
    {
        safeSet(parameterIndex-1,""+x);
    }
    public void clearParameters()
    {
        parameters.clear();
    }
    String eval()
    {
        StringBuilder sb = new StringBuilder();
        int tlen = template.length();
        int tpos = 0;
        int ppos = 0;
        while (tpos<tlen)
        {
            char ch = template.charAt(tpos++);
            if (ch == '?' && ppos<parameters.size())
                sb.append(parameters.get(ppos++));
            else
                sb.append(ch);
        }
        return sb.toString();
    }
    public ResultSet executeQuery() throws Exception
    {
        return executeQuery(eval());
    }
    public int executeUpdate() throws Exception
    {
        return executeUpdate(eval());
    }
    public void setQueryTimeout(int t)
    {
        
    }
}
