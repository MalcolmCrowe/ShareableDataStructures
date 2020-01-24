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
public class CallableStatement extends PreparedStatement {
    CallableStatement (Connection conn, String sql)
    {
        super(conn,sql);
    }
    ArrayList<Integer> outputs = new ArrayList<Integer>();
    public void registerOutParameter(int parameterIndex,int sqlType)
    {
        outputs.add(parameterIndex-1,sqlType);
    }
}
