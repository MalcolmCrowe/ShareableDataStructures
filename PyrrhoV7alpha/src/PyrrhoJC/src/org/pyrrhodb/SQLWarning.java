/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pyrrhodb;

/**
 *
 * @author 66668214
 */
public class SQLWarning {
    String sig;
    String[] obs;
    SQLWarning(String s,String[] o)
    {
        sig = s;
        obs = o;
    }
    public String getSQLState()
    {
        return sig;
    }
    public SQLWarning getNextWarning()
    {
        return null;
    }
}
