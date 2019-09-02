/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pyrrhodb;
import java.lang.Exception;
import java.util.ArrayList;
/**
 *
 * @author Malcolm
 */
public class SQLException extends Exception {
    String sig;
    SQLException(String m,String s)
    {
        super(m);
        sig = s;
    }
    public String getSQLState()
    {
        return sig;
    }
    public String getErrorCode()
    {
        return sig;
    }
    public SQLException getNextException()
    {
        return null;
    }
}
