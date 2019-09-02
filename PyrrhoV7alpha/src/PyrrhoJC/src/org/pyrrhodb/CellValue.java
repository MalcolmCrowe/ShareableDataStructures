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
public class CellValue {
    String subType = null;
    Object val = null;
    @Override
    public String toString()
    {
        if (val!=null)
            return val.toString();
        return "";
    }
}
