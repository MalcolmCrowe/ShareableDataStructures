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
public class Procedure {
    public String name;
    public ArrayList<Parameter> parameters = new ArrayList<Parameter>();
    public DataType returns = null;
    Procedure(String n,DataType r)
    {
        name = n;
        returns = r;
    }
}
