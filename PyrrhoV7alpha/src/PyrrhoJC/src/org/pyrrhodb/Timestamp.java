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
public class Timestamp {
    java.sql.Timestamp ts;
    public Timestamp(long t)
    {
        ts = new java.sql.Timestamp(t);
    }
    
}
