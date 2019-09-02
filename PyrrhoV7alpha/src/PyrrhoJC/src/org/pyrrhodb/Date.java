/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pyrrhodb;
import java.text.DateFormat;
/**
 *
 * @author Malcolm
 */
public class Date {
    public java.util.Date date;
    public Date(java.util.Date d)
    {
        date = d;
    }
    public String toString()
    {
        return DateFormat.getDateInstance(DateFormat.SHORT).format(date);
    }
}
