/*
 * PyrrhoDate.java
 *
 * Created on 28 November 2006, 09:00
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author crow-ci0
 */
public class PyrrhoDate {
    Date date;
    /** Creates a new instance of PyrrhoDate */
    public PyrrhoDate(Date d) {
        date = d;
    }
    public String toString()
    {
        return new SimpleDateFormat().format(date);
    }
}
