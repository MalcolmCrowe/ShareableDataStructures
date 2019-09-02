/*
 * PyrrhoInputStream.java
 *
 * Created on 10 December 2006, 19:04
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

import java.io.*;

/**
 *
 * @author Malcolm
 */
public class PyrrhoInputStream extends InputStream {
    InputStream str;
    byte[] rbuf = new byte[2048];
    int rx = 0, rcount = 0, rpos = 2;
    /** Creates a new instance of PyrrhoInputStream */
    PyrrhoInputStream(InputStream s)  {
        str = s;
    }
    public int read() throws IOException {
        if (rpos<rcount+2)
            return ((int)rbuf[rpos++])&0xff;
        rpos = 2;
        rcount = 0;
        rx = 0;
        int n = str.read(rbuf,0,2048);
        if (n <= 0)
            return n;
        rcount = (((int)rbuf[0])<<7) + ((int)rbuf[1]);
        if (rcount<=0)
            return -1;
        if (rcount==2047)
            return getException(12);
        return ((int)rbuf[rpos++])&0xff;
    }
    int getException(int proto)
    {
        if (proto==12)
        {
            rcount = (((int)rbuf[rpos++])<<7) + (((int)rbuf[rpos++])&255);
            rcount += 2;
            proto = (int)rbuf[rpos++];
        }
        return proto;
    }
}
