/*
 * PyrrhoOutputStream.java
 *
 * Created on 10 December 2006, 19:12
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
public class PyrrhoOutputStream extends OutputStream {
    OutputStream str;
    int wx = 0, wcount = 2;
    byte[] wbuf = new byte[2048];
    /** Creates a new instance of PyrrhoOutputStream
     * @param s */
    public PyrrhoOutputStream(OutputStream s) {
        str = s;
    }
    @Override
    public void write(int b) throws IOException {
        if (wcount<2048)
            wbuf[wcount++] = (byte)b;
        if (wcount>=2048)
            writeBuf();
    }
    void writeBuf() throws IOException{
        wcount -= 2;
        wbuf[0] = (byte)(wcount>>7);
        wbuf[1] = (byte)(wcount&0x7f);
        str.write(wbuf);
        wcount = 2;
    }
    @Override
    public void flush() throws IOException
    {
        if (wcount>2)
            writeBuf();
    }
}
