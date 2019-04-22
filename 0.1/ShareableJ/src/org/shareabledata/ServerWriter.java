/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

import java.net.Socket;

/**
 * This class is not shareable
 * @author Malcolm
 */
public class ServerWriter extends SocketWriter {
    public boolean exception = false;
    public ServerWriter(Socket c)  
    {  
        super(c); 
    }
    @Override
    public void PutBuf() throws Exception
    {
        if (buf.pos == 2)
            return;
        // now always send bSize bytes (not wcount)
        if (!exception) {// version 2.0
            super.PutBuf();
            return;
        }
            exception = false;
            buf.buf[0] = (byte)((Buffer.Size - 1) >> 7);
            buf.buf[1] = (byte)((Buffer.Size - 1) & 0x7f);
            buf.pos -= 4;
            buf.buf[2] = (byte)(buf.pos >> 7);
            buf.buf[3] = (byte)(buf.pos & 0x7f);
        try
        {
            client.getOutputStream().write(buf.buf);
        }
        catch (Exception e)
        {
            System.out.println("Socket Exception reported on Flush");
        }
    }    
}
