/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

import java.net.Socket;

/**
 *
 * @author Malcolm
 */
public abstract class SocketWriter extends WriterBase {
    protected Socket client;
    public SocketWriter(Socket c)
    {
        client = c;
        buf.pos = 2;
        buf.len = 0;
    }
    @Override
    public void PutBuf() throws Exception
    {
        buf.pos -= 2;
        buf.buf[0] = (byte)(buf.pos >> 7);
        buf.buf[1] = (byte)(buf.pos & 0x7f);
        client.getOutputStream().write(buf.buf);
        buf.pos = 2;
    }
    @Override
    public void WriteByte(byte value) throws Exception
    {
        if (buf.pos >= Buffer.Size)
            PutBuf();
        buf.buf[buf.pos++] = value;
    }
}
