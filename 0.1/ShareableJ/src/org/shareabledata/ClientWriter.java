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
public class ClientWriter extends SocketWriter {
    public ClientWriter(Socket c) { super(c); } 
    public void SendUids(SDict<Long, String> u) throws Exception
    {
        Write(Types.SNames);
        PutInt(u.Length);
        for (var b = u.First(); b != null; b = b.Next())
        {
            PutLong(b.getValue().key);
            PutString(b.getValue().val);
        }
    }
    public void SendUids(SSlot<String, Long> ... u) throws Exception
    {
        Write(Types.SNames);
        PutInt(u.length);
        for (var i = 0; i < u.length; i++)
        {
            PutString(u[i].key);
            PutLong(u[i].val);
        }
    }
}
