/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.net.*;
/**
 *
 * @author Malcolm
 */
public class ClientStream {
        static long _cid = 0;
        long cid = ++_cid;
        Socket client;
        ClientReader rdr;
        ClientWriter wtr;
        int rx = 0;
        ClientStream(StrongConnect pc, Socket c) throws Exception
        {
            client = c;
            rdr = new ClientReader(client);
            wtr = new ClientWriter(client);
            rdr.buf.pos = 2;
            rdr.buf.len = 0;
        }
        public ClientTriple Receive() throws Exception
        {
            if (wtr.buf.pos > 2)
                wtr.PutBuf();
            rdr.buf.pos = 2;
            rdr.buf.len = 0;
            long ts = 0,te = 0;
            var t = (byte)rdr.ReadByte();
            if (t==Types.Done)
            {
                ts = rdr.GetLong();
                te = rdr.GetLong();
            }
            return new ClientTriple(t,ts,te);
        }
        public void Flush() throws Exception
        {
            rdr.buf.pos = 2;
            rdr.buf.len = 0;
            try
            {
                wtr.PutBuf();
                wtr.buf.pos = 2;
            }
            catch (SocketException e)
            {
                System.out.println("Flush reports exception " + e.getMessage());
                throw e;
            }
        }
}
