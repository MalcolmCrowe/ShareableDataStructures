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
public class ServerStream  {
        Socket client;
        int rx = 0;
        ServerReader rdr;
        ServerWriter wtr;
        boolean exception = false;
        ServerStream(Socket c) throws Exception
        {
            super();
            client = c;
            rdr = new ServerReader(client);
            wtr = new ServerWriter(client);
            rdr.buf.pos = 2;
            rdr.buf.len = 0;
        }
        void StartException()
        {
            rdr.buf.pos = rdr.buf.len;
            wtr.buf.pos = 4;
            wtr.exception = true;
        }
        public void Flush() throws Exception
        {
            wtr.PutBuf();
        }
};
