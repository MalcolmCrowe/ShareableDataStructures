/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 *
 * @author Malcolm
 */
public class SocketReader extends Reader {
        public SocketReader(StreamBase f) throws Exception
        {
            super(f);
            pos = 2;
        }
        @Override
        public int ReadByte()
        {
            if (pos >= buf.len)
            {
                if (!buf.fs.GetBuf(buf))
                    return -1;
                pos = 2;
            }
            return (buf.len == 0) ? -1 : buf.buf[pos++];
        }  
}
