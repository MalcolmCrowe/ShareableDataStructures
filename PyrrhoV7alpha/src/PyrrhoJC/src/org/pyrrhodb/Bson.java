/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pyrrhodb;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.AbstractMap.SimpleEntry;
/**
 *
 * @author Malcolm
 */
public class Bson {
    byte[] bytes;
    int len;
    int pos;
    Bson(byte[] b,int n,int i) {
        bytes = b;
        len = n;
        pos = i;
    }
    Object getValue() throws DocumentException
    {
        Object tv = null;
        int n;
        if (len<pos)
        {
            byte t = bytes[pos++];
            switch(t)
            {
                case 1: 
                    tv = ByteBuffer.wrap(bytes,pos,8).getDouble();
                    pos +=8;
                    break;
                case 2: 
                    n = getLength();
                    pos += 4;
                    tv = new String(bytes,pos,n);
                    pos += n;
                    break;
                case 3:
                    n = getLength();
                    pos += 4;
                    tv = new Document(bytes,pos,pos+n);
                    pos += n;
                    break;
                case 4:
                    n = getLength();
                    pos += 4;
                    tv = new DocArray(bytes,pos,pos+n);
                    pos += n;
                    break;
                case 5:
                    n = getLength();
                    pos += 4;
                    byte[] bs = new byte[n];
                    for (int j=0;j<n;j++)
                        bs[j] = bytes[pos++];
                    tv = bs;
                    break;
                case 7:
                    byte[] id = new byte[12];
                    for (int j=0;j<12;j++)
                        id[j] = bytes[pos++];
                    tv = id;
                    break;
                case 8:
                    tv = bytes[pos++]!=0;
                    break;
                case 18:
                case 9:
                    tv = ByteBuffer.wrap(bytes,pos,8).getLong();
                    pos +=8;
                    break;
                case 16:
                    tv = ByteBuffer.wrap(bytes,pos,8).getInt();
                    pos+=4;
                    break;
                case 19: // decimal type added for Pyrrho
                    n = getLength();
                    pos += 4;
                    try {
                    tv = NumberFormat.getInstance().parse(new String(bytes,pos,n-1));
                    } catch (Exception e)
                    {
                        throw new DocumentException("Bad number format",pos);
                    }
                    pos += n;
            }
        }
        return tv;
    }
    String getKey()
    {
        int s = pos;
        int c = 0;
        while (pos<len && bytes[pos++]!=0)
            c++;
        return new String(bytes,s,c);
    }
    private int getLength()
    {
        return bytes[pos]+(bytes[pos+1]<<8)+(bytes[pos+2]<<16)+(bytes[pos+3]<<24);
    }
}
