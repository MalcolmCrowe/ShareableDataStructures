/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 * This class is not shareable
 * 
 * @author Malcolm
 */
public abstract class IOBase {
    public Buffer buf = new Buffer();
    protected IOBase() {}
    protected boolean GetBuf(long s) throws Exception
    {
        throw new Exception("Not implemented");
    }
    protected void PutBuf() throws Exception
    {
        throw new Exception("Not implemented");
    }
    public int ReadByte() throws Exception
    {
        throw new Exception("Not implemented");
    }
    public void WriteByte(byte value) throws Exception
    {
        throw new Exception("Not implemented");
    }
}
