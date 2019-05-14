/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

import java.io.IOException;

/**
 * This class is not shareable
 * @author Malcolm
 */
public class Buffer {

    public static final int Size = 1024;
    public byte[] buf;
    public long start;
    public int len;
    public int pos;
    public Buffer() {
        buf = new byte[Size];
        pos = 0;
    }

    Buffer(long s, int n){
        buf = new byte[Size];
        start = s;
        len = n;
        pos = 0;
    }
}
