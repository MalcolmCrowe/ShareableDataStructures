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
public class ClientTriple {
    public final int proto;
    public final long start;
    public final long end;
    public ClientTriple(int p,long s,long e)
    {
        proto = p; start = s; end = e;
    }
}
