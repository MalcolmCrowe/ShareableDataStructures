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
public class ParsePair {
    public final Serialisable ob;
    public final SDict<Long,String> ns;
    public ParsePair(Serialisable s,SDict<Long,String> n) {ob = s; ns=n; }
}
