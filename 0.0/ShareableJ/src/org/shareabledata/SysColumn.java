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
public class SysColumn extends SColumn {
    SysColumn(String n,int t) {
        super(n,t,--SysTable._uid);
    }
}
