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
public class Protocol {
        public static final byte EoF = -1, Get = 1, Begin = 2, Commit = 3, Rollback = 4,
        Table = 5, Alter = 6, Drop = 7, Index = 8, Insert = 9,
        Read = 10, Update = 11, Delete = 12, View = 13;
}
