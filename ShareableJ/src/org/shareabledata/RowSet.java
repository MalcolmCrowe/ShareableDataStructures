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
public abstract class RowSet  extends Shareable<Serialisable> {
        public final SQuery _qry;
        public final SDatabase _db;
        public RowSet(SDatabase d, SQuery q)
        {
            super(0);
            _db = d; _qry = q;
        }
}
