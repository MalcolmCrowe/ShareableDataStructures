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
public abstract class RowSet  extends Collection<Serialisable> {
        public final SQuery _qry;
        public final STransaction _tr;
        public final SDict<Long,SFunction> _aggregates;
        public RowSet(STransaction d, SQuery q, SDict<Long,SFunction>ags)
        {
            super(0);
            _tr = d; _qry = q; _aggregates = ags;
        }
}
