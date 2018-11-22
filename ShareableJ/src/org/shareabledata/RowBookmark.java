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
    public abstract class RowBookmark extends Bookmark<Serialisable>
    {
        public final RowSet _rs;
        public final Serialisable _ob;
        protected RowBookmark(RowSet rs, Serialisable ob, int p)
        {
            super(p);
            _rs = rs; _ob = ob;
        }
        @Override
        public Serialisable getValue() { return  _ob; }
    }
