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
public class AliasRowSet extends RowSet {
        public final SAlias _alias;
        public final RowSet _sce;
        public AliasRowSet(SAlias a,RowSet sce)
        {
            super(sce._tr,a,Context.New(new SDict(a.alias,a.qry),sce._cx));
            _alias = a;
            _sce = sce;
        }
        static Context _Context(AliasRowSet ars,Context cx)
        {
            var a = ars._alias;
            return Context.New(cx.refs,
                Context.Replace(new SDict(a.alias, a.qry),cx));
        }
        @Override
        public Bookmark<Serialisable> First()
        {
                var b = _sce.First();
                if (b!=null)
                    return new AliasRowBookmark(this,(RowBookmark)b,0);
                return null;
        }
        class AliasRowBookmark extends RowBookmark
        {
            public final AliasRowSet _ars;
            public final RowBookmark _bmk;
            AliasRowBookmark(AliasRowSet ars,RowBookmark bmk,int p)
            {
                super(ars,_Context(ars,bmk._cx),p);
                _ars = ars;
                _bmk = bmk;
            }
            public Bookmark<Serialisable> Next()
            {
                var b = _bmk.Next();
                if (b!=null)
                    return new AliasRowBookmark(_ars, (RowBookmark)b, Position+1);
                return null;
            }
        }

}
