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
            super(sce._tr,a,sce._aggregates);
            _alias = a;
            _sce = sce;
        }
        static Context _Context(AliasRowSet ars,Context cx)
        {
            var a = ars._alias.alias;
            var u = ars._tr.objects.get(a).uid;
            return Context.New(cx.refs,
                Context.New(new SDict(a, cx.get(u)),cx.next,ars._tr),ars._tr);
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
