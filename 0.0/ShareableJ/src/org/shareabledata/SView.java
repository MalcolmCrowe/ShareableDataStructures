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
public class SView extends SDbObject {
        public final String name;
        public final SList<SColumn> cols;
        public final String viewdef;
        public SView(STransaction tr,String n,SList<SColumn> c,String d) 
        {
            super(Types.SView,tr);
            name = n; cols = c; viewdef = d;
        }
        SView(SView v,SList<SColumn>c)
        {
            super(v);
            cols = c; name = v.name; viewdef = v.viewdef;
        }
        SView(SDatabase d, Reader f) throws Exception
        {
            super(Types.SView,f);
            name = f.GetString();
            var n = f.GetInt();
            SList<SColumn> c = null;
            for (var i = 0; i < n; i++)
            {
                var nm = f.GetString();
                var sc = new SColumn(null, nm, f.ReadByte(), 0);
                if (c==null)
                    c = new SList<SColumn>(sc);
                else
                    c = c.InsertAt(sc,i);
            }
            cols = c;
            viewdef = f.GetString();
        }
        public SView(STransaction tr,SView v,AStream f)
        {
            super(v,f);
            name = v.name;
            cols = v.cols;
            viewdef = v.viewdef;
            f.PutString(name);
            f.PutInt(cols.Length);
            for (var b=cols.First();b!=null;b=b.Next())
            {
                f.PutString(b.getValue().name);
                f.WriteByte((byte)b.getValue().type);
            }
            f.PutString(viewdef);
        }
        public static SView Get(SDatabase d, Reader f) throws Exception
        {
            return new SView(d, f);
        }
        public boolean Conflicts(Serialisable that)
        {
            switch (that.type)
            {
                case Types.SView:
                    {
                        var v = (SView)that;
                        return name.compareTo(v.name) == 0;
                    }
            }
            return false;
        }
}
