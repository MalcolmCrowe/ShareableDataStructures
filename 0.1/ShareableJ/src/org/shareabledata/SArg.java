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
public class SArg extends Serialisable {
        public final SDbObject target;
        public static SArg Value = new SArg();
        SArg()
        {
            super(Types.SArg); 
            target = SRole.Public; 
        }
        public SArg(ReaderBase f)
        {
            super(Types.SArg);
            target = f.context;
        }
        @Override
        public Serialisable Lookup(SDatabase tr,Context cx)
        {
            return cx.refs.get(target.uid);
        }
        @Override
        public Context Arg(Serialisable v,Context cx)
        {
            return Context.New(new SDict(target.uid,v),cx);
        }
        @Override
        public boolean isValue()
        {
            return false;
        }
        @Override
        public String toString()
        {
            return "VALUE";
        }
}
