/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pyrrhodb;
import java.util.ArrayList;
import java.util.AbstractMap.SimpleEntry;
/**
 *
 * @author Malcolm
 */
public class Document {
    public ArrayList<SimpleEntry<String,Object>> fields
            = new ArrayList<SimpleEntry<String,Object>>();
    public Document(){}
    public Document(String s) throws DocumentException
    {
        if (s==null)
            return;
        s = s.trim();
        fields = new DocBase(s,0,s.length()).getFields();
    }
    public Document(byte[] bytes,int pos, int len) throws DocumentException
    {
        Bson bs = new Bson(bytes,pos,len);
        while (bs.pos<len-1)
        {
            String key = bs.getKey();
            fields.add(new SimpleEntry<String,Object>(key,bs.getValue()));
        }
    }
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("[");
        String comma = "";
        for (int i=0;i<fields.size();i++)
        {
            SimpleEntry e = fields.get(i);
            sb.append(comma); comma = ",";
            sb.append('"');sb.append(e.getKey());sb.append('"');
            sb.append(": ");sb.append(e.getValue());
        }
        sb.append("]");
        return sb.toString();
    }
}
