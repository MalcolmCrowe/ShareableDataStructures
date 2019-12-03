/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pyrrhodb;
import java.util.ArrayList;
/**
 *
 * @author 66668214
 */
public class DocArray {
    ArrayList<Object> items = new ArrayList<Object>();
    public DocArray() {}
    public DocArray(String s) throws DocumentException
    {
        if (s==null)
            return;
        s = s.trim();
        int n = s.length();
        if (n<=2 || s.charAt(0)!='[' || s.charAt(n-1)!='}')
            throw new DocumentException("[..] expected",0);
        items = new DocBase(s,1,n-1).getItems();
    }
    public DocArray(byte[] bytes,int pos,int end) throws DocumentException
    {
        Bson bs = new Bson(bytes,pos,end);
        while (bs.pos<end-1)
            items.add(bs.getValue());
    }
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("[");
        String comma = "";
        for (int i=0;i<items.size();i++)
        {
            sb.append(comma); comma = ",";
            sb.append('"');sb.append(i);sb.append('"');
            sb.append(": ");sb.append(items.get(i));
        }
        sb.append("]");
        return sb.toString();
    }
}
