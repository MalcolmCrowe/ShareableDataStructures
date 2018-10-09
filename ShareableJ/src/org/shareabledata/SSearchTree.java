/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 *
 * @author 66668214
 */
public class SSearchTree<T extends Comparable<T>> extends Shareable<T>
{
        public final T node;
        public final SSearchTree<T> left, right;
        SSearchTree(T n,SSearchTree<T> lf,SSearchTree<T> rg)
        {
            super(1+((lf==null)?0:lf.Length)+((rg==null)?0:rg.Length));
            node = n;
            left = lf;
            right = rg;
        }
        public SSearchTree(T ... els) throws Exception
        {
            super(els.length);
            if (els.length==0)
                throw new Exception("Bad parameter");
            node = els[0];
            SSearchTree<T> lf=null,rg=null;
            for (int i=1;i<els.length;i++)
               if (els[i].compareTo(node)<=0)
                   lf = (lf==null)?new SSearchTree<T>(els[i],null,null)
                           :lf.Add(els[i]);
               else
                   rg = (rg==null)?new SSearchTree<T>(els[i],null,null)
                           :rg.Add(els[i]);  
            left = lf;
            right = rg;
        }
        public SSearchTree<T> Add(T n)
        {
            if (n.compareTo(node) <= 0)
                return new SSearchTree<T>(node, 
                        (left==null)?new SSearchTree<T>(n,null,null):left.Add(n),
                        right);
            else
                return new SSearchTree<T>(node, left, 
                        (right==null)?new SSearchTree<T>(n,null,null):right.Add(n));
        }
        public boolean Contains(T n)
        {
            int c = n.compareTo(node);
            return (c == 0) ? true : (c < 0) ? 
                    ((left==null)?false:left.Contains(n)) : 
                    (right==null)?false:right.Contains(n);
        }
        public Bookmark<T> First()
        {
            return new SSearchTreeBookmark<T>(this,true,null,0);
        }
   
}
