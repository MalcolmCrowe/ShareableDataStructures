/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata.test.common;

/**
 *
 * @author 77800577
 */
public class TreeDeepCopy {
    public static DietelJavaTree<PayLoad> deepCopy(DietelJavaTree<PayLoad> originalTree) {
        DietelJavaTree<PayLoad> copiedtree = new DietelJavaTree<PayLoad>();
        
        deepCopyHelper(copiedtree, originalTree.getRoot());
        
       
        return copiedtree;
    }

    private static void deepCopyHelper(DietelJavaTree<PayLoad> copiedtree, DietelTreeNode<PayLoad> originalTree) {
        if (originalTree != null){
            copiedtree.insertNode(new PayLoad(originalTree.getData().getPayload()));
            deepCopyHelper(copiedtree, originalTree.getLeftNode());
            deepCopyHelper(copiedtree, originalTree.getRightNode());
        }
    }
}
