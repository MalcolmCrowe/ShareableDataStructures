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
public class DietelTreeNode<T extends Comparable<T>> {
    // package access members
   DietelTreeNode<T> leftNode; // left node  
   T data; // node value
   DietelTreeNode<T> rightNode; // right node

   // constructor initializes data and makes this a leaf node
   public DietelTreeNode( T nodeData )
   { 
      data = nodeData;              
      leftNode = rightNode = null; // node has no children
   } // end TreeNode no-argument constructor

   public T getData(){
        return data;
   }
   
   public DietelTreeNode<T> getLeftNode(){
       return leftNode;
   }
   
   public DietelTreeNode<T> getRightNode(){
       return rightNode;
   }
   
   // locate insertion point and insert new node; ignore duplicate values
   public void insert( T insertValue )
   {
      // insert in left subtree
      if ( insertValue.compareTo(data) < 0 ) 
      {
         // insert new TreeNode
         if ( leftNode == null )
            leftNode = new DietelTreeNode( insertValue );
         else // continue traversing left subtree
            leftNode.insert( insertValue ); 
      } // end if
      else if ( insertValue.compareTo(data) >= 0 ) // insert in right subtree
      {
         // insert new TreeNode
         if ( rightNode == null )
            rightNode = new DietelTreeNode( insertValue );
         else // continue traversing right subtree
            rightNode.insert( insertValue ); 
      } // end else if
   } // end method insert
}
