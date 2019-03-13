/*
 * This Tree implementationis based on Java: How to Program, 8th Edition 8th edition 
 * by Harvey M. Deitel, Paul J. Deitel (2009) Paperback
 * Amazon Link: https://amzn.to/2Sne1ff
 * We have modified it to support Generics to enable the
 * comparison to SharableTree Structure
 */
package org.shareabledata.test.common;

/**
 *
 * @author 77800577
 */
public class DietelJavaTree<T extends Comparable<T> > {
    private DietelTreeNode<T> root;

   // constructor initializes an empty Tree of integers
   public DietelJavaTree() 
   { 
      root = null; 
   } // end Tree no-argument constructor

   public DietelTreeNode<T> getRoot(){
       return root;
   }
   // insert a new node in the binary search tree
   public void insertNode( T insertValue )
   {
      if ( root == null )
         root = new DietelTreeNode( insertValue ); // create the root node here
      else
         root.insert( insertValue ); // call the insert method
   } // end method insertNode

   // begin preorder traversal
   public boolean contains (T value)
   { 
      return containsHelper( root , value); 
   } 

   // recursive method to perform preorder traversal
   private boolean containsHelper( DietelTreeNode node, T value )
   {
      if ( node == null ){
         return false;
      }
      else if (node.data.equals(value)){
          return true;
      }
      else{
          return containsHelper( node.leftNode, value )
                  || containsHelper( node.rightNode, value ); 
      }

     
   } 

   // begin inorder traversal
   public void inorderTraversal()
   { 
      inorderHelper( root ); 
   } // end method inorderTraversal

   // recursive method to perform inorder traversal
   private void inorderHelper( DietelTreeNode node )
   {
      if ( node == null )
         return;

      inorderHelper( node.leftNode );        // traverse left subtree
      System.out.printf( "%d ", node.data ); // output node data
      inorderHelper( node.rightNode );       // traverse right subtree
   } // end method inorderHelper

   // begin postorder traversal
   public void postorderTraversal()
   { 
      postorderHelper( root ); 
   } // end method postorderTraversal

   // recursive method to perform postorder traversal
   private void postorderHelper( DietelTreeNode node )
   {
      if ( node == null )
         return;
  
      postorderHelper( node.leftNode );      // traverse left subtree
      postorderHelper( node.rightNode );     // traverse right subtree
      System.out.printf( "%d ", node.data ); // output node data
   } // end method postorderHelper
   
   
}
  