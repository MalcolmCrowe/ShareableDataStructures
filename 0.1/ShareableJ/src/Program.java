
import org.shareabledata.*;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author 66668214
 */
public class Program {

    public static void main(String[] args) throws Exception {
            // Tests for SList (unordered list)
            SList<String> sl = new SList<String>("Red", "Blue", "Green");
            sl = sl.InsertAt("Yellow", 0);
            SList<String> s2 = sl;
            sl = sl.RemoveAt(3);
            sl = sl.UpdateAt("Pink", 1);
            sl = sl.InsertAt("Orange", 2);
            String[] aa = sl.ToArray(String.class);
            Check(aa, new String[]{"Yellow", "Pink", "Orange", "Blue"});
            Check(s2.ToArray(String.class), "Yellow", "Red", "Blue", "Green");
            System.out.println("SList done");
            // Tests for SArray
            SArray<String> sa = new SArray<String>("Red", "Blue", "Green");
            sa = sa.InsertAt(0,"Yellow");
            sa = sa.RemoveAt(3);
            SArray<String> sb = sa;
            sa = sa.InsertAt(2,"Orange", "Violet");
            Check(sa.ToArray(String.class), "Yellow", "Red", "Orange", "Violet", "Blue");
            Check(sb.ToArray(String.class), "Yellow", "Red", "Blue");
            System.out.println("SArray done");
            // Tests for SSearchTree<string>
            SSearchTree<String> ss = new SSearchTree<String>("InfraRed", "Red", "Orange", "Yellow", "Green", "Blue", "Violet");
            Check(ss.ToArray(String.class), "Blue", "Green", "InfraRed", "Orange", "Red", "Violet","Yellow");
            SSearchTree<Integer> si = new SSearchTree<Integer>(56, 22, 24, 31, 23);
            Check(si.ToArray(Integer.class), 22, 23, 24, 31, 56);
            System.out.println("SSearchTree done");
            System.in.read();
    }

    static <T extends Comparable<T>> void Check(T[] a, T... b) {
        if (a.length != b.length) {
            System.out.println("wrong length");
        }
        for (int i = 0; i < a.length; i++) {
            if (a[i].compareTo(b[i]) != 0) {
                System.out.println("wrong value");
            }
        }
    }

    static void Check(String a, String b) {
        if (a != b) {
            System.out.println("wrong value");
        }
    }
}
