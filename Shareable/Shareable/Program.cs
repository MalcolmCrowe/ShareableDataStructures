using System;

/// <summary>
/// See "Shareable Data Structures" (c) Malcolm Crowe, University of the West of Scotland 2018
/// http://shareabledata.org 
/// This is free-to-use software
/// </summary>
namespace Shareable
{
    class Program
    {
        static void Main(string[] args)
        {
            // Tests for SList (unordered list)
            var sl = SList<string>.New("Red", "Blue", "Green");
            sl = sl.InsertAt("Yellow", 0);
            var s2 = sl;
            sl = sl.RemoveAt(3);
            sl = sl.UpdateAt("Pink", 1);
            sl = sl.InsertAt("Orange", 2);
            Check<string>(sl.ToArray(), "Yellow", "Pink", "Orange", "Blue");
            Check<string>(s2.ToArray(), "Yellow", "Red", "Blue", "Green");
            Console.WriteLine("SList done");
            // Tests for SArray
            var sa = new SArray<string>("Red", "Blue", "Green");
            sa = sa.InsertAt(0,"Yellow");
            sa = sa.RemoveAt(3);
            var sb = sa;
            sa = sa.InsertAt(2,"Orange", "Violet");
            Check(sa.ToArray(), "Yellow", "Red", "Orange", "Violet", "Blue");
            Check(sb.ToArray(), "Yellow", "Red", "Blue");
            Console.WriteLine("SArray done");
            // Tests for SSearchTree<string>
            var ss = SSearchTree<string>.New("InfraRed", "Red", "Orange", "Yellow", "Green", "Blue", "Violet");
            Check(ss.ToArray(), "Blue", "Green", "InfraRed", "Orange", "Red", "Violet","Yellow");
            var si = SSearchTree<int>.New(56, 22, 24, 31, 23);
            Check(si.ToArray(), 22, 23, 24, 31, 56);
            Console.WriteLine("SSearchTree done");
            // Tests for SDict
            var sd = SDict<string, string>.Empty;
            sd = sd.Add("Y", "Yellow");
            sd = sd.Add("G", "Green");
            sd = sd.Add("B","Blue");
            sd = sd.Remove("G");
            for (var b = sd.First(); b != null; b = b.Next())
                Console.WriteLine(b.Value.key + ": " + b.Value.val);
            Console.WriteLine("SDict done");
            Console.ReadKey();
        }
        static void Check<T>(T[] a,params T[] b) where T:System.IComparable
        {
            if (a.Length != b.Length)
                Console.WriteLine("wrong length");
            for (var i = 0; i < a.Length; i++)
                if (a[i].CompareTo(b[i])!=0)
                    Console.WriteLine("wrong value");
        }
        static void Check(string a, string b)
        {
            if (a!=b)
                Console.WriteLine("wrong value");
        }
    }
}
