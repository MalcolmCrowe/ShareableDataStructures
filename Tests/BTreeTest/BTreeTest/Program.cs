using BTree;
using Shareable;
using System;
namespace BTreeTest
{
    class Program
    {
        static int[] N = new int[] { 1000, 10000, 100000 };
        static void Main(string[] args)
        {
            var seeds = new Random();
            for (int j = 0; j<N.Length; j++)
            {
                var rnd = new Random(seeds.Next());
                for (int k = 0; k < 4; k++)
                {
                    var ss = new string[N[j]];
                    var ps = new int[N[j]];
                    for (int i = 0; i < N[j]; i++)
                        ss[i] = AString(rnd);
                    long ct = 0;
                    // Castro
                    var t = DateTime.Now;
                    var bt = new BTree<string, int>(8);
                    for (int i = 0; i < N[j]; i++)
                    {
                        if (bt.Search(ss[i]) is Entry<string, int> e)
                            ps[i] = e.Pointer;
                        else
                        {
                            bt.Insert(ss[i], i);
                            ps[i] = i;
                        }
                    }
                    for (int i = 0; i < N[j]; i++)
                        if (bt.Search(ss[i]).Pointer != ps[i])
                            Console.WriteLine("Error 1");
         //           for (int i = 0; i < N[j]; i++)
         //               bt.Delete(ss[i]);
                    ct = (DateTime.Now - t).Ticks;
                    // Crowe
                    t = DateTime.Now;
                    var sb = SDict<string, int>.Empty;
                    for (int i = 0; i < N[j]; i++)
                    {
                        if (sb.Contains(ss[i]))
                        {
                            if (ps[i] != sb.Lookup(ss[i]))
                                Console.WriteLine("Mismatch");
                        }
                        else
                        {
                            sb = sb.Add(ss[i], i);
                            ps[i] = i;
                        }
                    }
                    for (int i = 0; i < N[j]; i++)
                        if (sb.Lookup(ss[i]) != ps[i])
                            Console.WriteLine("Error 2");
          //          for (int i = 0; i < N[j]; i++)
          //              sb = sb.Remove(ss[i]);
                    var st = (DateTime.Now - t).Ticks;
                    Console.WriteLine(ct + " " + st + " " + (st * 100.0 / ct));
                }
            }
            Console.ReadLine();
        }
        static string AString(Random rnd)
        {
            var n = rnd.Next() % 100;
            var r = new char[n];
            for (var i = 0; i < n; i++)
                r[i] = (char)('A' + (rnd.Next() % 26));
            return new string(r);
        }
    }
}
