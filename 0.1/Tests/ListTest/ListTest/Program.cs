using System;
using System.Collections.Generic;
using Shareable;

namespace ListTest
{
    /// <summary>
    /// Test List and SList using InsertionSort: neither is optimised for this
    /// </summary>
    class Program
    {
        static int[] N = new int[] { 100, 1000 };
        static void Main(string[] args)
        {
            var seeds = new Random();
            for (int j = 0; j < N.Length; j++)
            {
                var rnd = new Random(seeds.Next());
                for (int k = 0; k < 10; k++)
                {
                    var ss = new string[N[j]];
                    for (int i = 0; i < N[j]; i++)
                        ss[i] = AString(rnd);
                    long ct = 0;
                    var t = DateTime.Now;
                    var bt = new List<string>();
                    for (int i = 0; i < N[j]; i++)
                    {
                        var done = false;
                        for (int m = 0; m < bt.Count; m++)
                            if (bt[m].CompareTo(ss[i]) < 0)
                            {
                                bt.Insert(m, ss[i]);
                                done = true;
                                break;
                            }
                        if (!done)
                            bt.Add(ss[i]);
                    }
                    if (bt.Count != N[j])
                        Console.WriteLine("Error 3");
                    for (int i = 0; i < N[j]; i++)
                    {
                        var n = 0;
                        foreach (var s in bt)
                            if (s.CompareTo(ss[i])==0)
                                break;
                            else
                                n++;
                        bt.RemoveAt(n);
                     }
                    if (bt.Count != 0)
                        Console.WriteLine("Error 1");
                    ct = (DateTime.Now - t).Ticks;
                    // Crowe
                    t = DateTime.Now;
                    var sb = SList<string>.Empty;
                    for (int i = 0; i < N[j]; i++)
                    {
                        var n = 0;
                        var done = false;
                        for (var b = sb.First(); b != null; b = b.Next(), n++)
                            if (b.Value.CompareTo(ss[i]) < 0)
                            {
                                sb = sb.InsertAt(ss[i], n);
                                done = true;
                                break;
                            }
                        if (!done)
                            sb = sb.InsertAt(ss[i], sb.Length);
                    }
                    if (sb.Length != N[j])
                        Console.WriteLine("Error 4");
                    for (int i = 0; i < N[j]; i++)
                    {
                        var n = 0;
                        for (var b = sb.First(); b != null; b = b.Next(), n++)
                            if (b.Value.CompareTo(ss[i]) == 0)
                            {
                                sb = sb.RemoveAt(n);
                                break;
                            }
                    }
                    if (sb.Length != 0)
                        Console.WriteLine("Error 2");
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