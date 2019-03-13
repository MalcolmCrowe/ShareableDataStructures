using System;
using System.Collections;
using System.Collections.Generic;
using BTree;
using Shareable;

namespace IterTest
{
    internal class BTree1<K, V> : BTree<K, V>, IEnumerable<Entry<K, V>> where K : IComparable<K>
    {
        public BTree1(int degree):base(degree) {}

        public IEnumerator<Entry<K, V>> GetEnumerator()
        {
            return (Root==null)?null:new NodeEnumerator<K,V>(Root);
        }
        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }

    }
    internal class NodeEnumerator<K,V> :IEnumerator<Entry<K,V>> where K:IComparable<K>
    {
        NodeEnumerator<K, V> inner = null;
        Node<K, V> node;
        int pos;
        public NodeEnumerator(Node<K, V> t)
        {
            node = t;
            Reset();
        }

        public Entry<K, V> Current => inner?.Current ?? node.Entries[pos];

        object IEnumerator.Current => throw new NotImplementedException();

        public void Dispose()
        {
        }

        public bool MoveNext()
        {
            if (node.IsLeaf)
                return ++pos < node.Entries.Count;
            for (; ; )
            {
                if (inner != null)
                {
                    if (inner.MoveNext())
                        return true;
                    inner = null;
                    return ++pos < node.Entries.Count;
                }
                if (pos <node.Children.Count)
                    inner = new NodeEnumerator<K, V>(node.Children[pos+1]);
                else
                    return false;
            }
        }

        public void Reset()
        {
            pos = -1;
            if (!node.IsLeaf)
                inner = new NodeEnumerator<K, V>(node.Children[0]);
        }
    }
    class Program
    {
        const int N = 100000;
        static void Main(string[] args)
        {
            var rnd = new Random(0);
            for (int k = 0; k < 10; k++)
            {
                var ss = new List<string>();
                var sd = SDict<string, int>.Empty;
                while (ss.Count < N)
                {
                    var s = AString(rnd);
                    if (!sd.Contains(s))
                    {
                        sd = sd.Add(s, sd.Count);
                        ss.Add(s);
                    }
                }
                long ct = 0;
                // Castro
                var bt = new BTree1<string, int>(4);
                for (int i = 0; i < N; i++)
                    bt.Insert(ss[i], i);
                var t = DateTime.Now;
                var sa = new List<string>();
                foreach (var e in bt)
                {
                    //                   Console.WriteLine(e.Key);
                    sa.Add(e.Key);
                }
                ct = (DateTime.Now - t).Ticks;
                // Crowe
                var sb = SDict<string, int>.Empty;
                for (int i = 0; i < N; i++)
                    sb = sb.Add(ss[i], i);
                t = DateTime.Now;
                var sc = new List<string>();
                for (var b = sb.First(); b != null; b = b.Next())
                    sc.Add(b.Value.key);
                var st = (DateTime.Now - t).Ticks;
                Console.WriteLine(ct + " " + st + ((st != 0 && ct == 0) ? " Infinity" : " " + (st * 100.0 / ct)));
                if (sa.Count != sc.Count)
                    Console.WriteLine("Lists differ in length");
                for (var i = 0; i < sb.Count; i++)
                    if (sa[i].CompareTo(sc[i]) != 0)
                        Console.WriteLine("Error");
            }
            Console.WriteLine("Done");
            Console.ReadLine();
        }
        static string AString(Random rnd)
        {
            var n = rnd.Next() % 100+1;
            var r = new char[n];
            for (var i = 0; i < n; i++)
                r[i] = (char)('A' + (rnd.Next() % 26));
            return new string(r);
        }
    }
}
