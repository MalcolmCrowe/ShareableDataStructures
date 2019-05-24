using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shareable
{
    class Program
    {
        static SDict<string, bool> dict = SDict<string, bool>.Empty;
        static void Main(string[] args)
        {
            Console.WriteLine("A List of words");
            Console.WriteLine("Operations are Add, Clear, Exit, Remove, Tree");
            for(; ;)
            {
                Console.Write("A/C/E/R/T: ");
                var op = Console.ReadLine();
                if (op.StartsWith("A"))
                {
                    Console.Write("Word: ");
                    var wd = Console.ReadLine();
                    dict += (wd, true);
                    Show();
                }
                else if (op.StartsWith("R"))
                {
                    Console.Write("Word: ");
                    var wd = Console.ReadLine();
                    dict -= wd;
                    Show();
                }
                else if (op.StartsWith("T"))
                    Tree();
                else if (op.StartsWith("E"))
                    return;
                else if (op.StartsWith("C"))
                    dict = SDict<string, bool>.Empty;
                else
                    continue;
            }
        }
        static void Show()
        {
            for (var b = dict.First(); b != null; b = b.Next())
            {
                Console.Write(b.Value.Item1); Console.Write(" ");
            }
            Console.WriteLine();
        }
        static void Tree()
        {
            dict.root.Show(0);
        }
    }
}
