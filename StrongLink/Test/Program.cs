using System;
using StrongLink;
using Shareable;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                var conn = new StrongConnect("127.0.0.1", 50433, "test");
                Console.Write("Rebuilding (Y/N)?");
                if (Console.ReadLine().StartsWith("Y"))
                {
                    conn.CreateTable("A",
                        new SColumn("B", Types.SInteger, -1), 
                        // uid parameter can be -1 if no ambiguity: otherwise use other negative numbers
                        new SColumn("C", Types.SInteger, -1),
                        new SColumn("D", Types.SString, -1));
                    conn.CreateIndex("A", IndexType.Primary, null, "B", "C");
                    conn.Insert("A", null, new Serialisable[] { new SInteger(2), new SInteger(3), new SString("TwentyThree") },
                      new Serialisable[] { new SInteger(1), new SInteger(9), new SString("Nineteen") });
                }
                Console.WriteLine("OK: Table contents are:");
                Console.WriteLine(conn.Get(new STable("A",-1)));
                Console.WriteLine("OK: Log contains:");
                Console.WriteLine(conn.Get(new STable("_Log",-1)));
            } catch(Exception e)
            {
                Console.WriteLine("Exception: "+e.Message);
            }
            Console.ReadLine();
        }
    }
}
