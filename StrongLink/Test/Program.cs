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
         //       conn.CreateTable("A", new SColumn("B", Types.SInteger),
           //         new SColumn("C", Types.SInteger), new SColumn("D", Types.SString));
         //       conn.CreateIndex("A", IndexType.Primary, null, "B", "C");
           //     conn.Insert("A", null, new Serialisable[] { new SInteger(2), new SInteger(3), new SString("TwentyThree") },
             //       new Serialisable[] { new SInteger(1), new SInteger(9), new SString("Nineteen") });
                Console.WriteLine(conn.Get("A"));
            } catch(Exception e)
            {
                Console.WriteLine(e.Message);
            }
            Console.ReadLine();
        }
    }
}
