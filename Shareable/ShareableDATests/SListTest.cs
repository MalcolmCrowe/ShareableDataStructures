using NUnit.Framework;
using Shareable;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShareableDATests
{
    [TestFixture]
    public class SListTest
    {
        private static TimeAndMemoryLogger tml;

        [OneTimeSetUp]
        public static void setUpClass()
        {
            tml = new TimeAndMemoryLogger();
        }

        [OneTimeTearDown]
        public static void tearDownClass()
        {

            try
            {
                tml.writeToCSV("LinkedListTestOutput_CSharp.csv");
            }
            catch (Exception e)
            {
                // TODO Auto-generated catch block
                Console.Write(e.StackTrace);
            }
        }

        [TearDown]
        public void tearDown()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        private void reusableSlistTestCase(String caseName, int numberOfElements)
        {
            tml.setTestCaseName(caseName);

            SList<Payload> list;
            Payload firstElement = new Payload("Load 0");
            tml.setInitialTimeAndMemory();
            //list = SList.New();


            /*tml.logTimeAndMemoryUsage(1);
        
            for (int i = 1; i<numberOfElements; i++)
            {
            
                list.UpdateAt(new Payload("Load " + i), 0);
                //list.InsertAt(new Payload("Load "+i), 0);

                tml.logTimeAndMemoryUsage(i+1);
            }*/

        }

        [Test]
        public void testInsert100Slist()
        {
            String caseID = "SList 100 insert";
            reusableSlistTestCase(caseID, 100);

        }

    }
}
