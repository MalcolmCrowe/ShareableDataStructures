using NUnit.Framework;
using Shareable;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Immutable;

namespace ShareableDATests
{
    [TestFixture]
    public class InmitableListTest
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
                tml.writeToCSV("ImutableListTestOutput_CSharp.csv");
            }
            catch (Exception e)
            {
                // TODO Auto-generated catch block
                Console.Write(e.StackTrace);
                throw e;
            }
        }

        [TearDown]
        public void tearDown()
        {
            callTheGC();
        }

        private void callTheGC()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        private void reusableSlistTestCase(String caseName, int numberOfElements)
        {
            tml.setTestCaseName(caseName);

            
            var listBuilder = ImmutableList.Create<Payload>();
            Payload firstElement = new Payload("Load 0");
            tml.setInitialTimeAndMemory();
            listBuilder.Add(firstElement);


            tml.logTimeAndMemoryUsage(1);

            for (int i = 1; i < numberOfElements; i++)
            {
                
                listBuilder.Add(new Payload("Load " + i);
                
                if (i< numberOfElements)
                    tml.logTimeAndMemoryUsage(i + 1);
            }
            ImmutableList<Payload> outputList = listBuilder.ToImmutableList<Payload>();
            tml.logTimeAndMemoryUsage(numberOfElements);
            
        }

        [Test]
        public void testInsert100Immutablelist()
        {
            String caseID = "Immutable List 100 insert";
            reusableSlistTestCase(caseID, 100);

        }

        [Test]
        public void testInsert1000Immutablelist()
        {
            String caseID = "Immutable 1000 insert";
            reusableSlistTestCase(caseID, 1000);

        }

        [Test]
        public void testInsert10000Immutablelist()
        {
            String caseID = "Immutable 10000 insert";
            reusableSlistTestCase(caseID, 10000);

        }


    }
}
