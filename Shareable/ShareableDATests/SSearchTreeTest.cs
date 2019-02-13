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
    public class SSearchTreeTest
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
                tml.writeToCSV("ShearableTreeTestOutput_CSharp.csv");
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

        private void reusableSTreeTestCase(String caseName, int numberOfElements)
        {
            tml.setTestCaseName(caseName);

            tml.setInitialTimeAndMemory();
            SSearchTree<Payload> tree = SSearchTree<Payload>.New(new Payload("Load 0"));


            for (int i = 1; i < numberOfElements; i++)
            {
                tree += new Payload("Load " + i);
                tml.logTimeAndMemoryUsage(i + 1);
            }
        }

        [Test]
        public void testInsert100()
        {
            String caseID = "SSearchTreeTest 100 insert";
            reusableSTreeTestCase(caseID, 100);
        }

        [Test]
        public void testInsert1000()
        {
            String caseID = "SSearchTreeTest 1000 insert";
            reusableSTreeTestCase(caseID, 1000);
        }

        [Test]
        public void testInsert10000()
        {
            String caseID = "SSearchTreeTest 10000 insert";
            reusableSTreeTestCase(caseID, 10000);
        }

        [Test]
        public void FindElementIn100()
        {
            tml.setTestCaseName("Find elements in 100");
        
            SSearchTree<Payload> tree = createTreeOfSize(100);

            Payload toBeFound_25 = new Payload("Load 25");
            Payload toBeFound_50 = new Payload("Load 50");
            Payload toBeFound_75 = new Payload("Load 75");
            Payload toBeFound_100 = new Payload("Load 99");

            tml.setInitialTimeAndMemory();
            tree.Contains(toBeFound_25);
            tml.logTimeAndMemoryUsage(25);
            tree.Contains(toBeFound_50);
            tml.logTimeAndMemoryUsage(50);
            tree.Contains(toBeFound_75);
            tml.logTimeAndMemoryUsage(75);
            tree.Contains(toBeFound_100);
            tml.logTimeAndMemoryUsage(100);

        }

        private SSearchTree<Payload> createTreeOfSize(int numberOfElements)
        {
            SSearchTree<Payload> tree = SSearchTree<Payload>.New(new Payload("Load 0"));


            for (int i = 1; i < numberOfElements; i++)
            {
                tree += new Payload("Load " + i);
                
            }

            return tree;
        }

        [Test]
        public void deepCopyAndAddTest() {
            tml.setTestCaseName("DeepCopyAndFindIn100");
            

            tml.setInitialTimeAndMemory();
            SSearchTree<Payload> tree = createTreeOfSize(100);
            SSearchTree<Payload> treeCpy = tree;
            treeCpy.Contains(new Payload("Load 25"));
            tml.logTimeAndMemoryUsage(25);
            tree = null;
            treeCpy = null;
            this.callTheGC();
            ///////////
            tml.setInitialTimeAndMemory();
            tree = createTreeOfSize(100);
            
            
            treeCpy = tree;
            
            treeCpy.Contains(new Payload("Load 50"));
            tml.logTimeAndMemoryUsage(50);
            tree = null;
            treeCpy = null;
            this.callTheGC();
            
            ///////////////
            tml.setInitialTimeAndMemory();
            tree = createTreeOfSize(100);
                      
            treeCpy = tree;
            treeCpy.Contains(new Payload("Load 75"));
            tml.logTimeAndMemoryUsage(75);
            tree = null;
            treeCpy = null;
            this.callTheGC();
            //////////////
            ///////////////
            tml.setInitialTimeAndMemory();
            tree = createTreeOfSize(100);
            
            
            treeCpy = tree;
            treeCpy.Contains(new Payload("Load 100"));
            tml.logTimeAndMemoryUsage(100);
            tree = null;
            treeCpy = null;
            this.callTheGC();
            //////////////


        }
    }

}

