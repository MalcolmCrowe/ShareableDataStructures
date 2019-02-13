using NUnit.Framework;
using System;
using System.Collections.Immutable;

namespace ShareableDATests
{
    [TestFixture]
    public class ImmutableTreeTest
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
                tml.writeToCSV("STreeTestOutput_CSharp.csv");
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

            var treeBuilder = ImmutableSortedDictionary.Create<Payload,Payload>();
            Payload firstElement = new Payload("Load 0");
            tml.setInitialTimeAndMemory();
            treeBuilder = treeBuilder.Add(firstElement, firstElement);



            for (int i = 1; i < numberOfElements; i++)
            {
                Payload objecttoAdd = new Payload("Load " + i);
                treeBuilder = treeBuilder.Add(objecttoAdd, objecttoAdd);
                if (i < numberOfElements)
                    tml.logTimeAndMemoryUsage(i + 1);
                
            }

            ImmutableSortedDictionary<Payload, Payload > tree = treeBuilder.ToImmutableSortedDictionary<Payload, Payload>();
            tml.logTimeAndMemoryUsage(numberOfElements);
        }

        [Test]
        public void testInsert100()
        {
            String caseID = "ImmutableTree 100 insert";
            reusableSTreeTestCase(caseID, 100);
        }

        [Test]
        public void testInsert1000()
        {
            String caseID = "ImmutableTree 1000 insert";
            reusableSTreeTestCase(caseID, 1000);
        }

        [Test]
        public void testInsert10000()
        {
            String caseID = "ImmutableTree 10000 insert";
            reusableSTreeTestCase(caseID, 1000);
        }

        [Test]
        public void FindElementIn100()
        {
            tml.setTestCaseName("Find elements in 100");

            ImmutableSortedDictionary<Payload, Payload> tree = this.creatASListWithNelement(100);
            Payload toBeFound_25 = new Payload("Load 25");
            Payload toBeFound_50 = new Payload("Load 50");
            Payload toBeFound_75 = new Payload("Load 75");

            Payload toBeFound_100 = new Payload("Load 99");
            Assert.AreEqual(100, tree.Count);
            tml.setInitialTimeAndMemory();
            ImmutableSortedDictionary<Payload, Payload> removedList = tree.Remove(toBeFound_25);//removes element at 25
            Assert.AreEqual(99, removedList.Count);
            tml.logTimeAndMemoryUsage(25);
            removedList = tree.Remove(toBeFound_50);
            Assert.AreEqual(99, removedList.Count);
            tml.logTimeAndMemoryUsage(50);
            removedList = tree.Remove(toBeFound_75);
            Assert.AreEqual(99, removedList.Count);
            tml.logTimeAndMemoryUsage(75);
            removedList = tree.Remove(toBeFound_100);
            Assert.AreEqual(99, removedList.Count);
            tml.logTimeAndMemoryUsage(100);
        }

        private ImmutableSortedDictionary<Payload, Payload> creatASListWithNelement(int numberOfElements)
        {
            

            var treeBuilder = ImmutableSortedDictionary.Create<Payload, Payload>();
            Payload firstElement = new Payload("Load 0");
            treeBuilder = treeBuilder.Add(firstElement, firstElement);



            for (int i = 1; i < numberOfElements; i++)
            {
                Payload objecttoAdd = new Payload("Load " + i);
                treeBuilder = treeBuilder.Add(objecttoAdd, objecttoAdd);
                

            }

            return treeBuilder.ToImmutableSortedDictionary<Payload, Payload>();
            
        }

        [Test]
        public void DeepCopyAndFindIn100()
        {
            tml.setTestCaseName("DeepCopyAndFindIn100");


            ImmutableSortedDictionary<Payload, Payload> tree = null;
            ImmutableSortedDictionary<Payload, Payload> treeCopy = null;

            tml.setInitialTimeAndMemory();
            tree = this.creatASListWithNelement(100);
            treeCopy = tree;
            Payload payloadtoFind = new Payload("Load 25");
            bool ans = treeCopy.ContainsValue(payloadtoFind);
                

            tml.logTimeAndMemoryUsage(25);
            tree = null;
            treeCopy = null;
            this.callTheGC();
            ///////////
            tml.setInitialTimeAndMemory();
            tree = this.creatASListWithNelement(100);

            
            treeCopy = tree;
            payloadtoFind = new Payload("Load 50");
            ans = treeCopy.ContainsValue(payloadtoFind);
            tml.logTimeAndMemoryUsage(50);
            tree = null;
            treeCopy = null;
            this.callTheGC();
            ///////////////
            tml.setInitialTimeAndMemory();
            tree = this.creatASListWithNelement(100);
            
            
            treeCopy = tree;
            payloadtoFind = new Payload("Load 75");
            ans = treeCopy.ContainsValue(payloadtoFind);
            tml.logTimeAndMemoryUsage(75);
            tree = null;
            treeCopy = null;
            this.callTheGC();
            //////////////
            tml.setInitialTimeAndMemory();
            tree = this.creatASListWithNelement(100);
            
            treeCopy = tree;
            payloadtoFind = new Payload("Load 99");
            ans = treeCopy.ContainsValue(payloadtoFind);

            tml.logTimeAndMemoryUsage(100);


            




        }
    }
}
