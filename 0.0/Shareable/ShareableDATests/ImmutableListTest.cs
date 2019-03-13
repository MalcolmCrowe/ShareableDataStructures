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
    public class ImmutableListTest
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
            listBuilder = listBuilder.Add(firstElement);


            tml.logTimeAndMemoryUsage(1);

            for (int i = 1; i < numberOfElements; i++)
            {

                listBuilder = listBuilder.Add(new Payload("Load " + i));
                
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


        private ImmutableList<Payload> creatASListWithNelement(int numberOfElements)
        {
            var listBuilder = ImmutableList.Create<Payload>();
            Payload firstElement = new Payload("Load 0");
            tml.setInitialTimeAndMemory();
            listBuilder = listBuilder.Add(firstElement);
            bool salida = listBuilder.Contains(firstElement);
            Assert.True(salida);
            for (int i = 1; i < numberOfElements; i++)
            {
                listBuilder = listBuilder.Add( new Payload("Load " + i));
            }

            ImmutableList<Payload> list = listBuilder.ToImmutableList<Payload>();
            salida = list.Contains(new Payload("Load 25"));
            Assert.True(salida);
            return list;
        }


        [Test]
        public void RemoveElementIn100()
        {
            tml.setTestCaseName("Remove elements in 100");

            ImmutableList<Payload> list = this.creatASListWithNelement(100);
            Payload toBeFound_25 = new Payload("Load 25");
            Payload toBeFound_50 = new Payload("Load 50");
            Payload toBeFound_75 = new Payload("Load 75");

            Payload toBeFound_100 = new Payload("Load 99");
            Assert.AreEqual(100, list.Count);
            tml.setInitialTimeAndMemory();
            ImmutableList<Payload> removedList = list.Remove(toBeFound_25);//removes element at 25
            Assert.AreEqual(99, removedList.Count);
            tml.logTimeAndMemoryUsage(25);
            removedList = list.Remove(toBeFound_50);
            Assert.AreEqual(99, removedList.Count);
            tml.logTimeAndMemoryUsage(50);
            removedList = list.Remove(toBeFound_75);
            Assert.AreEqual(99, removedList.Count);
            tml.logTimeAndMemoryUsage(75);
            removedList = list.Remove(toBeFound_100);
            Assert.AreEqual(99, removedList.Count);
            tml.logTimeAndMemoryUsage(100);
        }

        [Test]
        public void FindElementIn100()
        {
            tml.setTestCaseName("Find elements in 100");

            ImmutableList<Payload> list = this.creatASListWithNelement(100);
            Payload toBeFound_25 = new Payload("Load 25");
            Payload toBeFound_50 = new Payload("Load 50");
            Payload toBeFound_75 = new Payload("Load 75");

            Payload toBeFound_100 = new Payload("Load 99");
           // Assert.AreEqual(100, list.Count);
            tml.setInitialTimeAndMemory();
            bool outcome = list.Contains(toBeFound_25);
            Assert.IsTrue(outcome, toBeFound_25 + " not found");
            tml.logTimeAndMemoryUsage(25);
            outcome = list.Contains(toBeFound_50);
            Assert.IsTrue(outcome, toBeFound_50 + " not found");
            tml.logTimeAndMemoryUsage(50);
            outcome = list.Contains(toBeFound_75);
            Assert.IsTrue(outcome, toBeFound_75 + " not found");
            tml.logTimeAndMemoryUsage(75);
            outcome = list.Contains(toBeFound_100);
            Assert.IsTrue(outcome, toBeFound_100 + " not found");
            tml.logTimeAndMemoryUsage(100);
        }


        [Test]
        public void DeepCopyAndAddIn100()
        {
            tml.setTestCaseName("DeepCopyAndAddIn100");

            ImmutableList<Payload> list = this.creatASListWithNelement(100);
            ImmutableList<Payload> modifiedList = null;

            tml.setInitialTimeAndMemory();
            ImmutableList<Payload> listCpy = list;
            modifiedList = list.Insert(24, new Payload("Load 25*"));

            tml.logTimeAndMemoryUsage(25);
            ///////////
            list = this.creatASListWithNelement(100);

            tml.setInitialTimeAndMemory();
            listCpy = list;
            modifiedList = list.Insert(24, new Payload("Load 50*"));
            tml.logTimeAndMemoryUsage(50);
            ///////////////
            list = this.creatASListWithNelement(100);
            this.callTheGC();
            tml.setInitialTimeAndMemory();
            listCpy = list;
            modifiedList = list.Insert(24, new Payload("Load 75*"));
            tml.logTimeAndMemoryUsage(75);
            //////////////
            list = this.creatASListWithNelement(100);
            this.callTheGC();
            tml.setInitialTimeAndMemory();
            listCpy = list;
            modifiedList = list.Insert(24, new Payload("Load 99*"));

            tml.logTimeAndMemoryUsage(100);




        }
    }
}
