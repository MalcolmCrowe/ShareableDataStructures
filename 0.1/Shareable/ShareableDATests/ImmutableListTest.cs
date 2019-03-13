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
            listBuilder.Add(firstElement);


            tml.logTimeAndMemoryUsage(1);

            for (int i = 1; i < numberOfElements; i++)
            {
                
                listBuilder.Add(new Payload("Load " + i));
                
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
            listBuilder.Add(firstElement);
            bool salida = listBuilder.Contains(firstElement);
            Assert.True(salida);
            for (int i = 1; i < numberOfElements; i++)
            {
                listBuilder.Add( new Payload("Load " + i));
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

            Payload toBeFound_100 = new Payload("Load 100");
            Assert.AreEqual(100, list.Count);
            tml.setInitialTimeAndMemory();
            list.Remove(toBeFound_25);//removes element at 25
            Assert.AreEqual(99, list.Count);
            tml.logTimeAndMemoryUsage(25);
            list.Remove(toBeFound_50);
            Assert.AreEqual(98, list.Count);
            tml.logTimeAndMemoryUsage(50);
            list.Remove(toBeFound_75);
            Assert.AreEqual(97, list.Count);
            tml.logTimeAndMemoryUsage(75);
            list.Remove(toBeFound_100);
            Assert.AreEqual(96, list.Count);
            tml.logTimeAndMemoryUsage(100);
        }


        [Test]
        public void DeepCopyAndAddIn100()
        {
            tml.setTestCaseName("DeepCopyAndAddIn100");

            ImmutableList<Payload> list = this.creatASListWithNelement(100);


            tml.setInitialTimeAndMemory();
            ImmutableList<Payload> listCpy = list;
            list.Insert(24, new Payload("Load 25*"));

            tml.logTimeAndMemoryUsage(25);
            ///////////
            list = this.creatASListWithNelement(100);

            tml.setInitialTimeAndMemory();
            listCpy = list;
            list.Insert(24, new Payload("Load 50*"));
            tml.logTimeAndMemoryUsage(50);
            ///////////////
            list = this.creatASListWithNelement(100);
            this.callTheGC();
            tml.setInitialTimeAndMemory();
            listCpy = list;
            list.Insert(24, new Payload("Load 75*"));
            tml.logTimeAndMemoryUsage(75);
            //////////////
            list = this.creatASListWithNelement(100);
            this.callTheGC();
            tml.setInitialTimeAndMemory();
            listCpy = list;
            list.Insert(24, new Payload("Load 99*"));

            tml.logTimeAndMemoryUsage(100);




        }
    }
}
