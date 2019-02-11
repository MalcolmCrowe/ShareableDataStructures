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
    public class LinkedListTest
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
                throw e;
            }
        }

        [TearDown]
        public void tearDown()
        {
            CallTheGC();
        }

        private void CallTheGC()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        private void reusableLinkedlistTestCase(String caseName, int numberOfElements)
        {
            tml.setTestCaseName(caseName);

            LinkedList<Payload> list = new LinkedList<Payload>();
            list.AddFirst(new Payload("Load 0"));
            tml.setInitialTimeAndMemory();
            

            tml.logTimeAndMemoryUsage(1);

            for (int i = 1; i < numberOfElements; i++)
            {
                list.AddFirst(new Payload("load " + i));

                tml.logTimeAndMemoryUsage(i + 1);
            }

        }

        [Test]
        public void testInsert100Slist()
        {
            String caseID = "LinkedList 100 insert";
            reusableLinkedlistTestCase(caseID, 100);

        }

        [Test]
        public void testInsert1000Slist()
        {
            String caseID = "LinkedList 1000 insert";
            reusableLinkedlistTestCase(caseID, 1000);

        }

        [Test]
        public void testInsert10000Slist()
        {
            String caseID = "LinkedList 10000 insert";
            reusableLinkedlistTestCase(caseID, 10000);

        }



        private LinkedList<Payload> creatASListWithNelement(int numberOfElements)
        {
            LinkedList<Payload> list = new LinkedList<Payload>();
            Payload firstElement = new Payload("Load 0");
            list.AddFirst(firstElement);

            for (int i = 1; i<numberOfElements; i++)
            {
                Payload toAdd = new Payload("Load " + i);
                list.AddAfter(new LinkedListNode<Payload>(firstElement), toAdd);
                firstElement = toAdd;
            }

           
            return list;
        }

        [Test]
        public void RemoveElementIn100()
        {
            tml.setTestCaseName("Remove elements in 100");

            LinkedList<Payload> list = this.creatASListWithNelement(100);
            Payload toBeFound_25 = new Payload("Load 25");
            Payload toBeFound_50 = new Payload("Load 50");
            Payload toBeFound_75 = new Payload("Load 75");
            Payload toBeFound_100 = new Payload("Load 100");

            Assert.AreEqual(100, list.Count);
            tml.setInitialTimeAndMemory();
            list.Remove(toBeFound_25);
            ///////////////////////
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


        public void DeepCopyAndAddIn100()
        {
            tml.setTestCaseName("DeepCopyAndAddIn100");

            LinkedList<Payload> list = this.creatASListWithNelement(100);


            tml.setInitialTimeAndMemory();
            LinkedList<Payload> listCpy = list;
            //listCpy.InsertAt(new Payload("Load 25*"), 25);
            tml.logTimeAndMemoryUsage(25);
            ///////////
            list = this.creatASListWithNelement(100);

            tml.setInitialTimeAndMemory();
            listCpy = list;
            //listCpy.InsertAt(new Payload("Load 50*"), 50);
            tml.logTimeAndMemoryUsage(50);
            ///////////////
            list = this.creatASListWithNelement(100);
            this.callTheGC();
            tml.setInitialTimeAndMemory();
            listCpy = list;
            //listCpy.InsertAt(new Payload("Load 75*"), 75);
            tml.logTimeAndMemoryUsage(75);
            //////////////
            list = this.creatASListWithNelement(100);
            this.callTheGC();
            tml.setInitialTimeAndMemory();
            listCpy = list;
            //listCpy.InsertAt(new Payload("Load 99*"), 99);
            tml.logTimeAndMemoryUsage(100);




        }




    }
}
