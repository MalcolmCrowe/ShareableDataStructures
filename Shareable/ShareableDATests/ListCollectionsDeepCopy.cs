using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShareableDATests
{
    public class ListCollectionsDeepCopy
    {
        public static LinkedList<Payload> deepCopy(LinkedList<Payload> list)
        {
            LinkedList<Payload> copiedList = new LinkedList<Payload>();
            
            Payload lastItem = null;
            foreach (Payload payloadObject in list)
            {
                Payload copied = (Payload ) payloadObject.Clone();
                if (lastItem == null) {
                    copiedList.AddFirst(copied);
                }
                else {
                    copiedList.AddAfter(new LinkedListNode<Payload>(lastItem), copied);
                }
                lastItem = copied;

            }
            return copiedList;
        }
    }
}
