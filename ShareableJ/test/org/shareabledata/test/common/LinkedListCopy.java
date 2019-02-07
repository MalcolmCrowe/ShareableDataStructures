/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata.test.common;

import java.util.LinkedList;

/**
 *
 * @author 77800577
 */
public class LinkedListCopy {

    public static LinkedList<PayLoad> deepCopy(LinkedList<PayLoad> list) {
        LinkedList<PayLoad> copiedList = new LinkedList<PayLoad>();
        
        for(PayLoad payloadObject : list){
            PayLoad copied = (PayLoad) payloadObject.clone();
            copiedList.add(copied);
                    
        }
        return copiedList;
    }
    
}
