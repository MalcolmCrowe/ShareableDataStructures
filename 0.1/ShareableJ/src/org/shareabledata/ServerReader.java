/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

import java.net.Socket;

/**
 * This class is not shareable
 * @author Malcolm
 */
public class ServerReader extends SocketReader {
    public ServerReader(Socket c) throws Exception
    {
        super(c);
    }
}
