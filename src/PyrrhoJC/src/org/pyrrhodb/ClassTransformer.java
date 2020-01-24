/*
 * ClassTransformer.java
 *
 * Created on 25 November 2006, 14:20
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.pyrrhodb;

import java.security.ProtectionDomain;

/**
 *
 * @author Malcolm
 */
public interface ClassTransformer {
    
    public byte[] transform(ClassLoader loader,String className,
            Class<?> classBeingRedefined, ProtectionDomain protectionDomain,
            byte[] classfileBuffer);
}
