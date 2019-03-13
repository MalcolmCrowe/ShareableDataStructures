/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 *
 * @author 66668214
 */
public abstract class Bookmark<T> {
        public final int Position;
        protected Bookmark(int p) { Position = p; }
        public abstract Bookmark<T> Next();
        public abstract T getValue();
        public void Append(StringBuilder sb) { }
}
