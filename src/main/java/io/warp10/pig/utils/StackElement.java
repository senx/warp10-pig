package io.warp10.pig.utils;

import com.google.common.base.Charsets;
import com.google.common.collect.ComparisonChain;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.SipHashInline;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Object wrapper to sort Element in the Warpscript Stack
 */
public class StackElement implements Comparable<StackElement> {

  private int level = Integer.MAX_VALUE;

  private Object element = null;

  //
  // Hash computed onto the current element
  //

  private long elementId = Long.MAX_VALUE;

  //
  // Hash key to compute Sip64 hash on Object
  //

  protected static long[] SIPKEY_OBJECT =  new long[] {0x2C1878AF43ED268CL, 0x12D4C8A1E526B8D8L};


  public StackElement(int level, Object element) throws IOException {

    if(!(element instanceof Serializable)) {
      throw new IOException("element must implement Serializable");
    }

    if (level < 0) {
      throw new IOException("level must be >= 0");
    }

    this.level = level;
    this.element = element;

    byte[] rawElement = null;
    if (element instanceof String) {
      rawElement = ((String)element).getBytes(Charsets.UTF_8);
    } else if (element instanceof GTSWrapper) {
      rawElement = ((GTSWrapper) element).getEncoded();
    } else if (element instanceof byte[]) {
      rawElement = (byte[])element;
    } else {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(element);
      oos.flush();
      oos.close();
      bos.close();
      rawElement = bos.toByteArray();
    }
    elementId = SipHashInline.hash24(SIPKEY_OBJECT[0], SIPKEY_OBJECT[1], rawElement, 0, rawElement.length);

  }

  @Override
  public int compareTo(StackElement other) {
    return ComparisonChain.start()
        .compare(level, other.getLevel())
        .compare(elementId, other.getElementId()).result();
  }

  public int getLevel() {
    return level;
  }

  public Object getElement() { return element; }

  public long getElementId() {
    return elementId;
  }

}
