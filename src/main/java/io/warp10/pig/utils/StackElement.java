//
//   Copyright 2018  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
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

  private Object object = null;

  //
  // Hash computed onto the current object
  //

  private long elementId = Long.MAX_VALUE;

  //
  // Hash key to compute Sip64 hash on Object
  //

  protected static long[] SIPKEY_OBJECT =  new long[] {0x2C1878AF43ED268CL, 0x12D4C8A1E526B8D8L};


  public StackElement(int level, Object object) throws IOException {

    if(!(object instanceof Serializable)) {
      throw new IOException("object must implement Serializable");
    }

    if (level < 0) {
      throw new IOException("level must be >= 0");
    }

    this.level = level;
    this.object = object;

    byte[] rawElement = null;
    if (object instanceof String) {
      rawElement = ((String) object).getBytes(Charsets.UTF_8);
    } else if (object instanceof GTSWrapper) {
      rawElement = ((GTSWrapper) object).getEncoded();
    } else if (object instanceof byte[]) {
      rawElement = (byte[]) object;
    } else {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(object);
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

  public Object getObject() { return object; }

  public long getElementId() {
    return elementId;
  }

}
