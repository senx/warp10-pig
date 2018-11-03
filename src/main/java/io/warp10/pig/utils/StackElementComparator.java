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

import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.Comparator;

/**
 * Comparator to sort Tuple with 2 fields (level: int, object: any) in SortedBag
 */
public class StackElementComparator implements Comparator<Tuple> {
  public int compare(Tuple t1, Tuple t2) {
    StackElement elt1 = null;
    StackElement elt2 = null;
    try {
      if ((t1.size() == t2.size()) && (2 == t1.size())) {
        elt1 = new StackElement((int) t1.get(0), (Object) t1.get(1));
        elt2 = new StackElement((int) t2.get(0), (Object) t2.get(1));
      } else {
        throw new IOException("Size of tuples is not the same or it is not equal to 2");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return elt1.compareTo(elt2);
  }
}
