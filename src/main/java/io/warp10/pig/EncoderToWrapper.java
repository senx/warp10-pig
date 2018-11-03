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
package io.warp10.pig;

import io.warp10.pig.utils.PigUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;

/**
 * Convert a GTSEncoder wrapped as String to GTSWrapper encoded as bytearray (DataByteArray)
 *
 */
public class EncoderToWrapper extends EvalFunc<DataByteArray> {

  public EncoderToWrapper() {  }

  @Override
  public DataByteArray exec(Tuple input) throws IOException {

    if (1 != input.size()) {
      throw new IOException("Invalid input (nb fields != 1), expecting a single tuple with one field (chararray)");
    } else {
      if (DataType.findType(input.get(0)) != DataType.CHARARRAY) {
        throw new IOException("Invalid input (" + DataType.findTypeName(input.get(0)) + "), expecting a single tuple with one field (chararray)");
      }
    }

    return PigUtils.gtsEncoderToGTSWrapper((String)input.get(0));

  }

  /**
   * @param input
   * @return encoded: bytearray
   */
  @Override
  public Schema outputSchema(Schema input) {

    Schema.FieldSchema fieldSchema = new Schema.FieldSchema("encoded", DataType.BYTEARRAY);

    return new Schema(fieldSchema);

  }
}
