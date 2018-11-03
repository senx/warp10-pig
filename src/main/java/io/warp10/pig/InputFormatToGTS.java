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

import com.geoxp.GeoXPLib;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.pig.utils.GTSWrapperPigHelper;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;
import java.util.Map;

/**
 * Converts a Tuple of String (Warp 10 Input Format) into a Tuple of GTSWrapper
 *
 */
public class InputFormatToGTS extends EvalFunc<DataByteArray> {

  public InputFormatToGTS() {  }

  @Override
  public DataByteArray exec(Tuple input) throws IOException {

    if (1 != input.size()) {
      throw new IOException("Invalid input (nb fields != 1), expecting a single tuple with one field (chararray)");
    } else {
      if (DataType.findType(input.get(0)) != DataType.CHARARRAY) {
        throw new IOException("Invalid input (" + DataType.findTypeName(input.get(0)) + "), expecting a single tuple with one field (chararray)");
      }
    }

    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    String gtsStr = (String) input.get(0);
    GTSEncoder encoder = null;
    DataByteArray encoded = null;

    try {
      encoder = GTSHelper.parse(encoder, gtsStr);
      GTSWrapper gtsWrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper(encoder, true);

      encoded = new DataByteArray(serializer.serialize(gtsWrapper));

    } catch (ParseException pe) {
      throw new IOException(pe);
    } catch (TException te) {
      throw new IOException(te);
    }

    return encoded;

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
