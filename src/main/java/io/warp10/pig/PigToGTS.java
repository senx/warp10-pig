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
import java.util.Iterator;
import java.util.Map;

/**
 * Converts a bag of tuples into a bag of GTSWrappers
 * {(ts, lat, lon, elev, value)} or {metadata: [], {(ts, lat, lon, elev, value)}}
 * {(ts, lat, lon, value)} or {metadata: [], {(ts, lat, lon, value)}}
 * {(ts, elev, value)} or {metadata: [], {(ts, elev, value)}}
 * {(ts, value)} or {metadata: [], {(ts, value)}}
 */
public class PigToGTS extends EvalFunc<DataBag> {

  private final long threshold;

  public PigToGTS() {
    this.threshold = 10000000L;
  }

  public PigToGTS(String... args) {
    this.threshold = Long.parseLong(args[0]);
  }

  @Override
  public DataBag exec(Tuple input) throws IOException {

    if (1 != input.size()) {
      throw new IOException("Invalid input (nb fields != 1), expecting a single bag with a tuple:(map, bag))");
    }

    if (1 == input.size()) {
      if (!(input.get(0) instanceof DataBag)) {
        throw new IOException(
            "Invalid input (" + DataType.findTypeName(input.get(0))
                + "), expecting a single bag with a tuple:(map, bag))");
      }
    }
    DataBag inbag = (DataBag) input.get(0);
    DataBag outbag = new DefaultDataBag();

    boolean isMetaProvided = false;

    GTSEncoder encoder = new GTSEncoder(0L);
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

    Iterator<Tuple> iterMain = inbag.iterator();

    while (iterMain.hasNext()) {
      Tuple mainTuple = iterMain.next();

      //
      // Get metadata (if provided)
      //

      Metadata metadata = null;
      if ((mainTuple.get(0) instanceof Map)) {
        Map inMap = (Map) mainTuple.get(0);
        metadata = GTSWrapperPigHelper.pig2Metadata(inMap);
        encoder.setMetadata(metadata);
        isMetaProvided = true;
      }

      DataBag valuesBag = null;
      if (isMetaProvided) {
        valuesBag = (DataBag) mainTuple.get(1);
      } else if (DataType.findType(mainTuple.get(0)) == DataType.BAG) {
        valuesBag = (DataBag) mainTuple.get(0);
      }

      if (null != valuesBag) {
        /**
         * 'Pig like' format  {(ts, lat, lon, elev, value)}
         */
        Iterator<Tuple> iter = valuesBag.iterator();

        while (iter.hasNext()) {
          Tuple tData = iter.next();

          Tuple t = decodeValues(tData);

          long ts = (long) t.get(0);
          long location = (long) t.get(1);
          long elevation = (long) t.get(2);
          Object value = t.get(3);

          encoder.addValue(ts, location, elevation, value);

          if (encoder.size() > this.threshold || !iter.hasNext()) {

            GTSWrapper wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper(encoder, true);
            Tuple tuple = TupleFactory.getInstance().newTuple(1);

            try {
              byte[] data = serializer.serialize(wrapper);

              tuple.set(0, new DataByteArray(data));
            } catch (TException te) {
              throw new IOException(te);
            }

            outbag.add(tuple);

            encoder = new GTSEncoder(0L);

          }
        }
      } else {
        /**
         * 'CSV like' format  (ts, lat, lon, elev, value): one row (tuple) per value
         */

        Tuple t = decodeValues(mainTuple);

        long ts = (long) t.get(0);
        long location = (long) t.get(1);
        long elevation = (long) t.get(2);
        Object value = t.get(3);

        encoder.addValue(ts, location, elevation, value);

        if (encoder.size() > this.threshold || !iterMain.hasNext()) {

          GTSWrapper wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper(encoder, true);
          Tuple tuple = TupleFactory.getInstance().newTuple(1);

          try {
            byte[] data = serializer.serialize(wrapper);

            tuple.set(0, new DataByteArray(data));
          } catch (TException te) {
            throw new IOException(te);
          }

          outbag.add(tuple);

          encoder = new GTSEncoder(0L);

        }
      }
    }
    return outbag;
  }

  /**
   * Extract ts,location,elevation,value from input data
   *
   * @param input: tuple with 2, 3, 4, or 5 elements.
   * @return (ts, location, elev, value) according to 'input' data
   * @throws IOException
   */
  protected Tuple decodeValues(final Tuple input) throws IOException {

    Tuple result = TupleFactory.getInstance().newTuple(4);

    if (input.size() < 2 || input.size() > 5) {
      throw new IOException("Invalid input, tuples should have 2, 3, 4, or 5 elements.");
    }

    long ts = (long) input.get(0);
    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    Object value = null;
    switch (input.size()) {
      case 2:
        value = input.get(1);
        break;
      case 3:
        elevation = (long) input.get(1);
        value = input.get(2);
        break;
      case 4:
        double lat = (double) input.get(1);
        double lon = (double) input.get(2);
        location = GeoXPLib.toGeoXPPoint(lat, lon);
        value = input.get(3);
        break;
      case 5:
        lat = (double) input.get(1);
        lon = (double) input.get(2);
        location = GeoXPLib.toGeoXPPoint(lat, lon);
        elevation = (long) input.get(3);
        value = input.get(4);
        break;
    }
    result.set(0, ts);
    result.set(1, location);
    result.set(2, elevation);
    result.set(3, value);

    return result;
  }

  /**
   * @param input
   * @return gts: {(encoded: bytearray)}
   */
  @Override
  public Schema outputSchema(Schema input) {

    Schema tupleSchema = new Schema(new Schema.FieldSchema("encoded", DataType.BYTEARRAY));

    Schema bagSchema = new Schema(tupleSchema);

    Schema.FieldSchema outputBag = new Schema.FieldSchema("gts", DataType.BAG);

    outputBag.schema = bagSchema;

    return new Schema(outputBag);

  }
}
