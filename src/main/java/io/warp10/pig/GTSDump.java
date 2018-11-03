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

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigProgressable;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Dump GTS
 */
public class GTSDump extends EvalFunc<DataBag> {

  private final String OPTIMIZE = "optimize";
  
  private boolean optimize = false;

  public GTSDump() {  }
  
  public GTSDump(String... args) {
    if (OPTIMIZE.equals(args[0])) {
      this.optimize = true;
    }
  }

  /**
   * Dump a GTS (toString)
   *
   * @param input Tuple containing the GTSWrapper to dump
   * @return a Bag of String representation of each datapoint
   * @throws java.io.IOException
   */
  @Override
  public DataBag exec(Tuple input) throws IOException {

    //
    // Get encoded
    //

    DataByteArray serialized = (DataByteArray) input.get(0);

    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

    GTSWrapper gtsWrapper = new GTSWrapper();

    try {
      deserializer.deserialize(gtsWrapper, (serialized.get()));
    } catch (TException te) {
      throw new IOException(te);
    }

    Metadata metadataChunk = new Metadata(gtsWrapper.getMetadata());

    GTSDecoder decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(gtsWrapper);

    //
    // Metadata as String (native mode only)
    //
    StringBuilder metasb = new StringBuilder();
    GTSHelper.metadataToString(metasb, metadataChunk.getName(), metadataChunk.getLabels());

    //
    // convert metadata to Map (intermediate mode only)
    //
    Map<String,Object> mapMetadata = new HashMap<String, Object>();

    if (null != metadataChunk) {
      if (null != metadataChunk.getName()) {
        mapMetadata.put("class", metadataChunk.getName());
      } else {
        mapMetadata.put("class", "");
      }
      if (null != metadataChunk.getLabels()) {
        mapMetadata.put("labels", metadataChunk.getLabels());
      } else {
        mapMetadata.put("labels", new HashMap<String, String>());
      }
      if (null != metadataChunk.getAttributes()) {
        mapMetadata.put("attributes", metadataChunk.getAttributes());
      } else {
        mapMetadata.put("attributes", new HashMap<String, String>());
      }
    }

    //
    // MainTuple and Databag to store values (intermediate mode only)
    //
    Tuple mainTuple = TupleFactory.getInstance().newTuple(2);
    mainTuple.set(0, mapMetadata);
    DataBag valueBag = new DefaultDataBag();
    mainTuple.set(1, valueBag);

    //
    // main Databag
    //
    DataBag outbag = new DefaultDataBag();


    PigProgressable progressable = this.getReporter();
  
    StringBuilder optsb = new StringBuilder("=");

    boolean first = true;

    while(decoder.next()) {
      if (null != progressable) {
        progressable.progress();
      }

      Tuple tValues = TupleFactory.getInstance().newTuple(1);

      if (this.optimize && !first) {
        optsb.setLength(1);
        optsb.append(GTSHelper
            .tickToString(null, decoder.getTimestamp(), decoder.getLocation(),
                decoder.getElevation(), decoder.getValue()));
        tValues.set(0, optsb.toString());
      } else {
        tValues.set(0, GTSHelper.tickToString(metasb, decoder.getTimestamp(),
            decoder.getLocation(), decoder.getElevation(),
            decoder.getValue()));
        first = false;
      }
      outbag.add(tValues);
    }

    return outbag;
  }

  @Override
  public Schema outputSchema(Schema input) {

    //
    // GTS (DUMP)
    //

    Schema.FieldSchema fieldSchema = new Schema.FieldSchema("gts", DataType.BAG);

    return new Schema(fieldSchema);
  }
}