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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.DummyKeyStore;
import io.warp10.crypto.KeyStore;
import io.warp10.pig.utils.GTSWrapperPigHelper;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import org.apache.commons.lang.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;
import java.util.*;

/**
 * Generate classId,labelsId
 * Input : GTSWrapper
 */
public class GenIds extends EvalFunc<Tuple> {

  public GenIds() { }

  /**
   *
   * @param input : GTSWrapper
   * @return tuple (classId, labelsId)
   * @throws IOException
   */
  @Override
  public Tuple exec(Tuple input) throws IOException {

    if (null == input || input.size() != 1) {
      throw new IOException("1 parameter is required ! - GTSWrapper");
    }

    reporter.progress();

    DataByteArray gtsWrapperBytes = (DataByteArray) input.get(0);
    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

    GTSWrapper gtsWrapper = new GTSWrapper();

    try {
      deserializer.deserialize(gtsWrapper, (gtsWrapperBytes.get()));
    } catch (TException te) {
      throw new IOException(te);
    }

    if (null == gtsWrapper.getMetadata()) {
      gtsWrapper.setMetadata(new Metadata());
    }

    String className = gtsWrapper.getMetadata().getName();
    Map labels = gtsWrapper.getMetadata().getLabels();

    List<Long> ids = GTSWrapperPigHelper.genIds(gtsWrapper);

    Long classIdForJoin = ids.get(0);
    Long labelsIdForJoin = ids.get(1);

    Tuple tuple = TupleFactory.getInstance().newTuple(2);

    tuple.set(0, classIdForJoin);
    tuple.set(1, labelsIdForJoin);

    return tuple;

  }

  @Override
  public Schema outputSchema(Schema input) {

    List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>();
    fields.add(new Schema.FieldSchema("classId", DataType.LONG));
    fields.add(new Schema.FieldSchema("labelsId", DataType.LONG));

    Schema tupleSchema = new Schema(fields);

    Schema.FieldSchema outputTuple = new Schema.FieldSchema("gtsIds", DataType.TUPLE);

    outputTuple.schema = tupleSchema;

    return new Schema(outputTuple);

  }

}