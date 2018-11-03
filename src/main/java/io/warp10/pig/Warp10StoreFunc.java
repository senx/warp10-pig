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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import com.google.common.base.Charsets;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.hadoop.Warp10OutputFormat;

public class Warp10StoreFunc extends StoreFunc {
  
  private final String suffix;
  
  private RecordWriter writer = null;
  
  public Warp10StoreFunc() {
    this.suffix = "";
  }
  
  public Warp10StoreFunc(String suffix) {
    this.suffix = suffix;
  }
  
  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
    this.writer = writer;
  }
  
  @Override
  public void putNext(Tuple t) throws IOException {
    //
    // We support the following types:
    //
    // chararray, either a GTS formatted line, i.e. TS/LAT:LON/ELEV CLASS{LABELS} VALUE
    // or an OPB64 encoded serialized GTSWrapper
    //
    // bytearray, a serialized GTSWrapper
    //
    // data bag with tuples containing 0 or 1 element of the above type
    //

    if (0 == t.size()) {
      return;
    }

    if (t.size() > 1) {
      throw new IOException("Invalid input size, expected a tuple with 0 or 1 element, encountered " + t.size());
    }
        
    try {
      putValue(t.get(0));
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
  
  private void putValue(Object value) throws Exception {
    if (DataType.BYTEARRAY == DataType.findType(value)) {
      DataByteArray dba = (DataByteArray) value;
      this.writer.write(NullWritable.get(), new BytesWritable(dba.get()));
    } else if (DataType.CHARARRAY == DataType.findType(value)) {
      String valueStr = (String) value;

      //
      // If the String contains a '/' this is Input Format
      // Otherwise this a GTSWrapper wrapped as OrderPreservingBase64 (no '/')
      //

      if (valueStr.contains("/")) {
        GTSEncoder encoder = GTSHelper.parse(null, valueStr);
        GTSWrapper wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper(encoder, false);
        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        this.writer.write(NullWritable.get(), new BytesWritable(serializer.serialize(wrapper)));
      } else {
        byte[] bytes = OrderPreservingBase64.decode(valueStr.getBytes(Charsets.US_ASCII));
        this.writer.write(NullWritable.get(), new BytesWritable(bytes));
      }
    } else if (DataType.BAG == DataType.findType(value)) {
      DataBag inbag = (DataBag) value;
      Iterator<Tuple> iterMain = inbag.iterator();

      while (iterMain.hasNext()) {
        //
        // One tuple with one field (bytearray or chararray)
        //

        Tuple inTuple = iterMain.next();
        
        if (0 == inTuple.size()) {
          continue;
        }
        
        Object inValue = inTuple.get(0);

        if (DataType.BYTEARRAY == DataType.findType(inValue)) {
          putValue(inValue);
        } else if (DataType.CHARARRAY == DataType.findType(inValue)) {
          putValue(inValue);
        } else {
          throw new IOException("Invalid tuple found, only chararrays and bytearrays can appear in a bag.");
        }
      }
    } else {
      throw new IOException("Invalid input, only chararray, bytearray and bag are supported.");
    }
  }
  
  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
  }   
  
  @Override
  public OutputFormat getOutputFormat() throws IOException {
    return new Warp10OutputFormat(this.suffix);
  }
}
