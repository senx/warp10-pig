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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

public class BytesStorage extends StoreFunc {
  
  private RecordWriter writer = null;
  
  public BytesStorage() {
  }
  
  @Override
  public OutputFormat getOutputFormat() throws IOException {
    return new BytesOutputFormat();
  }
  
  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
    this.writer = writer;
  }
  
  @Override
  public void putNext(Tuple t) throws IOException {
    //
    // Check that tuple only has one element of type DataByteArray
    //
    
    if (1 != t.size()) {
      throw new IOException("Invalid tuple size, only singletons are supported.");
    }
    
    Object elt = t.get(0);
    
    if (!(elt instanceof DataByteArray)) {
      throw new IOException("Invalid element type, only bytearray elements are supported.");
    }
    
    DataByteArray dba = (DataByteArray) elt;
    
    BytesWritable bw = new BytesWritable(dba.get(), dba.size());
    
    try {
      writer.write(null, bw);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }
  
  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    BytesOutputFormat.setOutputPath(job, new Path(location));
  }
}
