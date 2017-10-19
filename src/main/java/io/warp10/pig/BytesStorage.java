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
