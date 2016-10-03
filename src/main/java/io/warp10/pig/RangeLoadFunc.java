package io.warp10.pig;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BinSedesTupleFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * A handy load function which produces a sequence of LONGs
 * of the specified size and starting from 0.
 * 
 * Usage:
 * 
 * REL = LOAD 'range://start:count' USING io.warp10.pig.RangeLoadFunc();
 * 
 * Set range.splits to the desired number of splits (defaults to 1).
 */
public class RangeLoadFunc extends LoadFunc implements LoadMetadata {

  private TupleFactory tfactory = new BinSedesTupleFactory();

  private final RangeInputFormat inputFormat;
  
  private RecordReader reader = null;
  
  public RangeLoadFunc() {
    this.inputFormat = new RangeInputFormat();
  }
  
  public RangeLoadFunc(String... args) {
    this();
    String[] tokens = args[0].split(":");        
    
    this.inputFormat.setRange(Long.parseLong(tokens[0]), Long.parseLong(tokens[1]));
  }

  @Override
  public InputFormat getInputFormat() throws IOException {
    return this.inputFormat;
  }

  @Override
  public Tuple getNext() throws IOException {
    
    try {
      if (!reader.nextKeyValue()) {
        return null;
      }
      
      
      Tuple t = this.tfactory.newTuple(2);

      t.set(0, ((LongWritable) reader.getCurrentKey()).get());
      t.set(1, ((LongWritable) reader.getCurrentValue()).get());

      return t;      
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
    this.reader = reader;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    if (!location.startsWith("range://")) {
      throw new IOException("Location is expected to be of the form range://start:count");
    }
    String[] tokens = location.substring(8).split(":");
    
    this.inputFormat.setRange(Long.parseLong(tokens[0]), Long.parseLong(tokens[1]));
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    ResourceSchema schema = new ResourceSchema();

    ResourceSchema.ResourceFieldSchema[] fields = new ResourceSchema.ResourceFieldSchema[2];

    fields[0] = new ResourceSchema.ResourceFieldSchema(new Schema.FieldSchema("seqno", DataType.LONG));
    fields[1] = new ResourceSchema.ResourceFieldSchema(new Schema.FieldSchema("value", DataType.LONG));

    schema.setFields(fields);
    return schema;
  }

  @Override
  public String[] getPartitionKeys(String location, Job job) throws IOException {
    return null;
  }

  @Override
  public ResourceStatistics getStatistics(String location, Job job) throws IOException {
    return null;
  }

  @Override
  public void setPartitionFilter(Expression partitionFilter) throws IOException {
  }
  
  @Override
  public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
    return location;
  }
}
