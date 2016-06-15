package io.warp10.pig;

import io.warp10.hadoop.Warp10InputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.*;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;

public class Warp10LoadFunc extends LoadFunc implements LoadMetadata {

  private TupleFactory tfactory = new BinSedesTupleFactory();
  private RecordReader reader;
  private PigSplit split;
  private String location;
  private String udfcSignature;

  private String splitsEndpoint;
  private String splitsSelector;
  private String splitsToken;

  public Warp10LoadFunc() {
  }

  @Override
  public InputFormat getInputFormat() throws IOException {
    return (InputFormat) new Warp10InputFormat();
  }

  @Override
  public Tuple getNext() throws IOException {

    try {
      if (!this.reader.nextKeyValue()) {
        return null;
      }

      String key = this.reader.getCurrentKey().toString();
      BytesWritable value = (BytesWritable) this.reader.getCurrentValue();

      Tuple t = this.tfactory.newTuple(2);

      t.set(0, key);
      t.set(1, new DataByteArray(value.copyBytes()));

      return t;
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
    this.reader = reader;
    this.split = split;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    this.location = location;
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    ResourceSchema schema = new ResourceSchema();

    ResourceSchema.ResourceFieldSchema[] fields = new ResourceSchema.ResourceFieldSchema[2];

    fields[0] = new ResourceSchema.ResourceFieldSchema(new Schema.FieldSchema("id", DataType.CHARARRAY));
    fields[1] = new ResourceSchema.ResourceFieldSchema(new Schema.FieldSchema("data", DataType.BYTEARRAY));

    schema.setFields(fields);
    return schema;
  }

  @Override
  public void setUDFContextSignature(String signature) {
    this.udfcSignature = signature;
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

}
