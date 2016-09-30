package io.warp10.pig;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
 */
public class RangeLoadFunc extends LoadFunc implements LoadMetadata {

  private TupleFactory tfactory = new BinSedesTupleFactory();

  private final long occurrences;
  
  private long count = 0;

  public RangeLoadFunc(String... args) {
    if (1 != args.length) {
      throw new RuntimeException("Expecting number of elements as parameter.");
    }    
    
    this.occurrences = Long.parseLong(args[0].toString());    
  }

  @Override
  public InputFormat getInputFormat() throws IOException {
    return new TextInputFormat();
  }

  @Override
  public Tuple getNext() throws IOException {
    if (this.count >= this.occurrences) {
      return null;
    }
      
    Tuple t = this.tfactory.newTuple(1);

    t.set(0, this.count++);

    return t;
  }

  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {}

  @Override
  public void setLocation(String location, Job job) throws IOException {
    org.apache.hadoop.mapreduce.lib.input.TextInputFormat.setInputPaths(job, location);
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    ResourceSchema schema = new ResourceSchema();

    ResourceSchema.ResourceFieldSchema[] fields = new ResourceSchema.ResourceFieldSchema[1];

    fields[0] = new ResourceSchema.ResourceFieldSchema(new Schema.FieldSchema("seqno", DataType.LONG));

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

}
