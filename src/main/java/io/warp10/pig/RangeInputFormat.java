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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.Reporter;

public class RangeInputFormat extends InputFormat<LongWritable, LongWritable> implements org.apache.hadoop.mapred.InputFormat<LongWritable, LongWritable> {

  public static final String RANGE_SPLITS = "range.splits";
  
  private long start;
  private long count;
  
  public static final class RangeSplit extends InputSplit implements org.apache.hadoop.mapred.InputSplit {
    
    /**
     * First element of the sequence
     */
    private long start;
    
    /**
     * Number of elements in the sequence
     */
    private long count;
    
    public RangeSplit() {
    }

    public RangeSplit(long start, long count) {
      this.start = start;
      this.count = count;
    }
    
    public long getLength() throws IOException {
      return this.count;
    }
    
    @Override
    public String[] getLocations() throws IOException {
      return new String[0];
    }        
    
    @Override
    public void readFields(DataInput in) throws IOException {
      this.start = in.readLong();
      this.count = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(this.start);
      out.writeLong(this.count);
    }

    @Override
    public String toString() {
      return "range://" + Long.toString(this.start) + ":" + Long.toString(this.count);
    }
  }
  
  public static final class RangeRecordReader extends RecordReader<LongWritable, LongWritable> {
    
    private long start;
    private long count;
    private AtomicLong index = new AtomicLong(-1L);    

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      if (index.get() < 0) {
        throw new IOException("nextKeyValue was not called.");
      }
      return new LongWritable(index.get()); 
    }
    
    @Override
    public LongWritable getCurrentValue() throws IOException, InterruptedException {
      if (index.get() < 0) {
        throw new IOException("nextKeyValue was not called.");
      }
      return new LongWritable(this.start + this.index.get());
    }
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      if (!(split instanceof RangeSplit)) {
        throw new IOException("Invalid split type, expecting " + RangeSplit.class + " but was " + split.getClass());
      }
      
      RangeSplit rsplit = (RangeSplit) split;
      
      this.start = rsplit.start;
      this.count = rsplit.count;
      this.index.set(-1L);
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      long idx = this.index.getAndIncrement();
      
      idx++;
      
      if (idx >= this.count) {
        return false;
      }
      
      return true;
    }
    
    @Override
    public void close() throws IOException {}
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
      return this.index.get() / (float) this.count;
    }
  }
  
 
  @Override
  public List<InputSplit> getSplits(org.apache.hadoop.mapreduce.JobContext context) throws IOException ,InterruptedException {
    return createSplits(context.getConfiguration().getInt(RANGE_SPLITS, 1));
  }

  @Override
  public RecordReader<LongWritable, LongWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {    
    return new RangeRecordReader();
  }
  
  public void setRange(long start, long count) {
    this.start = start;
    this.count = count;
  }
  
  @Override
  public org.apache.hadoop.mapred.RecordReader<LongWritable, LongWritable> getRecordReader(org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter) throws IOException {
    try {
      return (org.apache.hadoop.mapred.RecordReader<LongWritable, LongWritable>) createRecordReader((InputSplit) split, (TaskAttemptContext) null);      
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }
  
  @Override
  public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    List<InputSplit> splits = createSplits(numSplits);
    
    org.apache.hadoop.mapred.InputSplit[] asplits = new org.apache.hadoop.mapred.InputSplit[splits.size()];
    
    for (int i = 0; i < asplits.length; i++) {
      asplits[i] = (org.apache.hadoop.mapred.InputSplit) splits.get(i);      
    }
    
    return asplits;
  }
  
  private List<InputSplit> createSplits(int numSplits) {
    long step = (int) Math.ceil(this.count / (double) numSplits);
    
    List<InputSplit> splits = new ArrayList<InputSplit>();
    
    long start = this.start;
    long end = start + count - 1;

    while(start <= end) {      
      splits.add(new RangeSplit(start, Math.min(end, start + step - 1) - start + 1));
      start = start + step;
    }

    return splits;
  }
}
