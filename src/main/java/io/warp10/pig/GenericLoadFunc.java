package io.warp10.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BinSedesTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import io.warp10.hadoop.WritableUtils;
import io.warp10.pig.utils.PigUtils;

public class GenericLoadFunc extends LoadFunc {
  
  private static final String PIG_INPUT_FORMAT = "pig.input.format";
  private static final String PIG_CONF_SUFFIX = "pig.conf.suffix";
  
  private TupleFactory tfactory = new BinSedesTupleFactory();
  
  private Job job = null;
  private final String suffix;
  private RecordReader reader = null;  
  
  static {
    PigWarpConfig.ensureConfig();
  }

  @Override
  public InputFormat getInputFormat() throws IOException {
    //
    // Retrieve the class of the InputFormat we should create
    //
    
    Configuration conf = this.job.getConfiguration();
    
    String confsfx = conf.get(PIG_CONF_SUFFIX + this.suffix, "");
    
    if (!"".equals(confsfx)) {
      confsfx = "." + confsfx;
      List<Entry<String,String>> keys = new ArrayList<Entry<String,String>>();
      Iterator<Entry<String,String>> iter = conf.iterator();
      while(iter.hasNext()) {
        Entry<String,String> entry = iter.next();
        
        if (entry.getKey().endsWith(confsfx)) {
          keys.add(entry);
        }
      }
      
      // Override or create the unsuffixed configuration parameters
      for (Entry<String,String> entry: keys) {
        String key = entry.getKey().substring(0, entry.getKey().length() - confsfx.length());
        conf.set(key, entry.getValue());
      }
    }

    String inputFormat = conf.get(PIG_INPUT_FORMAT + this.suffix);
        
    if (null == inputFormat) {
      inputFormat = conf.get(PIG_INPUT_FORMAT);
    }
    
    try {
      Class ifclass = Class.forName(inputFormat);
      InputFormat format = (InputFormat) ifclass.newInstance();

      return format;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
  
  public GenericLoadFunc() {
    this.suffix = "";
  }
  
  
  public GenericLoadFunc(String suffix) {
    if (null != suffix) {
      if (!"".equals(suffix)) {
        this.suffix = "." + suffix;
      } else {
        this.suffix = "";
      }
    } else {
      this.suffix = "";
    }
  }
  
  @Override
  public Tuple getNext() throws IOException {
    try {
      if (!this.reader.nextKeyValue()) {
        return null;
      }

      Object key = WritableUtils.fromWritable((Writable) this.reader.getCurrentKey());
      Object value = WritableUtils.fromWritable((Writable) this.reader.getCurrentValue());

      Tuple t = this.tfactory.newTuple(2);

      t.set(0, PigUtils.toPig(key));
      t.set(1, PigUtils.toPig(value));

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
    this.job = job;
  }  
}
