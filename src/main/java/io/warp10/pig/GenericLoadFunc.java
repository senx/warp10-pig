//
//   Copyright 2018-2021  SenX S.A.S.
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
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BinSedesTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import io.warp10.hadoop.WarpScriptInputFormat;
import io.warp10.hadoop.WritableUtils;
import io.warp10.pig.utils.PigUtils;

public class GenericLoadFunc extends LoadFunc {

  private static final String PIG_GENERICLOAD_INPUTFORMAT = "pig.genericload.inputformat";
  private static final String PIG_GENERICLOAD_CONF_SUFFIX = "pig.genericload.conf.suffix";

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

    String confsfx = conf.get(PIG_GENERICLOAD_CONF_SUFFIX + this.suffix, "");

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

    String inputFormat = conf.get(PIG_GENERICLOAD_INPUTFORMAT + this.suffix);

    if (null == inputFormat) {
      inputFormat = conf.get(PIG_GENERICLOAD_INPUTFORMAT);
    }

    try {
      Class ifclass = Class.forName(inputFormat);
      InputFormat format = (InputFormat) ifclass.newInstance();

      // Special case for the WarpScriptInputFormat to set its suffix
      if (format instanceof WarpScriptInputFormat) {
        ((WarpScriptInputFormat) format).init(conf);
      }
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

      Object key = this.reader.getCurrentKey();
      Object value = this.reader.getCurrentValue();

      if (key instanceof Writable) {
        key = WritableUtils.fromWritable((Writable) key);
      }

      if (value instanceof Writable) {
        value = WritableUtils.fromWritable((Writable) value);
      }

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
