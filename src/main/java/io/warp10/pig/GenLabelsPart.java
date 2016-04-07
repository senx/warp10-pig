package io.warp10.pig;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Generate a value against the list of labels we want as equivalence classes (Partition)
 * This value is the list of values for each label
 *
 * Created by dav on 20/10/14.
 */
public class GenLabelsPart extends EvalFunc<String> {

  /**
   * List of labels we want as equivalence classes. Separator : ','
   */
  protected String labelsEquiv = "*";

  /**
   *
   * @param labelsEquiv
   */
  public GenLabelsPart(String... args) {
    this.labelsEquiv = StringUtils.trim(args[0]);
  }

  /**
   *
   * @param input
   * @return Tuple
   */
  @Override
  public String exec(Tuple input) throws IOException {

    StringBuffer filterValue = null;

    Map<String,String> labels = (Map)input.get(0);

    //
    // String generated against the values of labels we want as equivalence classes.
    // if * => return * (stands for all ie reduce on all equivalence classes)
    //
    if ("*".equals(labelsEquiv)) {
      filterValue = new StringBuffer("*");
    } else {
      if (null != labels) {
        List<String> listOfLabelsEquiv = Lists
            .newArrayList(Splitter.on(",").trimResults().split(labelsEquiv));
        for (String labelFilter : listOfLabelsEquiv) {
          //
          // Get the value for this key
          //
          String labelValue = labels.get(labelFilter);
          if (null != labelValue) {
            if (null == filterValue) {
              filterValue = new StringBuffer();
            } else {
              filterValue.append("#");
            }
            filterValue.append(labelValue);
          }
        }
      }
    }
    return (null != filterValue) ? filterValue.toString() : null;
  }

  @Override
  public Schema outputSchema(Schema input) {

    Schema.FieldSchema fieldSchema = new Schema.FieldSchema("labelsEquiv", DataType.CHARARRAY);

    return new Schema(fieldSchema);

  }

}