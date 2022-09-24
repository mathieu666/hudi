package org.apache.hudi.utilities.schema.postprocessor.add;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.schema.SchemaPostProcessor;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * A {@link SchemaPostProcessor} used to add a new decimal column of primitive types to given schema.
 */
public class AddDecimalColumnSchemaPostProcessor extends SchemaPostProcessor {

  protected AddDecimalColumnSchemaPostProcessor(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
  }

  @Override
  public Schema processSchema(Schema schema) {
    return null;
  }
}
