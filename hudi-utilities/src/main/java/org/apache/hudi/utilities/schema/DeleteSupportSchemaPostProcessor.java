package org.apache.hudi.utilities.schema;

import org.apache.avro.Schema;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.types.StructType;

public class DeleteSupportSchemaPostProcessor extends SchemaPostProcessor {
  public DeleteSupportSchemaPostProcessor(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
  }

  @Override
  public Schema processSchema(Schema schema) {
    if (schema == null) {
      return null;
    }

    final StructType structType = AvroConversionUtils.convertAvroSchemaToStructType(schema);
    final StructType result = structType.add("_hoodie_is_deleted", "boolean", true);

    return AvroConversionUtils.convertStructTypeToAvroSchema(
        result, RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME,
        RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE);
  }
}
