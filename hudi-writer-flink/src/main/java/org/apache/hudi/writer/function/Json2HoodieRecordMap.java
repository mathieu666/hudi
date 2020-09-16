package org.apache.hudi.writer.function;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.writer.WriteJob;
import org.apache.hudi.writer.keygen.KeyGenerator;
import org.apache.hudi.writer.schema.FilebasedSchemaProvider;
import org.apache.hudi.writer.source.helpers.AvroConvertor;
import org.apache.hudi.writer.utils.DataSourceUtils;
import org.apache.hudi.writer.utils.UtilHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Json2HoodieRecordMap implements MapFunction<String, HoodieRecord> {
  private final WriteJob.Config cfg;
  private TypedProperties props;
  private AvroConvertor convertor;
  private KeyGenerator keyGenerator;

  private static Logger logger = LoggerFactory.getLogger(Json2HoodieRecordMap.class);

  public Json2HoodieRecordMap(WriteJob.Config cfg) {
    this.cfg = cfg;
    init();
  }

  @Override
  public HoodieRecord map(String value) throws Exception {
    convertor = new AvroConvertor(new FilebasedSchemaProvider(props).getSourceSchema());
    GenericRecord gr = convertor.fromJson(value);

    HoodieRecordPayload payload = DataSourceUtils.createPayload(cfg.payloadClassName, gr,
        (Comparable) HoodieAvroUtils.getNestedFieldVal(gr, cfg.sourceOrderingField, false));

    return new HoodieRecord<>(keyGenerator.getKey(gr), payload);
  }

  private void init() {
    this.props = UtilHelpers.getProps(cfg);
    try {
      keyGenerator = DataSourceUtils.createKeyGenerator(props);
    } catch (IOException e) {
      logger.error(" init keyGenerator failed ", e);
    }
  }
}
