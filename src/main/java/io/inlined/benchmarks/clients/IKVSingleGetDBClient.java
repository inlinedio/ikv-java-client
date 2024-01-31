package io.inlined.benchmarks.clients;

import io.inlined.benchmarks.SingleGetDBClient;
import io.inlined.clients.*;
import java.util.Map;

public class IKVSingleGetDBClient implements SingleGetDBClient {
  private final InlineKVReader _reader;
  private final InlineKVWriter _writer;

  public IKVSingleGetDBClient(String accountId, String accountPassKey) {
    ClientOptions writerClientOptions =
        new ClientOptions.Builder()
            .withAccountId(accountId)
            .withAccountPassKey(accountPassKey)
            .withStoreName("testing-store")
            .build();

    ClientOptions readerClientOptions =
        new ClientOptions.Builder()
            .withMountDirectory("/tmp/Benchmarks")
            .withStoreName("testing-store")
            .withAccountId(accountId)
            .withAccountPassKey(accountPassKey)
            .withKafkaPropertyOverride("ssl.ca.location", "/etc/ssl/certs")
            .useBytesPrimaryKey()
            .build();

    IKVClientFactory factory = new IKVClientFactory();
    _reader = factory.createNewReaderInstance(readerClientOptions);
    _writer = factory.createNewWriterInstance(writerClientOptions);
  }

  @Override
  public void startup() {
    _writer.startupWriter();
    _reader.startupReader();
  }

  @Override
  public void shutdown() {
    _writer.shutdownWriter();
    _reader.shutdownReader();
  }

  @Override
  public byte[] getValue(byte[] key, String fieldName, byte[] fieldNameUtf8Bytes) {
    return _reader.getBytesValue(key, fieldName);
  }

  @Override
  public void setValues(byte[] key, Map<String, byte[]> fieldValues) {
    IKVDocument.Builder builder =
        new IKVDocument.Builder().putBytesField("userid", key); // init with pkey value

    for (Map.Entry<String, byte[]> entry : fieldValues.entrySet()) {
      builder.putBytesField(entry.getKey(), entry.getValue());
    }

    _writer.upsertFieldValues(builder.build());
  }
}
