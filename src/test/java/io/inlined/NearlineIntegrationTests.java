package io.inlined;

import com.google.common.collect.ImmutableList;
import io.inlined.clients.*;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;

@Disabled
// ./gradlew test --tests "io.inlined.NearlineIntegrationTests" -PmaxParallelForks=1

public class NearlineIntegrationTests {
  private static final String ACCOUNT_ID = "foo";
  private static final String ACCOUNT_PASSKEY = "foo";
  private static final String MOUNT_DIR = "/tmp/JavaNearlineIntegrationTests";
  private static final String STORE_NAME = "foo";
  private static final String PRIMARY_KEY_FIELD_NAME = "foo";

  private static InlineKVReader _reader = null;
  private static InlineKVWriter _writer = null;

  @BeforeAll
  public static void setup() {
    ClientOptions clientOptions =
        new ClientOptions.Builder()
            .withMountDirectory(MOUNT_DIR)
            .withStoreName(STORE_NAME)
            .withAccountId(ACCOUNT_ID)
            .withAccountPassKey(ACCOUNT_PASSKEY)
            .useStringPrimaryKey()
            .build();

    IKVClientFactory factory = new IKVClientFactory();
    _writer = factory.createNewWriterInstance(clientOptions);
    _writer.startupWriter();
    _reader = factory.createNewReaderInstance(clientOptions);
    _reader.startupReader();
  }

  @AfterAll
  public static void teardown() {
    _writer.shutdownWriter();
    _reader.shutdownReader();
    _writer = null;
    _reader = null;
  }

  @Test
  public void upsertThenDelete() throws InterruptedException {
    int startDocId = 0;
    int endDocId = 100;

    // upsert documents
    // format: {pkey: "docid", field0: ..., field1: ...} {pkey: "docid", field2: ...}
    for (int docid = startDocId; docid < endDocId; docid++) {
      String primaryKey = String.format("pkey:%d", docid);
      IKVDocument document = new IKVDocument.Builder()
              .putStringField(PRIMARY_KEY_FIELD_NAME, primaryKey)
              .putStringField("field0", String.format("field0:%d", docid))
              .putBytesField("field1", String.format("field1:%d", docid)
                      .getBytes(StandardCharsets.UTF_8))
              .build();
      _writer.upsertFieldValues(document);
    }
    for (int docid = startDocId; docid < endDocId; docid++) {
      String primaryKey = String.format("pkey:%d", docid);
      IKVDocument document = new IKVDocument.Builder()
              .putStringField(PRIMARY_KEY_FIELD_NAME, primaryKey)
              .putIntField("field2", docid)
              .build();
      _writer.upsertFieldValues(document);
    }

    // sleep for 5s
    Thread.sleep(5000);

    // read and assert on present fields
    for (int docid = startDocId; docid < endDocId; docid++) {
      String primaryKey = String.format("pkey:%d", docid);

      String fetchedPrimaryKey = _reader.getStringValue(primaryKey, PRIMARY_KEY_FIELD_NAME);
      Assertions.assertEquals(fetchedPrimaryKey, primaryKey);

      String fetchedField0 = _reader.getStringValue(primaryKey, "field0");
      Assertions.assertEquals(fetchedField0, String.format("field0:%d", docid));

      byte[] fetchedField1 = _reader.getBytesValue(primaryKey, "field1");
      Assertions.assertArrayEquals(fetchedField1, String.format("field1:%d", docid).getBytes(StandardCharsets.UTF_8));

      Integer fetchedField2 = _reader.getIntValue(primaryKey, "field2");
      Assertions.assertEquals(fetchedField2, docid);
    }

    // delete documents
    for (int docid = startDocId; docid < endDocId; docid++) {
      String primaryKey = String.format("pkey:%d", docid);
      IKVDocument document = new IKVDocument.Builder()
              .putStringField(PRIMARY_KEY_FIELD_NAME, primaryKey).build();
      _writer.deleteDocument(document);
    }

    // sleep for 5s
    Thread.sleep(5000);

    for (int docid = startDocId; docid < endDocId; docid++) {
      String primaryKey = String.format("pkey:%d", docid);
      Assertions.assertNull(_reader.getStringValue(primaryKey, PRIMARY_KEY_FIELD_NAME));
    }
  }

  @Test
  public void dropFieldsAndDropDocuments() throws InterruptedException {
    int startDocId = 200;
    int endDocId = 210;

    // upsert documents
    // format: {pkey: "docid", field3: ..., field4: ...}
    for (int docid = startDocId; docid < endDocId; docid++) {
      String primaryKey = String.format("pkey:%d", docid);
      IKVDocument document = new IKVDocument.Builder()
              .putStringField(PRIMARY_KEY_FIELD_NAME, primaryKey)
              .putStringField("field3", String.format("field3:%d", docid))
              .putBytesField("field4", String.format("field4:%d", docid)
                      .getBytes(StandardCharsets.UTF_8))
              .build();
      _writer.upsertFieldValues(document);
    }

    _writer.dropFieldsByName(ImmutableList.of("field3", PRIMARY_KEY_FIELD_NAME));

    Thread.sleep(5000);

    for (int docid = startDocId; docid < endDocId; docid++) {
      String primaryKey = String.format("pkey:%d", docid);

      String fetchedPrimaryKey = _reader.getStringValue(primaryKey, PRIMARY_KEY_FIELD_NAME);
      Assertions.assertEquals(fetchedPrimaryKey, primaryKey);

      Assertions.assertNull(_reader.getStringValue(primaryKey, "field3"));

      byte[] fetchedField4 = _reader.getBytesValue(primaryKey, "field4");
      Assertions.assertArrayEquals(fetchedField4, String.format("field4:%d", docid).getBytes(StandardCharsets.UTF_8));
    }

    // delete all documents
    _writer.dropAllDocuments();
    Thread.sleep(5000);

    for (int docid = startDocId; docid < endDocId; docid++) {
      String primaryKey = String.format("pkey:%d", docid);
      String fetchedPrimaryKey = _reader.getStringValue(primaryKey, PRIMARY_KEY_FIELD_NAME);
      Assertions.assertNull(fetchedPrimaryKey);
    }
  }

  // @Test
  public void newStoreProvisioning() throws InterruptedException {
    // Upsert document
    IKVDocument document =
        new IKVDocument.Builder()
            .putStringField(PRIMARY_KEY_FIELD_NAME, "testing_primary_key")
            .build();
    _writer.upsertFieldValues(document);
    Thread.sleep(1000);

    String value = _reader.getStringValue("testing_primary_key", PRIMARY_KEY_FIELD_NAME);
    Assertions.assertEquals(value, "testing_primary_key");

    // Delete document
    _writer.deleteDocument(document);
    Thread.sleep(1000);
    Assertions.assertNull(_reader.getStringValue("testing_primary_key", PRIMARY_KEY_FIELD_NAME));
  }
}
