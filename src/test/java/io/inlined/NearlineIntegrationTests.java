package io.inlined;

import io.inlined.clients.*;
import org.junit.jupiter.api.*;

@Disabled
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
  public void upsertAndRead() throws InterruptedException {
    IKVDocument document =
        new IKVDocument.Builder()
            .putStringField("userid", "id_2") // primary key
            .putIntField("age", 25)
            .putLongField("ageAsLong", 25)
            .putFloatField("ageAsFloat", 25.2f)
            .putDoubleField("ageAsDouble", 25.2)
            .putStringField("firstname", "Alice")
            .build();
    _writer.upsertFieldValues(document);

    Thread.sleep(1000);

    String userid = _reader.getStringValue("id_2", "userid");
    Assertions.assertEquals(userid, "id_2");

    Assertions.assertEquals(_reader.getIntValue("id_2", "age"), 25);
    Assertions.assertEquals(_reader.getLongValue("id_2", "ageAsLong"), 25);
    Assertions.assertEquals(_reader.getFloatValue("id_2", "ageAsFloat"), 25.2f);
    Assertions.assertEquals(_reader.getDoubleValue("id_2", "ageAsDouble"), 25.2);

    String firstName = _reader.getStringValue("id_2", "firstname");
    Assertions.assertEquals(firstName, "Alice");

    // cleanup doc
    _writer.deleteDocument(document);
    Thread.sleep(1000);
  }

  @Test
  public void newStoreProvisioning() throws InterruptedException {
    // Upsert document
    IKVDocument document =
        new IKVDocument.Builder()
            .putStringField(PRIMARY_KEY_FIELD_NAME, "testing_primary_key")
            .build();
    _writer.upsertFieldValues(document);
    Thread.sleep(5000);

    String value = _reader.getStringValue("testing_primary_key", PRIMARY_KEY_FIELD_NAME);
    Assertions.assertEquals(value, "testing_primary_key");

    // Delete document
    _writer.deleteDocument(document);
    Thread.sleep(5000);
    Assertions.assertNull(_reader.getStringValue("testing_primary_key", PRIMARY_KEY_FIELD_NAME));
  }
}
