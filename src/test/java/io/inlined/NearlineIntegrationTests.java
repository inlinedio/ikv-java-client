package io.inlined;

import io.inlined.clients.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class NearlineIntegrationTests {

  @Test
  @Disabled
  public void upsertAndRead() throws InterruptedException {
    String accountId = System.getenv("IKV_ACCOUNT_ID");
    String storeName = System.getenv("IKV_STORE_NAME");
    String accountPasskey = System.getenv("IKV_ACCOUNT_PASSKEY");

    ClientOptions clientOptions =
        new ClientOptions.Builder()
            .withMountDirectory("/tmp/UpsertAndRead")
            .withStoreName(storeName)
            .withAccountId(accountId)
            .withAccountPassKey(accountPasskey)
            .useStringPrimaryKey()
            .build();

    IKVClientFactory factory = new IKVClientFactory();

    InlineKVWriter writer = factory.createNewWriterInstance(clientOptions);

    writer.startupWriter();

    IKVDocument document =
        new IKVDocument.Builder()
            .putStringField("userid", "id_2") // primary key
            .putIntField("age", 25)
            .putLongField("ageAsLong", 25)
            .putFloatField("ageAsFloat", 25.2f)
            .putDoubleField("ageAsDouble", 25.2)
            .putStringField("firstname", "Alice")
            .build();
    writer.upsertFieldValues(document);

    Thread.sleep(1000);

    InlineKVReader reader = factory.createNewReaderInstance(clientOptions);
    reader.startupReader();

    String userid = reader.getStringValue("id_2", "userid");
    Assertions.assertEquals(userid, "id_2");

    Assertions.assertEquals(reader.getIntValue("id_2", "age"), 25);
    Assertions.assertEquals(reader.getLongValue("id_2", "ageAsLong"), 25);
    Assertions.assertEquals(reader.getFloatValue("id_2", "ageAsFloat"), 25.2f);
    Assertions.assertEquals(reader.getDoubleValue("id_2", "ageAsDouble"), 25.2);

    String firstName = reader.getStringValue("id_2", "firstname");
    Assertions.assertEquals(firstName, "Alice");

    reader.shutdownReader();
  }

  @Test
  @Disabled
  public void newStoreProvisioning() throws InterruptedException {
    String accountId = System.getenv("IKV_ACCOUNT_ID");
    String storeName = System.getenv("IKV_STORE_NAME");
    String accountPasskey = System.getenv("IKV_ACCOUNT_PASSKEY");
    String primaryKeyFieldName = System.getenv("IKV_PRIMARY_KEY");

    ClientOptions clientOptions =
        new ClientOptions.Builder()
            .withMountDirectory("/tmp/NewStoreProvisioning")
            .withStoreName(storeName)
            .withAccountId(accountId)
            .withAccountPassKey(accountPasskey)
            // WARNING! change based on usecase
            .useStringPrimaryKey()
            .build();

    IKVClientFactory factory = new IKVClientFactory();

    InlineKVWriter writer = factory.createNewWriterInstance(clientOptions);

    writer.startupWriter();

    // Upsert document
    IKVDocument document =
        new IKVDocument.Builder()
            .putStringField(primaryKeyFieldName, "testing_primary_key")
            .build();
    writer.upsertFieldValues(document);
    Thread.sleep(5000);

    InlineKVReader reader = factory.createNewReaderInstance(clientOptions);
    reader.startupReader();

    String value = reader.getStringValue("testing_primary_key", primaryKeyFieldName);
    Assertions.assertEquals(value, "testing_primary_key");

    // Delete document
    writer.deleteDocument(document);
    Thread.sleep(5000);
    Assertions.assertNull(reader.getStringValue("testing_primary_key", primaryKeyFieldName));

    reader.shutdownReader();
  }
}
