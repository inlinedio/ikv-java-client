package io.inlined.clients;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.inlineio.schemas.Common.FieldType;
import com.inlineio.schemas.Common.IKVStoreConfig;
import com.inlineio.schemas.Services;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

public final class ClientOptions {
  private final IKVStoreConfig _config;
  private final FieldType _primaryKeyType;

  private ClientOptions(IKVStoreConfig config, @Nullable FieldType primaryKeyType) {
    _config = Objects.requireNonNull(config);

    Preconditions.checkArgument(primaryKeyType != FieldType.UNKNOWN);
    _primaryKeyType = primaryKeyType;
  }

  public IKVStoreConfig asIKVStoreConfig() {
    return _config;
  }

  // TODO: deprecate - key type should not be an option
  @Nullable
  public FieldType primaryKeyType() {
    return _primaryKeyType;
  }

  public Optional<String> mountDirectory() {
    @Nullable
    String mountDirectory = _config.getStringConfigsOrDefault(IKVConstants.MOUNT_DIRECTORY, null);
    return Optional.ofNullable(mountDirectory);
  }

  public Services.UserStoreContextInitializer createUserStoreContextInitializer() {
    return Services.UserStoreContextInitializer.newBuilder()
        .setStoreName(_config.getStringConfigsOrThrow(IKVConstants.STORE_NAME))
        .setCredentials(
            Services.AccountCredentials.newBuilder()
                .setAccountId(_config.getStringConfigsOrThrow(IKVConstants.ACCOUNT_ID))
                .setAccountPasskey(_config.getStringConfigsOrThrow(IKVConstants.ACCOUNT_PASSKEY))
                .build())
        .build();
  }

  public static final class Builder {
    private static final Set<String> LOG_LEVELS =
        ImmutableSet.of("error", "warn", "info", "debug", "trace");

    private final IKVStoreConfig.Builder _configBuilder;
    private FieldType _primaryKeyType;

    public Builder() {
      _configBuilder = IKVStoreConfig.newBuilder();

      // defaults
      // TODO: remove after partitioning support
      _configBuilder.putIntConfigs(IKVConstants.PARTITION, 0);
      _configBuilder.putStringConfigs(IKVConstants.RUST_CLIENT_LOG_LEVEL, "info");
      _configBuilder.putBooleanConfigs(IKVConstants.RUST_CLIENT_LOG_TO_CONSOLE, true);
    }

    public ClientOptions build() {
      // enforce required configs
      _configBuilder.getStringConfigsOrThrow(IKVConstants.ACCOUNT_ID);
      _configBuilder.getStringConfigsOrThrow(IKVConstants.ACCOUNT_PASSKEY);
      _configBuilder.getStringConfigsOrThrow(IKVConstants.STORE_NAME);

      return new ClientOptions(_configBuilder.build(), _primaryKeyType);
    }

    public Builder useStringPrimaryKey() {
      // TODO: inspect how to pass in primary key field name
      _primaryKeyType = FieldType.STRING;
      return this;
    }

    public Builder useBytesPrimaryKey() {
      _primaryKeyType = FieldType.BYTES;
      return this;
    }

    public Builder withMountDirectory(String mountDirectory) {
      // user specified mount path
      Preconditions.checkArgument(mountDirectory != null && !mountDirectory.isEmpty());
      _configBuilder.putStringConfigs(IKVConstants.MOUNT_DIRECTORY, mountDirectory);
      return this;
    }

    public Builder withStoreName(String storeName) {
      Preconditions.checkArgument(storeName != null && !storeName.isEmpty());
      _configBuilder.putStringConfigs(IKVConstants.STORE_NAME, storeName);
      return this;
    }

    // TODO: allow partition to be injected
    private Builder withStorePartition(int partition) {
      throw new UnsupportedOperationException();
    }

    public Builder withAccountId(String accountId) {
      Preconditions.checkArgument(accountId != null && !accountId.isEmpty());
      _configBuilder.putStringConfigs(IKVConstants.ACCOUNT_ID, accountId);
      return this;
    }

    public Builder withAccountPassKey(String accountPassKey) {
      Preconditions.checkArgument(accountPassKey != null && !accountPassKey.isEmpty());
      _configBuilder.putStringConfigs(IKVConstants.ACCOUNT_PASSKEY, accountPassKey);
      return this;
    }

    // Ex. withKafkaPropertyOverride("ssl.ca.location", "/etc/ssl/certs")
    // is required on Ubuntu hosts to declare CA certificates.
    public Builder withKafkaPropertyOverride(String key, String value) {
      Preconditions.checkArgument(!key.isEmpty());
      Preconditions.checkArgument(!value.isEmpty());
      _configBuilder.putStringConfigs("kafkaprop_" + key, value);
      return this;
    }

    public Builder withConsoleLogging(String level) {
      Preconditions.checkArgument(level != null && !level.isEmpty());
      Preconditions.checkArgument(
          LOG_LEVELS.contains(level.toLowerCase()), "log-level should be one of: {}", LOG_LEVELS);
      _configBuilder.putStringConfigs(IKVConstants.RUST_CLIENT_LOG_LEVEL, level.toLowerCase());
      _configBuilder.putBooleanConfigs(IKVConstants.RUST_CLIENT_LOG_TO_CONSOLE, true);
      return this;
    }

    public Builder withFileLogging(String filePath, String level) {
      Preconditions.checkArgument(filePath != null && !filePath.isEmpty());
      Preconditions.checkArgument(level != null && !level.isEmpty());
      Preconditions.checkArgument(
          LOG_LEVELS.contains(level.toLowerCase()), "log-level should be one of: {}", LOG_LEVELS);
      _configBuilder.putStringConfigs(IKVConstants.RUST_CLIENT_LOG_LEVEL, level.toLowerCase());
      _configBuilder.putBooleanConfigs(IKVConstants.RUST_CLIENT_LOG_TO_CONSOLE, false);
      _configBuilder.putStringConfigs(IKVConstants.RUST_CLIENT_LOG_FILE, filePath);
      return this;
    }
  }
}
