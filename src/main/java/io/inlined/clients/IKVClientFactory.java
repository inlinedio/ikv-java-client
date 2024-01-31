package io.inlined.clients;

import com.google.common.annotations.VisibleForTesting;
import com.inlineio.schemas.Common;

public class IKVClientFactory {

  public IKVClientFactory() {}

  public InlineKVReader createNewReaderInstance(ClientOptions clientOptions) {
    // TODO: remove server side config fetching
    ServerSuppliedConfigFetcher fetcher = new ServerSuppliedConfigFetcher(clientOptions);
    Common.IKVStoreConfig serverConfig = fetcher.fetchServerConfig();
    Common.IKVStoreConfig clientSuppliedConfig = clientOptions.asIKVStoreConfig();
    Common.IKVStoreConfig mergedConfig = mergeConfigs(clientSuppliedConfig, serverConfig);

    return new DefaultInlineKVReader(clientOptions, mergedConfig);
  }

  @VisibleForTesting
  public static Common.IKVStoreConfig mergeConfigs(
      Common.IKVStoreConfig clientCfg, Common.IKVStoreConfig serverCfg) {
    return Common.IKVStoreConfig.newBuilder().mergeFrom(serverCfg).mergeFrom(clientCfg).build();
  }

  public InlineKVWriter createNewWriterInstance(ClientOptions clientOptions) {
    return new DefaultInlineKVWriter(clientOptions);
  }
}
