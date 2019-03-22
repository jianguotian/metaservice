package com.mservice.example.controller.metastore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicInteger;

public class MetastoreClient {
  private ThriftHiveMetastore.Iface client;

  public ThriftHiveMetastore.Iface open() throws MetaException {
    boolean isConnected = false;
//    final HiveConf conf;
    Log LOG = LogFactory.getLog("hive.metastore");
    TTransport transport = null;
    final AtomicInteger connCount = new AtomicInteger(0);
    int retries = 5;
    long retryDelaySeconds = 0;
    URI metastoreUris[] = new URI[1];
    try {
      metastoreUris[0] = new URI("thrift://localhost:9083");
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }

    TTransportException tte = null;
    int clientSocketTimeout = 600;

    for (int attempt = 0; !isConnected && attempt < retries; ++attempt) {
      for (URI store : metastoreUris) {
        LOG.info("Trying to connect to metastore with URI " + store);

        transport = new TSocket(store.getHost(), store.getPort(), clientSocketTimeout);

        final TProtocol protocol;
        protocol = new TBinaryProtocol(transport);
        client = new ThriftHiveMetastore.Client(protocol);
        try {
          if (!transport.isOpen()) {
            transport.open();
            LOG.info("Opened a connection to metastore, current connections: " + connCount.incrementAndGet());
          }
          isConnected = true;
        } catch (TTransportException e) {
          tte = e;
          if (LOG.isDebugEnabled()) {
            LOG.warn("Failed to connect to the MetaStore Server...", e);
          } else {
            // Don't print full exception trace if DEBUG is not on.
            LOG.warn("Failed to connect to the MetaStore Server...");
          }
        }

        if (isConnected) {
          break;
        }
      }
      // Wait before launching the next round of connection retries.
      if (!isConnected && retryDelaySeconds > 0) {
        try {
          LOG.info("Waiting " + retryDelaySeconds + " seconds before next connection attempt.");
          Thread.sleep(retryDelaySeconds * 1000);
        } catch (InterruptedException ignore) {
        }
      }
    }

    if (!isConnected) {
      throw new MetaException("Could not connect to meta store using any of the URIs provided." +
              " Most recent failure: " + StringUtils.stringifyException(tte));
    }

//    snapshotActiveConf();

    LOG.info("Connected to metastore.");

    return client;
  }
}