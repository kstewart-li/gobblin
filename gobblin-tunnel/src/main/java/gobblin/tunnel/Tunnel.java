package gobblin.tunnel;/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Optional;


/**
 * @author navteniev@linkedin.com
 *
 * Implements a tunnel through a proxy to resource on the internet
 */
public class Tunnel {

  private final String _remoteHost;
  private final int _remotePort;
  private final String _proxyHost;
  private final int _proxyPort;
  private ServerSocketChannel _server;
  private volatile boolean running = true;

  private Tunnel(String remoteHost, int remotePort, String proxyHost, int proxyPort) {
    _remoteHost = remoteHost;
    _remotePort = remotePort;
    _proxyHost = proxyHost;
    _proxyPort = proxyPort;
  }

  private Optional<Tunnel> open() {
    try {
      _server = ServerSocketChannel.open().bind(null);
      listen();

      return Optional.of(this);
    } catch (IOException e) {

    }

    return Optional.empty();
  }

  public int getPort() {
    SocketAddress localAddress = null;
    try {
      localAddress = _server.getLocalAddress();
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (localAddress instanceof InetSocketAddress) {
      return ((InetSocketAddress) localAddress).getPort();
    }

    return -1;
  }

  private void listen() {
    Thread thread = new Thread("Tunnel Listener");
  }

  private class Listener implements Runnable {

    @Override
    public void run() {

      try {
        while (running) {
          SocketChannel client = _server.accept();
        }
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        try {
          _server.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      //connect to remote via proxy and setup a relay
    }
  }

  public void close() {
    running = false;
  }

  public static Optional<Tunnel> build(String remoteHost, int remotePort, String proxyHost, int proxyPort) {
    return new Tunnel(remoteHost, remotePort, proxyHost, proxyPort).open();
  }
}
