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

import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Optional;
import java.util.Set;


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
  private volatile boolean _running = true;
  private Thread _thread;
  private static final ByteBuffer OK_REPLY = ByteBuffer.wrap("HTTP/1.1 200".getBytes());
  private static final Set<ByteBuffer> OK_REPLIES =
      Sets.newHashSet(OK_REPLY, ByteBuffer.wrap("HTTP/1.0 200".getBytes()));

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
    _thread = new Thread(new Listener(), "Tunnel Listener");
    _thread.start();
  }

  private class Listener implements Runnable {

    @Override
    public void run() {

      System.out.println("_running = " + _running);

      try {
        while (_running) {
          SocketChannel client = _server.accept();

          System.out.println("client.getLocalAddress() = " + client.getLocalAddress());
          SocketChannel proxy = connect();

          System.out.println("connected = " + proxy);

          ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1000000);

          int bytesRead = 0;
          while((bytesRead = client.read(byteBuffer)) > 0){
            System.out.println("read bytes " + bytesRead);
            byteBuffer.flip();
            int wroteBytes = 0;
            while((wroteBytes=proxy.write(byteBuffer)) > 0){
              System.out.println("wrote bytes "+wroteBytes);
            }
          }

          byteBuffer.clear();
          while(proxy.read(byteBuffer) > 0){
            System.out.println("read from proxy");
            byteBuffer.flip();
            while (client.write(byteBuffer) > 0){
              System.out.println("wrote to client");
            }
          }


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

      System.out.println("_running = " + _running);
      //connect to remote via proxy and setup a relay
    }
  }


  protected SocketChannel connect()
      throws IOException {
   final SocketChannel proxyChannel = requestConnectionToRemoteHost(_remoteHost, _remotePort, _proxyHost, _proxyPort);
    final ByteBuffer statusLine = readVersionAndStatus(proxyChannel);

    if (!OK_REPLIES.contains(statusLine)) {
      System.out.println(String.format("Failed to connect to proxy server.  Response: %n%s",
          new String(statusLine.array(), 0, OK_REPLY.limit())));

      try {
        proxyChannel.close();
      } catch (IOException e) {
        System.out.println(e);
      }

      throw new IOException(String.format("Failed to connect to proxy %s:%n%s", _remoteHost, _remotePort,
          new String(statusLine.array(), 0, OK_REPLY.limit())));
    }

    drainChannel(proxyChannel);

    return proxyChannel;
  }

  private ByteBuffer readVersionAndStatus(SocketChannel channel)
      throws IOException {
    final ByteBuffer reply = ByteBuffer.allocate(1024);

    int soTimeout = channel.socket().getSoTimeout();
    ReadableByteChannel channelWithTimeout = createChannelWithReadTimeout(channel, 5000);

    try {
      int read = channelWithTimeout.read(reply);
      while (read > 0 && reply.position() < OK_REPLY.limit()) {
        read = channelWithTimeout.read(reply);
      }
    } catch (SocketTimeoutException ste) {
      System.out.println("Read from squid proxy timed out" + ste);
    } finally {
      channel.socket().setSoTimeout(soTimeout);
    }

    final ByteBuffer statusLine = reply.duplicate();
    statusLine.flip();
    statusLine.limit(OK_REPLY.limit());
    return statusLine;
  }

  /**
   * The socket channel does not respect the socket level soTimeout setting.  This method returns a channel which
   * does respect the socket lever soTimeout setting
   *
   * @param channel whose socket will be read from
   * @param readTimeout the timeout that the read operation will respect
   * @return a channel which honors the read time out
   * @throws IOException if an error occurs when accessing the socket input channel
   */
  private ReadableByteChannel createChannelWithReadTimeout(SocketChannel channel, long readTimeout)
      throws IOException {
    channel.socket().setSoTimeout((int) readTimeout);
    return Channels.newChannel(channel.socket().getInputStream());
  }

  private SocketChannel requestConnectionToRemoteHost(String host, int port,
      String proxyHost, int proxyPort)
      throws IOException {
    final SocketChannel proxyChannel = SocketChannel.open();

    if (proxyChannel == null) {
      throw new IOException("unable to connect to " + host + ":" + port);
    }

    try {
      proxyChannel.socket().setTcpNoDelay(true);
      proxyChannel.socket().connect(new InetSocketAddress(proxyHost, proxyPort), 5000);

      final ByteBuffer connect = ByteBuffer.wrap(
          String.format("CONNECT %s:%s HTTP/1.1%nUser-Agent: GaaP%nConnection: keep-alive%nHost:%s%n%n", host, port, host)
              .getBytes());

      while (proxyChannel.write(connect) > 0) {
      }
    } catch (IOException e) {
      try {
        proxyChannel.close();
      } catch (IOException ex) {
        System.out.println(ex);
      }
      throw e;
    }

    return proxyChannel;

  }

  private void drainChannel(SocketChannel socketChannel)
      throws IOException {
    if (socketChannel.socket().getInputStream().available() > 0) {
      final ByteBuffer ignored = ByteBuffer.allocate(1024);
      while (socketChannel.socket().getInputStream().available() > 0) {
        socketChannel.read(ignored);
        ignored.clear();
      }
    }
  }

  public void close() {
    _running = false;

    try {
      _thread.interrupt();
      _thread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static Optional<Tunnel> build(String remoteHost, int remotePort, String proxyHost, int proxyPort) {
    return new Tunnel(remoteHost, remotePort, proxyHost, proxyPort).open();
  }
}
