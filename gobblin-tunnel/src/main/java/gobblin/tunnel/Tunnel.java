package gobblin.tunnel;
/*
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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import org.slf4j.LoggerFactory;

import static java.nio.channels.SelectionKey.OP_READ;


/**
 * @author navteniev@linkedin.com
 *
 * Implements a tunnel through a proxy to resource on the internet
 */
public class Tunnel {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Tunnel.class);

  private final String _remoteHost;
  private final int _remotePort;
  private final String _proxyHost;
  private final int _proxyPort;
  private ServerSocketChannel _server;
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
      startTunnelThread();

      return Optional.of(this);
    } catch (IOException ioe) {
      LOG.error("Failed to open the tunnel", ioe);
    }

    return Optional.empty();
  }

  public int getPort() {
    SocketAddress localAddress = null;
    try {
      if (_server != null && _server.isOpen()) {
        localAddress = _server.getLocalAddress();
      }
      if (localAddress instanceof InetSocketAddress) {
        return ((InetSocketAddress) localAddress).getPort();
      }
    } catch (IOException e) {
      LOG.error("Failed to get tunnel port", e);
    }

    return -1;
  }

  private void startTunnelThread() {
    _thread = new Thread(new Dispatcher(), "Tunnel Listener");
    _thread.start();
  }

  private class Dispatcher implements Runnable {

    private Selector _selector;

    public Dispatcher() {
      try {
        _selector = Selector.open();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void run() {
      try {
        _server.configureBlocking(false);
        _server.register(_selector, SelectionKey.OP_ACCEPT);

        while (!Thread.interrupted()) {

          _selector.select();
          Set<SelectionKey> selectionKeys = _selector.selectedKeys();

          for (SelectionKey selectionKey : selectionKeys) {
            dispatch(selectionKey);
          }
          selectionKeys.clear();
        }
      } catch (IOException ioe) {
        LOG.error("Unhandled exception.  Tunnel will close", ioe);
      }

      LOG.info("Closing tunnel");
    }

    private void dispatch(SelectionKey selectionKey)
        throws IOException {
      if (selectionKey.isAcceptable()) {
        acceptNewConnection(selectionKey);
      } else if (selectionKey.isReadable() || selectionKey.isWritable()) {
        ReadWriteHandler handler = (ReadWriteHandler) selectionKey.attachment();
        try {
          handler.call();
        } catch (Exception e) {
          LOG.warn("Failed to handle read/write event", e);
        }
      }
    }

    private void acceptNewConnection(SelectionKey selectionKey) {
      SocketChannel client = null;

      try {
        client = ((ServerSocketChannel) selectionKey.channel()).accept();

        LOG.info("Accepted connection from {}", client);

        SocketChannel proxy = connect();
        new ReadWriteHandler(proxy, client, _selector);
      } catch (IOException io) {
        if (client == null) {
          LOG.warn("Failed to accept connection from client", io);
        } else if (client.isOpen()) {
          LOG.warn(String.format("Failed to connect to proxy dropping connection from %s", client), io);
          try {
            client.close();
          } catch (IOException ignore) {
          }
        }
      }
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
        LOG.warn("failed to close connection to proxy", e);
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

  private SocketChannel requestConnectionToRemoteHost(String host, int port, String proxyHost, int proxyPort)
      throws IOException {
    final SocketChannel proxyChannel = SocketChannel.open();

    if (proxyChannel == null) {
      throw new IOException("unable to connect to " + host + ":" + port);
    }

    try {
      proxyChannel.socket().setTcpNoDelay(true);
      proxyChannel.socket().connect(new InetSocketAddress(proxyHost, proxyPort), 5000);

      final ByteBuffer connect = ByteBuffer.wrap(String
          .format("CONNECT %s:%s HTTP/1.1%nUser-Agent: GaaP%nConnection: keep-alive%nHost:%s%n%n", host, port, host)
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
    try {
      _thread.interrupt();
      _thread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        _server.close();
      } catch (IOException ioe) {
        LOG.warn("Failed to shutdown tunnel", ioe);
      }
    }
  }

  /**
   * This class is not thread safe
   */
  final class ReadWriteHandler implements Callable<Boolean> {
    private final SocketChannel _proxy;
    private final SocketChannel _client;
    private final Selector _selector;
    private final ByteBuffer _buffer = ByteBuffer.allocate(1000000);
    private boolean reading = true;

    ReadWriteHandler(SocketChannel proxy, SocketChannel client, Selector selector)
        throws IOException {
      _proxy = proxy;
      _client = client;
      _selector = selector;

      _proxy.configureBlocking(false);
      _client.configureBlocking(false);

      _proxy.register(_selector, OP_READ, this);
      _client.register(_selector, OP_READ, this);
    }

    @Override
    public Boolean call()
        throws Exception {

      try {
        if (reading) {
          read();
        } else {
          write();
        }
      } catch (IOException ioe) {
        closeChannels();
        throw new IOException(String.format("Could not read/write between %s and %s", _proxy, _client), ioe);
      }

      return reading;
    }

    private void write()
        throws IOException {
      SelectionKey proxyKey = _proxy.keyFor(_selector);
      SelectionKey clientKey = _client.keyFor(_selector);

      SocketChannel writeChannel = null;
      SocketChannel readChannel = null;
      SelectionKey writeKey = null;

      if (_selector.selectedKeys().contains(proxyKey) && proxyKey.isWritable()) {
        writeChannel = _proxy;
        readChannel = _client;
        writeKey = proxyKey;
      } else if (_selector.selectedKeys().contains(clientKey) && clientKey.isWritable()) {
        writeChannel = _client;
        readChannel = _proxy;
        writeKey = clientKey;
      }

      if (writeKey != null) {
        int lastWrite, totalWrite = 0;

        _buffer.flip();

        int available = _buffer.remaining();

        while ((lastWrite = writeChannel.write(_buffer)) > 0) {
          totalWrite += lastWrite;
        }

        if (totalWrite == available) {
          _buffer.clear();
          if(readChannel.isOpen()) {
            readChannel.register(_selector, SelectionKey.OP_READ, this);
            writeChannel.register(_selector, SelectionKey.OP_READ, this);
          }
          else{
            writeChannel.close();
          }
          reading = true;
        } else {
          _buffer.compact();
        }
        if (lastWrite == -1) {
          closeChannels();
        }
      }
    }

    private void read()
        throws IOException {
      SelectionKey proxyKey = _proxy.keyFor(_selector);
      SelectionKey clientKey = _client.keyFor(_selector);

      SocketChannel readChannel = null;
      SocketChannel writeChannel = null;
      SelectionKey readKey = null;

      if (_selector.selectedKeys().contains(proxyKey) && proxyKey.isReadable()) {
        readChannel = _proxy;
        writeChannel = _client;
        readKey = proxyKey;
      } else if (_selector.selectedKeys().contains(clientKey) && clientKey.isReadable()) {
        readChannel = _client;
        writeChannel = _proxy;
        readKey = clientKey;
      }

      if (readKey != null) {

        int lastRead, totalRead = 0;

        while ((lastRead = readChannel.read(_buffer)) > 0) {
          totalRead += lastRead;
        }

        if (totalRead > 0) {
          readKey.cancel();
          writeChannel.register(_selector, SelectionKey.OP_WRITE, this);
          reading = false;
        }
        if (lastRead == -1) {
          readChannel.close();
        }
      }
    }

    private void closeChannels() {
      if (_proxy.isOpen()) {
        try {
          _proxy.close();
        } catch (IOException log) {
          //log
        }
      }

      if (_client.isOpen()) {
        try {
          _client.close();
        } catch (IOException log) {
          //log
        }
      }
    }
  }

  public static Optional<Tunnel> build(String remoteHost, int remotePort, String proxyHost, int proxyPort) {
    return new Tunnel(remoteHost, remotePort, proxyHost, proxyPort).open();
  }
}
