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
import java.nio.ByteBuffer;
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
  public static final int PROXY_CONNECT_TIMEOUT = 1000;

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
        LOG.error("Could not open selector",e);
      }
    }

    @Override
    public void run() {
      try {
        new AcceptHandler(_server,_selector);

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
      Callable<?> attachment = (Callable<?>) selectionKey.attachment();
      try {
        attachment.call();
      }catch (Exception e){
        LOG.warn("exception handling event on {}",selectionKey.channel(), e);
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

  enum HandlerState {
    ACCEPTING,
    CONNECTING,
    READING,
    WRITING
  }

  final class AcceptHandler implements Callable<HandlerState> {

    private final ServerSocketChannel _server;
    private final Selector _selector;

    AcceptHandler(ServerSocketChannel server, Selector selector)
        throws IOException {

      _server = server;
      _selector = selector;

      _server.configureBlocking(false);
      _server.register(selector, SelectionKey.OP_ACCEPT, this);
    }

    @Override
    public HandlerState call()
        throws Exception {

      SocketChannel client = _server.accept();

      LOG.info("Accepted connection from {}", client.getRemoteAddress());
      try{
        new ProxySetupHandler(client, _selector);
      } catch (IOException ioe){
        client.close();
      }

      return HandlerState.ACCEPTING;
    }
  }


  final class ProxySetupHandler implements  Callable<HandlerState> {
    private final SocketChannel _client;
    private final Selector _selector;
    private final SocketChannel _proxy;
    private HandlerState _state = HandlerState.CONNECTING;
    private ByteBuffer _buffer = ByteBuffer.wrap(String
        .format("CONNECT %s:%s HTTP/1.1%nUser-Agent: GaaP%nConnection: keep-alive%nHost:%s%n%n", _remoteHost, _remotePort, _remoteHost)
        .getBytes());
    private final long _connectStartTime;

    ProxySetupHandler(SocketChannel client, Selector selector)
        throws IOException {

      _client = client;
      _selector = selector;

      //Blocking call
      _proxy = SocketChannel.open();
      _proxy.configureBlocking(false);
      _connectStartTime = System.currentTimeMillis();
      boolean connected = _proxy.connect(new InetSocketAddress(_proxyHost, _proxyPort));

      if(!connected) {
        _client.configureBlocking(false);
        _client.register(_selector, SelectionKey.OP_READ, this);
        _proxy.register(_selector, SelectionKey.OP_CONNECT, this);
      } else {
        _state = HandlerState.WRITING;
        _proxy.register(_selector, SelectionKey.OP_WRITE, this);
      }
    }

    @Override
    public HandlerState call()
        throws Exception {

      try {
        switch (_state) {
          case CONNECTING:
            connect();
            break;
          case WRITING:
            write();
            break;
          case READING:
            read();
            break;
        }
      }catch (IOException ioe){
        LOG.warn("Failed to establish a proxy connection for {}", _client.getRemoteAddress(), ioe);
        closeChannels();
      }

      return _state;
    }

    private void connect() throws IOException{
      if(_proxy.isOpen()) {

        if (_proxy.finishConnect()) {
          _proxy.register(_selector, SelectionKey.OP_WRITE, this);
          SelectionKey clientKey = _client.keyFor(_selector);
          if (clientKey != null) {
            clientKey.cancel();
          }
          _state = HandlerState.WRITING;
        } else if (_connectStartTime + PROXY_CONNECT_TIMEOUT < System.currentTimeMillis()) {
          LOG.warn("Proxy connect timed out for client {}", _client);
          closeChannels();
        }
      }
    }

    private void write() throws IOException{
      while(_proxy.write(_buffer)>0){
      }

      if(_buffer.remaining() == 0){
        _proxy.register(_selector,SelectionKey.OP_READ,this);
        _state = HandlerState.READING;
        _buffer = ByteBuffer.allocate(1000);
      }
    }

    private void read() throws IOException {
      int lastBytes, totalBytes = 0;

      while ((lastBytes = _proxy.read(_buffer))>0){
        totalBytes +=lastBytes;
      }

      if(totalBytes >= OK_REPLY.limit()){
        _buffer.flip();
        _buffer.limit(OK_REPLY.limit());
        if(OK_REPLIES.contains(_buffer)){
          _proxy.keyFor(_selector).cancel();
          _proxy.configureBlocking(true);
          drainChannel(_proxy);
          _state = null;

          new ReadWriteHandler(_proxy,_client,_selector);
        }
        else{
          _proxy.close();
          _client.close();
        }
      }
    }

    private void drainChannel(SocketChannel socketChannel)
        throws IOException {
      if (socketChannel.socket().getInputStream().available() > 0) {
        while (socketChannel.socket().getInputStream().available() > 0) {
          socketChannel.read(_buffer);
          _buffer.clear();
        }
      }
    }

    private void closeChannels() {
      if (_proxy.isOpen()) {
        try {
          _proxy.close();
        } catch (IOException log) {
          LOG.warn("Failed to close proxy channel {}",_proxy,log);
        }
      }

      if (_client.isOpen()) {
        try {
          _client.close();
        } catch (IOException log) {
          LOG.warn("Failed to close client channel {}",_client,log);
        }
      }
    }
  }

  /**
   * This class is not thread safe
   */
  final class ReadWriteHandler implements Callable<HandlerState> {
    private final SocketChannel _proxy;
    private final SocketChannel _client;
    private final Selector _selector;
    private final ByteBuffer _buffer = ByteBuffer.allocate(1000000);
    private HandlerState _state = HandlerState.READING;

    ReadWriteHandler(SocketChannel proxy, SocketChannel client, Selector selector)
        throws IOException {
      _proxy = proxy;
      _client = client;
      _selector = selector;

      _proxy.configureBlocking(false);
      _client.configureBlocking(false);

      _client.register(_selector, OP_READ, this);
    }

    @Override
    public HandlerState call()
        throws Exception {

      try {
        switch (_state){
          case READING:
            read();
            break;
          case WRITING:
            write();
            break;
        }
      } catch (IOException ioe) {
        closeChannels();
        throw new IOException(String.format("Could not read/write between %s and %s", _proxy, _client), ioe);
      }

      return _state;
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

        LOG.info("{} bytes written to {}", totalWrite, writeChannel == _proxy ? "proxy" : "client");

        if (totalWrite == available) {
          _buffer.clear();
          if(readChannel.isOpen()) {
            readChannel.register(_selector, SelectionKey.OP_READ, this);
            writeChannel.register(_selector, SelectionKey.OP_READ, this);
          }
          else{
            writeChannel.close();
          }
          _state = HandlerState.READING;
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

        LOG.info("{} bytes read from {}", totalRead, readChannel == _proxy ? "proxy":"client");

        if (totalRead > 0) {
          readKey.cancel();
          writeChannel.register(_selector, SelectionKey.OP_WRITE, this);
          _state = HandlerState.WRITING;
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
          LOG.warn("Failed to close proxy channel {}",_proxy,log);
        }
      }

      if (_client.isOpen()) {
        try {
          _client.close();
        } catch (IOException log) {
          LOG.warn("Failed to close client channel {}",_client,log);
        }
      }
    }
  }

  public static Optional<Tunnel> build(String remoteHost, int remotePort, String proxyHost, int proxyPort) {
    return new Tunnel(remoteHost, remotePort, proxyHost, proxyPort).open();
  }
}
