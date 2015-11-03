package gobblin.tunnel;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


@Test
public class TestTunnelWithArbitraryTCPTraffic {
  private static void sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
    }
  }

  private abstract class EasyThread extends Thread {
    EasyThread startThread() {
      start();
      return this;
    }

    @Override
    public void run() {
      try {
        runThread();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    abstract void runThread() throws Exception;
  };


  private abstract class MockServer {
    volatile boolean _serverRunning = true;
    ServerSocket _server;
    Set<EasyThread> _threads = Collections.synchronizedSet(new HashSet<>());
    int _serverSocketPort;

    public MockServer start() throws IOException {
      _server = new ServerSocket();
      _server.setSoTimeout(5000);
      _server.bind(new InetSocketAddress("localhost", 0));
      _serverSocketPort = _server.getLocalPort();
      _threads.add(new EasyThread() {
        @Override
        void runThread() throws Exception {
          runServer();
        }
      }.startThread());
      return this;
    }

    // accept thread
    public void runServer() {
      while (_serverRunning) {
        try {
          final Socket clientSocket = _server.accept();
          //clientSocket.setSoTimeout(5000);
          System.out.println("Accepted connection on " + getServerSocketPort());
          // client handler thread
          _threads.add(new EasyThread() {
            @Override
            void runThread() throws Exception {
              try {
                handleClientSocket(clientSocket);
              } catch (IOException e) {
                onIOException(clientSocket, e);
              }
              _threads.remove(this);
            }
          }.startThread());
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      try {
        _server.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    boolean isServerRunning() {
      return _serverRunning;
    }

    int getServerSocketPort() {
      return _serverSocketPort;
    }

    abstract void handleClientSocket(Socket socket) throws IOException;

    void onIOException(Socket clientSocket, IOException e) {
      stopServer();
    }

    public List<String> getReceivedMessages() {
      return Collections.emptyList();
    }

    public void stopServer() {
      _serverRunning = false;
      for (EasyThread thread : _threads) {
        if (thread.isAlive()) {
          thread.interrupt();
        }
      }
    }
  }

  /**
   * Due to the lack of a suitable embeddable proxy server (the Jetty version here is too old and MockServer's Proxy
   * expects SSL traffic and breaks for arbitrary bytes) we had to write our own mini CONNECT proxy.
   *
   * @param largeResponse Force proxy to send a large response
   */
  private MockServer startConnectProxyServer(final boolean largeResponse) throws IOException {
    return new MockServer() {
      Pattern hostPortPattern = Pattern.compile("Host: (.*):([0-9]+)");
      @Override
      void handleClientSocket(Socket clientSocket) throws IOException {
        final InputStream clientToProxyIn = clientSocket.getInputStream();
        BufferedReader clientToProxyReader = new BufferedReader(new InputStreamReader(clientToProxyIn));
        final OutputStream clientToProxyOut = clientSocket.getOutputStream();
        String line = clientToProxyReader.readLine();
        String connectRequest = "";
        while (line != null && isServerRunning()) {
          connectRequest += line + '\n';
          if (connectRequest.endsWith("\n\n")) {
            break;
          }
          line = clientToProxyReader.readLine();
        }
        // connect to given host:port
        Matcher matcher = hostPortPattern.matcher(connectRequest);
        if (!matcher.find()) {
          try {
            sendConnectResponse("400 Bad Request", clientToProxyOut);
          } finally {
            clientSocket.close();
            stopServer();
          }
          return;
        }
        String host = matcher.group(1);
        int port = Integer.decode(matcher.group(2));

        // connect to server
        Socket serverSocket = new Socket();
        try {
          serverSocket.connect(new InetSocketAddress(host, port));
          sendConnectResponse("200 OK", clientToProxyOut);
        } catch (IOException e) {
          try {
            sendConnectResponse("404 Not Found", clientToProxyOut);
          } finally {
            clientSocket.close();
            stopServer();
          }
          return;
        }
        final InputStream proxyToServerIn = serverSocket.getInputStream();
        final OutputStream proxyToServerOut = serverSocket.getOutputStream();
        _threads.add(new EasyThread() {
          @Override
          void runThread() throws Exception {
            try {
              IOUtils.copy(clientToProxyIn, proxyToServerOut);
            } catch (IOException e) {
              System.out.println("Exception " + e.getMessage() + " on " + getServerSocketPort());
              stopServer();
            }
          }
        }.startThread());
        try {
          IOUtils.copy(proxyToServerIn, clientToProxyOut);
        } catch (IOException e) {
          System.out.println("Exception " + e.getMessage() + " on " + getServerSocketPort());
          stopServer();
        }
        _threads.remove(this);
        clientSocket.close();
        serverSocket.close();
      }

      private void sendConnectResponse(String statusMessage, OutputStream out) throws IOException {
        String extraHeader = "";
        if (largeResponse) {
          // this is to force multiple reads while draining the proxy CONNECT response in Tunnel. Normal proxy responses
          // won't be this big (well, unless you annoy squid proxy, which happens sometimes), but a select() call
          // waking up for multiple reads before a buffer is full is normal
          for (int i = 0; i < 260; i++) {
            extraHeader += "a";
          }
        }
        out.write(("HTTP/1.1 " + statusMessage + "\nContent-Length: 0\nServer: MockProxy" + extraHeader + "\n\n").getBytes());
        out.flush();
      }
    }.start();
  }

  private MockServer startConnectProxyServer() throws IOException {
    return startConnectProxyServer(false);
  }

  private MockServer startDoubleEchoServer() throws IOException {
    return startDoubleEchoServer(0);
  }

  private MockServer startDoubleEchoServer(final long delay) throws IOException {
    return new MockServer() {
      @Override
      void handleClientSocket(Socket clientSocket) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        PrintWriter out = new PrintWriter(clientSocket.getOutputStream());
        String line = in.readLine();
        while (line != null && isServerRunning()) {
          if (delay >  0) {
            sleepQuietly(delay);
          }
          out.println(line + " " + line);
          out.flush();
          line = in.readLine();
        }
        clientSocket.close();
      }
    }.start();
  }

  private String readFromSocket(SocketChannel client) throws IOException {
    ByteBuffer readBuf = ByteBuffer.allocate(256);
    System.out.println("Reading from client");
    client.read(readBuf);
    readBuf.flip();
    return StandardCharsets.US_ASCII.decode(readBuf).toString();
  }

  private void writeToSocket(SocketChannel client, byte [] bytes) throws IOException {
    client.write(ByteBuffer.wrap(bytes));
    client.socket().getOutputStream().flush();
  }

  // Baseline test to ensure clients work without tunnel
  @Test(timeOut = 5000)
  public void testDirectConnectionToEchoServer() throws IOException {
    MockServer doubleEchoServer = startDoubleEchoServer();
    try {
      SocketChannel client = SocketChannel.open();
      client.connect(new InetSocketAddress("localhost", doubleEchoServer.getServerSocketPort()));
      writeToSocket(client, "Knock\n".getBytes());
      String response = readFromSocket(client);
      client.close();
      assertEquals(response, "Knock Knock\n");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      doubleEchoServer.stopServer();
    }
  }

  @Test(timeOut = 5000)
  public void testTunnelToEchoServer() throws IOException {
    MockServer doubleEchoServer = startDoubleEchoServer();
    MockServer proxyServer = startConnectProxyServer();
    System.out.println("Server " + doubleEchoServer.getServerSocketPort() + " Proxy " + proxyServer.getServerSocketPort());
    Optional<Tunnel> tunnel = Tunnel.build("localhost", doubleEchoServer.getServerSocketPort(), "localhost",
        proxyServer.getServerSocketPort());

    try {
      int tunnelPort = tunnel.get().getPort();
      SocketChannel client = SocketChannel.open();

      client.connect(new InetSocketAddress("localhost", tunnelPort));
      client.write(ByteBuffer.wrap("Knock\n".getBytes()));
      String response = readFromSocket(client);
      client.close();

      assertEquals(response, "Knock Knock\n");
    } finally {
      doubleEchoServer.stopServer();
      proxyServer.stopServer();
      tunnel.get().close();
    }
  }


  @Test(timeOut = 5000)
  public void testTunnelToDelayedEchoServer() throws IOException {
    MockServer doubleEchoServer = startDoubleEchoServer(2000);
    MockServer proxyServer = startConnectProxyServer();
    System.out.println("Server " + doubleEchoServer.getServerSocketPort() + " Proxy " + proxyServer.getServerSocketPort());
    Optional<Tunnel> tunnel = Tunnel.build("localhost", doubleEchoServer.getServerSocketPort(), "localhost",
        proxyServer.getServerSocketPort());

    try {
      int tunnelPort = tunnel.get().getPort();
      SocketChannel client = SocketChannel.open();

      client.connect(new InetSocketAddress("localhost", tunnelPort));
      client.write(ByteBuffer.wrap("Knock\n".getBytes()));
      String response = readFromSocket(client);
      client.close();

      assertEquals(response, "Knock Knock\n");
    } finally {
      doubleEchoServer.stopServer();
      proxyServer.stopServer();
      tunnel.get().close();
    }
  }

  @Test(timeOut = 5000)
  public void testTunnelToEchoServerMultiRequest() throws IOException {
    MockServer proxyServer = startConnectProxyServer();
    MockServer doubleEchoServer = startDoubleEchoServer();
    Optional<Tunnel> tunnel = Tunnel.build("localhost", doubleEchoServer.getServerSocketPort(),
        "localhost", proxyServer.getServerSocketPort());

    try {
      int tunnelPort = tunnel.get().getPort();
      SocketChannel client = SocketChannel.open();

      client.connect(new InetSocketAddress("localhost", tunnelPort));
      client.write(ByteBuffer.wrap("Knock\n".getBytes()));
      String response1 = readFromSocket(client);

      client.write(ByteBuffer.wrap("Hello\n".getBytes()));
      String response2 = readFromSocket(client);

      client.close();

      assertEquals(response1, "Knock Knock\n");
      assertEquals(response2, "Hello Hello\n");
    } finally {
      doubleEchoServer.stopServer();
      proxyServer.stopServer();
      tunnel.get().close();
    }
  }

  private MockServer startTalkFirstEchoServer() throws IOException {
    return new MockServer() {
      @Override
      void handleClientSocket(Socket clientSocket) throws IOException {
        System.out.println("Writing to client");

        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        PrintWriter out = new PrintWriter(clientSocket.getOutputStream());
        out.println("Hello");
        out.flush();

        String line = in.readLine();
        while (line != null && isServerRunning()) {
          out.println(line + " " + line);
          out.flush();
          line = in.readLine();
        }
        clientSocket.close();
      }
    }.start();
  }

  private void runClientToTalkFirstServer(int tunnelPort) throws IOException {
    SocketChannel client = SocketChannel.open();

    client.connect(new InetSocketAddress("localhost", tunnelPort));
    String response0 = readFromSocket(client);
    System.out.println(response0);

    client.write(ByteBuffer.wrap("Knock\n".getBytes()));
    String response1 = readFromSocket(client);
    System.out.println(response1);


    client.write(ByteBuffer.wrap("Hello\n".getBytes()));
    String response2 = readFromSocket(client);
    System.out.println(response2);

    client.close();

    assertEquals(response0, "Hello\n");
    assertEquals(response1, "Knock Knock\n");
    assertEquals(response2, "Hello Hello\n");
  }

  @Test(timeOut = 5000)
  public void testTunnelToEchoServerThatRespondsFirst() throws IOException {
    MockServer proxyServer = startConnectProxyServer();
    MockServer talkFirstEchoServer = startTalkFirstEchoServer();
    Optional<Tunnel> tunnel = Tunnel.build("localhost", talkFirstEchoServer.getServerSocketPort(),
        "localhost", proxyServer.getServerSocketPort());

    try {
      int tunnelPort = tunnel.get().getPort();
      runClientToTalkFirstServer(tunnelPort);
    } finally {
      talkFirstEchoServer.stopServer();
      proxyServer.stopServer();
      tunnel.get().close();
    }
  }

  @Test(timeOut = 5000)
  public void testTunnelToEchoServerThatRespondsFirstWithMixedProxyAndServerResponseInBuffer() throws IOException {
    MockServer proxyServer = startConnectProxyServer();
    MockServer talkFirstEchoServer = startTalkFirstEchoServer();
    Optional<Tunnel> tunnel = Tunnel.build("localhost", talkFirstEchoServer.getServerSocketPort(),
        "localhost", proxyServer.getServerSocketPort());
    tunnel.get().simulateDelayBetweenConnectWriteAndRead(true);

    try {
      int tunnelPort = tunnel.get().getPort();
      runClientToTalkFirstServer(tunnelPort);
    } finally {
      talkFirstEchoServer.stopServer();
      proxyServer.stopServer();
      tunnel.get().close();
    }
  }

  @Test(timeOut = 5000)
  public void testTunnelToEchoServerThatRespondsFirstAcrossMultipleDrainReads() throws IOException {
    MockServer proxyServer = startConnectProxyServer(true);
    MockServer talkFirstEchoServer = startTalkFirstEchoServer();
    Optional<Tunnel> tunnel = Tunnel.build("localhost", talkFirstEchoServer.getServerSocketPort(),
        "localhost", proxyServer.getServerSocketPort());
    tunnel.get().simulateDelayBetweenConnectWriteAndRead(true);

    try {
      int tunnelPort = tunnel.get().getPort();
      runClientToTalkFirstServer(tunnelPort);
    } finally {
      talkFirstEchoServer.stopServer();
      proxyServer.stopServer();
      tunnel.get().close();
    }
  }


  @Test(timeOut = 5000)
  public void testTunnelToEchoServerThatRespondsFirstAcrossMultipleDrainReadsWithMultipleClients()
      throws IOException, InterruptedException {
    MockServer proxyServer = startConnectProxyServer(true);
    MockServer talkFirstEchoServer = startTalkFirstEchoServer();
    Optional<Tunnel> tunnel = Tunnel.build("localhost", talkFirstEchoServer.getServerSocketPort(),
        "localhost", proxyServer.getServerSocketPort());
    tunnel.get().simulateDelayBetweenConnectWriteAndRead(true);

    try {
      final int tunnelPort = tunnel.get().getPort();
      List<Thread> threads = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        threads.add(new EasyThread() {
          @Override
          void runThread() throws Exception {
            try {
              runClientToTalkFirstServer(tunnelPort);
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }.startThread());
      }
      for (Thread t : threads) {
        t.join();
      }
    } finally {
      talkFirstEchoServer.stopServer();
      proxyServer.stopServer();
      tunnel.get().close();
    }
  }

  private void runSimultaneousDataTransfers(boolean useTunnel, int nclients) throws IOException, InterruptedException {
    long t0 = System.currentTimeMillis();
    int nMsgs = 50;
    final List<String> msgsFromServer = new ArrayList<String>(nMsgs);
    for (int i = 0; i < nMsgs; i++) {
      msgsFromServer.add(i + " " + StringUtils.repeat("Babble babble ", 100000));
    }
    final Map<String, List<String>> msgsRecvdAtServer = new HashMap<>();

    final Map<String, List<String>> msgsFromClients = new HashMap<>();
    for (int c = 0; c < nclients ; c++) {
      msgsRecvdAtServer.put("" + c, new ArrayList<>(nMsgs));
      List<String> msgsFromClient = new ArrayList<>(nMsgs);
      for (int i = 0; i < nMsgs; i++) {
        msgsFromClient.add(c + ":" + i + " " + StringUtils.repeat("Blahhh Blahhh ", 100000));
      }
      msgsFromClients.put("" + c, msgsFromClient);
    }
    final Map<String, List<String>> msgsRecvdAtClients = new HashMap<>();

    MockServer talkPastServer = new MockServer() {
      @Override
      void handleClientSocket(Socket clientSocket) throws IOException {
        System.out.println("Writing to client");
        try {
          BufferedOutputStream serverOut = new BufferedOutputStream(clientSocket.getOutputStream());
          EasyThread clientWriterThread = new EasyThread() {
            @Override
            void runThread() throws Exception {
              long t = System.currentTimeMillis();
              try {
                for (String msg : msgsFromServer) {
                  serverOut.write(msg.getBytes());
                  serverOut.write("\n".getBytes());
                  sleepQuietly(2);
                }
              } catch (IOException e) {
                e.printStackTrace();
              }
              System.out.println("Server done writing in " + (System.currentTimeMillis() - t) + " ms");
            }
          }.startThread();
          _threads.add(clientWriterThread);

          BufferedReader serverIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
          String line = serverIn.readLine();
          while (line != null && !line.equals("Goodbye")) {
            //System.out.println("Client said [" + line.substring(0, 32) + "... ]");
            String [] tokens = line.split(":", 2);
            String client = tokens[0];
            msgsRecvdAtServer.get(client).add(line);
            line = serverIn.readLine();
          }
          System.out.println("Server done reading");
          try {
            clientWriterThread.join();
          } catch (InterruptedException e) {
          }
          serverOut.write("Goodbye\n".getBytes());
          serverOut.flush();
          clientSocket.close();
        } catch (IOException e) {
          e.printStackTrace();
          throw e;
        }
      }
    }.start();

    int targetPort = talkPastServer.getServerSocketPort();
    Optional<Tunnel> tunnel = Optional.empty();
    MockServer proxyServer = null;
    if (useTunnel) {
      proxyServer = startConnectProxyServer();
      tunnel = Tunnel.build("localhost", talkPastServer.getServerSocketPort(),
          "localhost", proxyServer.getServerSocketPort());
      targetPort = tunnel.get().getPort();
    }

    try {
      List<Thread> clientThreads = new ArrayList<>();
      final int portToUse = targetPort;
      for (int c = 0; c < nclients; c++) {
        msgsRecvdAtClients.put("" + c, new ArrayList<>(nMsgs));
      }

      for (int c = 0; c < nclients; c++) {
        final int clientId = c;
        clientThreads.add(new EasyThread() {
          @Override
          void runThread() throws Exception {
            long t = System.currentTimeMillis();
            System.out.println("\t" + clientId + ": Client starting");
            final List<String> msgsFromClient = msgsFromClients.get("" + clientId);
            List<String> msgsRecvdAtClient = msgsRecvdAtClients.get("" + clientId);
            //final SocketChannel client = SocketChannel.open(); // tunnel test hangs for some reason with SocketChannel
            Socket client = new Socket();
            client.connect(new InetSocketAddress("localhost", portToUse));
            EasyThread serverReaderThread = new EasyThread() {
              @Override
              public void runThread() {
                try {
                  BufferedReader clientIn = new BufferedReader(new InputStreamReader(client.getInputStream()));
                  String line = clientIn.readLine();
                  while (line != null && !line.equals("Goodbye")) {
                    //System.out.println("\t" + clientId + ": Server said [" + line.substring(0, 32) + "... ]");
                    msgsRecvdAtClient.add(line);
                    line = clientIn.readLine();
                  }
                } catch (IOException e) {
                  e.printStackTrace();
                }
                System.out.println("\t" + clientId + ": Client done reading");
              }
            }.startThread();
            BufferedOutputStream clientOut = new BufferedOutputStream(client.getOutputStream());
            for (String msg : msgsFromClient) {
              //System.out.println(clientId + " sending " + msg.length() + " bytes");
              clientOut.write(msg.getBytes());
              clientOut.write("\n".getBytes());
              sleepQuietly(2);
            }
            clientOut.write(("Goodbye\n".getBytes()));
            clientOut.flush();
            System.out.println("\t" + clientId + ": Client done writing in " + (System.currentTimeMillis() - t) + " ms");
            serverReaderThread.join();
            System.out.println("\t" + clientId + ": Client done in " + (System.currentTimeMillis() - t) + " ms");
            client.close();
          }
        }.startThread());
      }
      for (Thread clientThread : clientThreads) {
        clientThread.join();
      }
      System.out.println("All data transfer done in " + (System.currentTimeMillis() - t0) + " ms");
    } finally {
      talkPastServer.stopServer();
      if (tunnel.isPresent()) {
        proxyServer.stopServer();
        tunnel.get().close();
      }

      System.out.println("\tComparing client sent to server received");
      assertEquals(msgsFromClients, msgsRecvdAtServer);

      System.out.println("\tComparing server sent to client received");
      for (List<String> msgsRecvdAtClient : msgsRecvdAtClients.values()) {
        assertEquals(msgsFromServer, msgsRecvdAtClient);
      }
      System.out.println("\tDone");
    }
  }

  // Baseline test2 to ensure simultaneous data transfer protocol is fine
  @Test(timeOut = 20000)
  public void testSimultaneousDataTransfersWithDirectConnection() throws IOException, InterruptedException {
    runSimultaneousDataTransfers(false, 1);
  }

  @Test(timeOut = 20000)
  public void testSimultaneousDataTransfersWithDirectConnectionAndMultipleClients() throws IOException, InterruptedException {
    runSimultaneousDataTransfers(false, 5);
  }
  /*
    I wrote this test because I saw this symptom once randomly while testing with Gobblin. Test passes, but occasionally
    we see the following warning in the logs:

    15/10/29 21:11:17 WARN tunnel.Tunnel: exception handling event on java.nio.channels.SocketChannel[connected local=/127.0.0.1:34669 remote=/127.0.0.1:38578]
    java.nio.channels.CancelledKeyException
      at sun.nio.ch.SelectionKeyImpl.ensureValid(SelectionKeyImpl.java:73)
      at sun.nio.ch.SelectionKeyImpl.readyOps(SelectionKeyImpl.java:87)
      at java.nio.channels.SelectionKey.isWritable(SelectionKey.java:312)
      at gobblin.tunnel.Tunnel$ReadWriteHandler.write(Tunnel.java:423)
      at gobblin.tunnel.Tunnel$ReadWriteHandler.call(Tunnel.java:403)
      at gobblin.tunnel.Tunnel$ReadWriteHandler.call(Tunnel.java:365)
      at gobblin.tunnel.Tunnel$Dispatcher.dispatch(Tunnel.java:142)
      at gobblin.tunnel.Tunnel$Dispatcher.run(Tunnel.java:127)
      at java.lang.Thread.run(Thread.java:745)
   */
  @Test(timeOut = 20000)
  public void testSimultaneousDataTransfersWithTunnel() throws IOException, InterruptedException {
    runSimultaneousDataTransfers(true, 1);
  }

  @Test(timeOut = 20000)
  public void testSimultaneousDataTransfersWithTunnelAndMultipleClients() throws IOException, InterruptedException {
    runSimultaneousDataTransfers(true, 5);
  }

  @Test(expectedExceptions = IOException.class)
  public void testTunnelWhereProxyConnectionToServerFailsWithWriteFirstClient() throws IOException {
    MockServer proxyServer = startConnectProxyServer();
    final int nonExistentPort = 54321; // hope this doesn't exist!
    Optional<Tunnel> tunnel = Tunnel.build("localhost", nonExistentPort, "localhost", proxyServer.getServerSocketPort());
    try {
      int tunnelPort = tunnel.get().getPort();
      SocketChannel client = SocketChannel.open();

      client.configureBlocking(true);
      client.connect(new InetSocketAddress("localhost", tunnelPort));
      client.write(ByteBuffer.wrap("Knock\n".getBytes()));
      String response1 = readFromSocket(client);
      System.out.println(response1);

      client.close();

    } finally {
      proxyServer.stopServer();
      tunnel.get().close();
    }
  }

  // Disabled, as this test will not pass because there is no way to tell if remote peer closed connection without
  // writing data to it. A client that reads first will wait indefinitely even after the tunnel closes its connection,
  // unless it configures a timeout on the socket. This could be a problem when the remote provider is down or
  // non-existent. If we cannot make this test pass, we can delete it
  @Test(enabled = false, timeOut = 5000, expectedExceptions = IOException.class)
  public void testTunnelWhereProxyConnectionToServerFailsWithReadFirstClient() throws IOException {
    MockServer proxyServer = startConnectProxyServer();
    final int nonExistentPort = 54321;
    Optional<Tunnel> tunnel = Tunnel.build("localhost", nonExistentPort, "localhost", proxyServer.getServerSocketPort());
    try {
      int tunnelPort = tunnel.get().getPort();
      SocketChannel client = SocketChannel.open();

      client.configureBlocking(true);
      client.connect(new InetSocketAddress("localhost", tunnelPort));
      while(true) {
        String response1 = readFromSocket(client);
        System.out.println("Response = " + response1);
        sleepQuietly(1000);
      }
    } finally {
      proxyServer.stopServer();
      tunnel.get().close();
    }
  }

  // This test will not pass for the same reason as above
  @Test
  public void testTunnelWhereServerClosesConnection() {
  }

  @Test(timeOut = 5000)
  public void testTunnelThreadDeadAfterClose() throws IOException, InterruptedException {
    MockServer proxyServer = startConnectProxyServer();
    MockServer talkFirstEchoServer = startTalkFirstEchoServer();
    Optional<Tunnel> tunnel = Tunnel.build("localhost", talkFirstEchoServer.getServerSocketPort(),
        "localhost", proxyServer.getServerSocketPort());

    try {
      int tunnelPort = tunnel.get().getPort();
      SocketChannel client = SocketChannel.open();

      client.connect(new InetSocketAddress("localhost", tunnelPort));
      String response0 = readFromSocket(client);
      System.out.println(response0);

      client.write(ByteBuffer.wrap("Knock\n".getBytes()));
      // don't wait for response
      client.close();

      assertEquals(response0, "Hello\n");
    } finally {
      talkFirstEchoServer.stopServer();
      proxyServer.stopServer();
      tunnel.get().close();
      assertFalse(tunnel.get().isTunnelThreadAlive());
    }
  }

  /**
   * This test demonstrates connecting to a mysql DB through
   * and http proxy tunnel to a public data set of genetic data
   * http://www.ensembl.org/info/data/mysql.html
   *
   * Disabled for now because it requires the inclusion of a mysql jdbc driver jar
   *
   * @throws Exception
   */
  @Test(enabled = false, timeOut = 5000)
  public void accessEnsembleDB() throws Exception{
    MockServer proxyServer = startConnectProxyServer();
    Optional<Tunnel> tunnel = Tunnel.build("useastdb.ensembl.org", 5306,
        "localhost", proxyServer.getServerSocketPort());

    try {
      int port = tunnel.get().getPort();

      Connection connection =
          DriverManager.getConnection("jdbc:mysql://localhost:" + port + "/homo_sapiens_core_82_38?user=anonymous");
      String query2 = "SELECT DISTINCT gene_id, biotype, source, description from gene LIMIT 1000";

      ResultSet resultSet = connection.createStatement().executeQuery(query2);

      int row = 0;

      while (resultSet.next()) {
        row++;
        System.out
            .printf("%s|%s|%s|%s|%s%n", row, resultSet.getString(1), resultSet.getString(2), resultSet.getString(3),
                resultSet.getString(4));

      }

      assertEquals(row, 1000);
    }
    finally {
      proxyServer.stopServer();
      tunnel.get().close();
    }
  }

}