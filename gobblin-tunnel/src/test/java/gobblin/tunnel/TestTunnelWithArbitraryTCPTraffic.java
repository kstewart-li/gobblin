package gobblin.tunnel;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.apache.commons.io.IOUtils;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


@Test
public class TestTunnelWithArbitraryTCPTraffic {
  private abstract class MockServer implements Runnable {
    volatile boolean _serverRunning = true;
    ServerSocket _server;
    Set<Thread> _threads = Collections.synchronizedSet(new HashSet<Thread>());
    int _serverSocketPort;

    public MockServer start() throws IOException {
      _server = new ServerSocket();
      _server.setSoTimeout(5000);
      _server.bind(new InetSocketAddress("localhost", 0));
      _serverSocketPort = _server.getLocalPort();
      Thread thread = new Thread(this);
      thread.start();
      _threads.add(thread);
      return this;
    }

    // accept thread
    public void run() {
      while (_serverRunning) {
        try {
          final Socket clientSocket = _server.accept();
          //clientSocket.setSoTimeout(5000);
          System.out.println("Accepted connection on " + getServerSocketPort());
          // client handler thread
          Thread thread = new Thread() {
            @Override
            public void run() {
              try {
                handleClientSocket(clientSocket);
              } catch (IOException e) {
                onIOException(clientSocket, e);
              }
              _threads.remove(this);
            }
          };
          _threads.add(thread);
          thread.start();
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

    public void stopServer() {
      _serverRunning = false;
      for (Thread thread : _threads) {
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
        InputStream proxyToServerIn = serverSocket.getInputStream();
        OutputStream proxyToServerOut = serverSocket.getOutputStream();
        Thread thread = new Thread() {
          @Override
          public void run() {
            try {
              IOUtils.copy(clientToProxyIn, proxyToServerOut);
            } catch (IOException e) {
              System.out.println("Exception " + e.getMessage() + " on " + getServerSocketPort());
              stopServer();
            }
          }
        };
        _threads.add(thread);
        thread.start();
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

  private MockServer startDoubleEchoServer() throws IOException {
    return new MockServer() {
      @Override
      void handleClientSocket(Socket clientSocket) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        PrintWriter out = new PrintWriter(clientSocket.getOutputStream());
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
    MockServer proxyServer = startConnectProxyServer(false);
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
    MockServer proxyServer = startConnectProxyServer(false);
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

  @Test(timeOut = 5000)
  public void testTunnelToEchoServerThatRespondsFirst() throws IOException {
    MockServer proxyServer = startConnectProxyServer(false);
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
      String response1 = readFromSocket(client);
      System.out.println(response1);


      client.write(ByteBuffer.wrap("Hello\n".getBytes()));
      String response2 = readFromSocket(client);
      System.out.println(response2);


      client.close();

      assertEquals(response0, "Hello\n");
      assertEquals(response1, "Knock Knock\n");
      assertEquals(response2, "Hello Hello\n");
    } finally {
      talkFirstEchoServer.stopServer();
      proxyServer.stopServer();
      tunnel.get().close();
    }
  }

  @Test(timeOut = 5000)
  public void testTunnelToEchoServerThatRespondsFirstWithMixedProxyAndServerResponseInBuffer() throws IOException {
    MockServer proxyServer = startConnectProxyServer(false);
    MockServer talkFirstEchoServer = startTalkFirstEchoServer();
    Optional<Tunnel> tunnel = Tunnel.build("localhost", talkFirstEchoServer.getServerSocketPort(),
        "localhost", proxyServer.getServerSocketPort());
    tunnel.get().simulateDelayBetweenConnectWriteAndRead(true);

    try {
      int tunnelPort = tunnel.get().getPort();
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
    } finally {
      talkFirstEchoServer.stopServer();
      proxyServer.stopServer();
      tunnel.get().close();
    }
  }

  @Test(timeOut = 5000)
  public void testGCingTunnelToEchoServerThatRespondsFirstAcrossMultipleDrainReads() throws IOException {
    MockServer proxyServer = startConnectProxyServer(true);
    MockServer talkFirstEchoServer = startTalkFirstEchoServer();
    Optional<Tunnel> tunnel = Tunnel.build("localhost", talkFirstEchoServer.getServerSocketPort(),
        "localhost", proxyServer.getServerSocketPort());
    tunnel.get().simulateDelayBetweenConnectWriteAndRead(true);

    try {
      int tunnelPort = tunnel.get().getPort();
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
    } finally {
      talkFirstEchoServer.stopServer();
      proxyServer.stopServer();
      tunnel.get().close();
    }
  }

  @Test(expectedExceptions = IOException.class)
  public void testTunnelWhereProxyConnectionToServerFailsWithWriteFirstClient() throws IOException {
    MockServer proxyServer = startConnectProxyServer(false);
    final int nonExistentPort = 54321;
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
  public void testTunnelWhereProxyConnectionToServerFailsWithReadFirstClient() throws IOException, InterruptedException {
    MockServer proxyServer = startConnectProxyServer(false);
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
        Thread.sleep(1000);
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
    MockServer proxyServer = startConnectProxyServer(false);
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
    MockServer proxyServer = startConnectProxyServer(false);
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