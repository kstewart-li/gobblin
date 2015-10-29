package gobblin.tunnel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;
import org.mockserver.integration.ClientAndProxy;
import org.mortbay.jetty.Server;
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Due to the lack of a suitable embeddable proxy server (MockServer tries to SSL-enable all CONNECT-initiated traffic
 * and the jetty version used here, v6, is very old and buggy) you would need to start up an HTTP proxy on port 10926
 * for these tests to work.
 */
@Test
public class TestTunnelWithArbitraryTCPTraffic {
  int _serverSocketPort;
  public static final int PORT = 10926;
  // Using Jetty 6 because that is already a dependency from somewhere
  Server _proxyServer;

/*  @BeforeMethod
  void setup() throws Exception {
    _proxyServer = new Server();
    SelectChannelConnector connector = new SelectChannelConnector();
    connector.setPort(PORT);
    _proxyServer.addConnector(connector);

    Context context = new Context(_proxyServer, "*//*");
    context.addServlet(new ServletHolder(new ProxyServlet()), "*//*");
    _proxyServer.start();
  }

  @AfterMethod
  void cleanup() throws Exception {
    _proxyServer.stop();
  }*/

  private abstract class MockServer implements Runnable {
    volatile boolean _serverRunning = true;
    ServerSocket _server;
    Set<Thread> _threads = Collections.synchronizedSet(new HashSet<Thread>());

    public MockServer start() throws IOException {
      _server = new ServerSocket();
      _server.setSoTimeout(5000);
      _server.bind(new InetSocketAddress("localhost", 0));
      _serverSocketPort = _server.getLocalPort();
      new Thread(this).start();
      return this;
    }

    // accept thread
    public void run() {
      while (_serverRunning) {
        try {
          final Socket clientSocket = _server.accept();
          clientSocket.setSoTimeout(5000);
          System.out.println("Accepted connection");
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
    System.out.println("Reading from socket");
    client.read(readBuf);
    readBuf.flip();
    return StandardCharsets.US_ASCII.decode(readBuf).toString();
  }

  private void writeToSocket(SocketChannel client, byte [] bytes) throws IOException {
    client.write(ByteBuffer.wrap(bytes));
    client.socket().getOutputStream().flush();
  }

  // Baseline test to ensure clients work without tunnel
  @Test
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
    Optional<Tunnel> tunnel = Tunnel.build("localhost", doubleEchoServer.getServerSocketPort(), "localhost", PORT);

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
      tunnel.get().close();
    }
  }

  @Test(timeOut = 5000)
  public void testTunnelToEchoServerMultiRequest() throws IOException {
    MockServer doubleEchoServer = startDoubleEchoServer();
    Optional<Tunnel> tunnel = Tunnel.build("localhost", doubleEchoServer.getServerSocketPort(), "localhost", PORT);

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
      tunnel.get().close();
    }
  }

  private MockServer startTalkFirstEchoServer() throws IOException {
    return new MockServer() {
      @Override
      void handleClientSocket(Socket clientSocket) throws IOException {
        System.out.println("Writting to client");

        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        PrintWriter out = new PrintWriter(clientSocket.getOutputStream());
        out.println("Hello\n");
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

  // Disabled because this needs to be fixed
  @Test(enabled = true)
  public void testTunnelToEchoServerThatRespondsFirst() throws IOException {
    MockServer talkFirstEchoServer = startTalkFirstEchoServer();
    Optional<Tunnel> tunnel = Tunnel.build("localhost", talkFirstEchoServer.getServerSocketPort(), "localhost", PORT);

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

      assertEquals(response0, "Hello\n\n");
      assertEquals(response1, "Knock Knock\n");
      assertEquals(response2, "Hello Hello\n");
    } finally {
      talkFirstEchoServer.stopServer();
      tunnel.get().close();
    }
  }

  @Test
  public void testTunnelWhereServerClosesConnection() {

  }

  @Test
  public void testTunnelWhereClientClosesConnection() {
  }

  @Test
  public void testTunnelWhereProxyConnectionToServerFails() {
  }

  /**
   * This test demonstrates connecting to a mysql DB through
   * and http proxy tunnel to a public data set of genetic data
   * http://www.ensembl.org/info/data/mysql.html
   *
   * @throws Exception
   */
  @Test(timeOut = 5000)
  public void accessEnsembleDB() throws Exception{

    Optional<Tunnel> tunnel = Tunnel.build("useastdb.ensembl.org", 5306, "localhost", PORT);

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
      tunnel.get().close();
    }
  }

}