package gobblin.tunnel;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpForward;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests tunneling through an http proxy.
 *
 * In order to execute these tests an http proxy must be running on localhost
 * and listening on port 10926
 */
public class TunnelTest {

  private ClientAndServer _mockServer;

  @BeforeClass
  public void startProxy()
      throws IOException {
    _mockServer = ClientAndServer.startClientAndServer(10926);
  }

  @AfterClass
  public void stopProxy() {
    _mockServer.stop();
  }

  @AfterMethod
  public void reset(){
    _mockServer.reset();
  }

  @Test
  public void mustBuildTunnelAndStartAcceptingConnections()
      throws Exception {
    Optional<Tunnel> tunnel = Tunnel.build("example.org", 80, "localhost", 10926);

    try {
      int tunnelPort = tunnel.get().getPort();
      assertTrue(tunnelPort > 0);
    } finally {
      tunnel.get().close();
    }
  }

  @Test
  public void mustHandleClientDisconnectingWithoutClosingTunnel()
      throws Exception {
    mockExample();
    Optional<Tunnel> tunnel = Tunnel.build("example.org", 80, "localhost", 10926);

    try {
      int tunnelPort = tunnel.get().getPort();
      SocketChannel client = SocketChannel.open();

      client.connect(new InetSocketAddress("localhost", tunnelPort));
      client.write(ByteBuffer.wrap("GET / HTTP/1.1%nUser-Agent: GaaP%nConnection:keep - alive %n%n".getBytes()));
      client.close();

      assertNotNull(fetchContent(tunnelPort));
    } finally {
      tunnel.get().close();
    }
  }

  @Test
  public void mustHandleConnectionToExternalResource()
      throws Exception {

    mockExample();
    Optional<Tunnel> tunnel = Tunnel.build("example.org", 80, "localhost", 10926);

    try {
      String content = fetchContent(tunnel.get().getPort());

      assertNotNull(content);
    } finally {
      tunnel.get().close();
    }
  }

  @Test
  public void mustHandleMultipleConnections()
      throws Exception {
    mockExample();
    Optional<Tunnel> tunnel = Tunnel.build("example.org", 80, "localhost", 10926);
    int clients = 5;

    final CountDownLatch startSignal = new CountDownLatch(1);
    final CountDownLatch doneSignal = new CountDownLatch(clients);

    ExecutorService executor = Executors.newFixedThreadPool(clients);
    try {
      final int tunnelPort = tunnel.get().getPort();

      List<Future<String>> results = new ArrayList<Future<String>>();

      for (int i = 0; i < clients; i++) {
        Future<String> result = executor.submit(new Callable<String>() {
          @Override
          public String call()
              throws Exception {
            startSignal.await();

            try {
              return fetchContent(tunnelPort);
            } finally {
              doneSignal.countDown();
            }
          }
        });

        results.add(result);
      }

      startSignal.countDown();
      doneSignal.await();

      for (Future<String> result : results) {
        assertNotNull(result.get());
      }
    } finally {
      tunnel.get().close();
    }
  }

  @Test(expectedExceptions = SocketException.class)
  public void mustRefuseConnectionWhenProxyIsUnreachable()
      throws Exception {

    Optional<Tunnel> tunnel = Tunnel.build("example.org", 80, "localhost", 1);

    try {
      int tunnelPort = tunnel.get().getPort();

      fetchContent(tunnelPort);
    } finally {
      tunnel.get().close();
    }
  }

  @Test(expectedExceptions = SocketException.class)
  public void mustRefuseConnectionWhenProxyRefuses() throws Exception{
    _mockServer.when(HttpRequest.request().withMethod("CONNECT").withPath("www.us.apache.org:80"))
        .respond(HttpResponse.response().withStatusCode(403));

    Optional<Tunnel> tunnel = Tunnel.build("example.org", 80, "localhost", 10926);

    try {
      int tunnelPort = tunnel.get().getPort();

      fetchContent(tunnelPort);
    } finally {
      tunnel.get().close();
    }
  }

  @Test(expectedExceptions = SocketException.class)
  public void mustRefuseConnectionWhenProxyTimesOut() throws Exception{
    _mockServer.when(HttpRequest.request().withMethod("CONNECT").withPath("www.us.apache.org:80"))
        .respond(HttpResponse.response().withDelay(TimeUnit.SECONDS,2).withStatusCode(200));

    Optional<Tunnel> tunnel = Tunnel.build("example.org", 80, "localhost", 10926);

    try {
      int tunnelPort = tunnel.get().getPort();

      fetchContent(tunnelPort);
    } finally {
      tunnel.get().close();
    }
  }

  @Test(enabled = false)
  public void mustDownloadLargeFiles()
      throws Exception {

    _mockServer.when(HttpRequest.request().withMethod("CONNECT").withPath("www.us.apache.org:80"))
        .respond(HttpResponse.response().withStatusCode(200));
    _mockServer.when(HttpRequest.request().withMethod("GET")
        .withPath("/dist//httpcomponents/httpclient/binary/httpcomponents-client-4.5.1-bin.tar.gz"))
        .forward(HttpForward.forward().withHost("www.us.apache.org").withPort(80));

    Optional<Tunnel> tunnel = Tunnel.build("www.us.apache.org", 80, "localhost", 10926);
    try {
      IOUtils.copyLarge((InputStream) new URL("http://localhost:" + tunnel.get().getPort()
              + "/dist//httpcomponents/httpclient/binary/httpcomponents-client-4.5.1-bin.tar.gz")
              .getContent(new Class[]{InputStream.class}),
          new FileOutputStream(File.createTempFile("httpcomponents-client-4.5.1-bin", "tar.gz")));
    } finally {
      tunnel.get().close();
    }
  }

  private String fetchContent(int tunnelPort)
      throws IOException {
    InputStream content = (InputStream) new URL(String.format("http://localhost:%s/", tunnelPort)).openConnection()
        .getContent(new Class[]{InputStream.class});
    return IOUtils.toString(content);
  }

  private void mockExample()
      throws IOException {
    _mockServer.when(HttpRequest.request().withMethod("CONNECT").withPath("example.org:80"))
        .respond(HttpResponse.response().withStatusCode(200));
    _mockServer.when(HttpRequest.request().withMethod("GET").withPath("/"))
        .respond(HttpResponse.response(IOUtils.toString(getClass().getResourceAsStream("/example.org.html"))));
  }

}