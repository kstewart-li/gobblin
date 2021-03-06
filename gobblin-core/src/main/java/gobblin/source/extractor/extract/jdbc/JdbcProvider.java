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

package gobblin.source.extractor.extract.jdbc;

import gobblin.tunnel.Tunnel;
import org.apache.commons.dbcp.BasicDataSource;

import java.sql.SQLException;
import java.util.Optional;


/**
 * Create JDBC data source
 *
 * @author nveeramr
 */
public class JdbcProvider extends BasicDataSource {
  private Tunnel _tunnel;
  // If extract type is not provided then consider it as a default type
  public JdbcProvider(String driver, String connectionUrl, String user, String password, int numconn, int timeout) {
    this.connect(driver, connectionUrl, user, password, numconn, timeout, "DEFAULT", null, -1);
  }

  public JdbcProvider(String driver, String connectionUrl, String user, String password, int numconn, int timeout,
      String type) {
    this.connect(driver, connectionUrl, user, password, numconn, timeout, type, null, -1);
  }

  public JdbcProvider(String driver, String connectionUrl, String user, String password, int numconn, int timeout,
      String type, String proxyHost, int proxyPort) {
    this.connect(driver, connectionUrl, user, password, numconn, timeout, type, proxyHost, proxyPort);
  }

  public void connect(String driver, String connectionUrl, String user, String password, int numconn, int timeout,
      String type, String proxyHost, int proxyPort) {

    if(proxyHost != null && proxyPort > 0) {
      String remoteHost="";
      int remotePort = 0;
      // TODO make connection Url parsing much more robust -- some connections URLs can have colons and slashes in the
      // weirdest places
      int hostStart = connectionUrl.indexOf("://") + 3;
      int portStart =  connectionUrl.indexOf(":", hostStart);
      remoteHost = connectionUrl.substring(hostStart, portStart);
      remotePort = Integer.decode(connectionUrl.substring(portStart + 1, connectionUrl.indexOf("/", portStart)));

      _tunnel = Tunnel.build(remoteHost, remotePort, proxyHost, proxyPort).get();
      int tunnelPort = _tunnel.getPort();

      //mangle connectionUrl, replace hostname with localhost -- hopefully the hostname is not needed!!!
      String newConnectionUrl = connectionUrl.replaceFirst(remoteHost, "127.0.0.1")
          .replaceFirst(":"+ remotePort, ":" + tunnelPort);
      System.out.println("*********** mangled " + connectionUrl + " to " + newConnectionUrl);
      connectionUrl = newConnectionUrl;
    }

    this.setDriverClassName(driver);
    this.setUsername(user);
    this.setPassword(password);
    this.setUrl(connectionUrl);
    this.setInitialSize(0);
    this.setMaxIdle(numconn);
    this.setMaxActive(timeout);
  }

  @Override
  public synchronized void close() throws SQLException {
    super.close();
    if (_tunnel != null) {
      _tunnel.close();
    }
  }
}
