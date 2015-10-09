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

/**
 * @author navteniev@linkedin.com
 *
 * Implements a tunnel through a proxy to resource on the internet
 */
public class Tunnel {
  public static int build(String remoteHost, int remotePort, String proxyHost, int proxyPort) {
    throw new AbstractMethodError();
  }
}
