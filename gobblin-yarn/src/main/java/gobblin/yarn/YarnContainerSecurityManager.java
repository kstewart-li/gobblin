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

package gobblin.yarn;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import com.google.common.base.Throwables;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;

import gobblin.yarn.event.DelegationTokenUpdatedEvent;


/**
 * A class for managing token renewing in the containers including the container for the
 * {@link GobblinApplicationMaster}.
 *
 * <p>
 *   This class implements a simple monitor for modifications on the token file and reloads tokens
 *   in the token file if the file has been modified and adds the tokens to the credentials of the
 *   current login user.
 * </p>
 *
 * @author ynli
 */
public class YarnContainerSecurityManager extends AbstractIdleService {

  private static final Logger LOGGER = LoggerFactory.getLogger(YarnContainerSecurityManager.class);

  private final FileSystem fs;
  private final Path tokenFilePath;
  private final EventBus eventBus;

  public YarnContainerSecurityManager(Config config, FileSystem fs, EventBus eventBus) {
    this.fs = fs;
    this.tokenFilePath = new Path(config.getString(GobblinYarnConfigurationKeys.TOKEN_FILE_PATH));
    this.eventBus = eventBus;
  }

  @SuppressWarnings("unused")
  @Subscribe
  public void handleTokenFileUpdatedEvent(DelegationTokenUpdatedEvent delegationTokenUpdatedEvent) {
    try {
      readAndAddDelegationTokens(this.tokenFilePath);
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
  }

  @Override
  protected void startUp() throws Exception {
    this.eventBus.register(this);
  }

  @Override
  protected void shutDown() throws Exception {
    // Nothing to do
  }

  /**
   * Read the {@link Token}s stored in the token file and add them for the login user.
   */
  private void readAndAddDelegationTokens(Path tokenFilePath) throws IOException {
    LOGGER.info("Reading updated token from token file: " + tokenFilePath);
    Collection<Token<? extends TokenIdentifier>> tokens =
        YarnHelixUtils.readTokensFromFile(tokenFilePath, this.fs.getConf());
    for (Token<? extends TokenIdentifier> token : tokens) {
      if (!UserGroupInformation.getCurrentUser().addToken(token)) {
        LOGGER.error(String.format("Failed to add token %s to user %s",
            token.toString(), UserGroupInformation.getLoginUser().getShortUserName()));
      }
    }
  }
}
