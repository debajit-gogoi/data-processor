/**
-- DDL for monitoring
*/
CREATE TABLE monitoring
(
  appId char(255) DEFAULT NULL,
  appName char(255) NOT NULL,
  appType char(63) NOT NULL,
  appState char(63) DEFAULT NULL,
  appFinalStatus char(63) DEFAULT NULL,
  startedTime datetime DEFAULT NULL,
  launchTime datetime DEFAULT NULL,
  finishedTime datetime DEFAULT NULL,
  elapsedTime int DEFAULT NULL,
  pdDedupKey char(127) PRIMARY KEY,
  pdCreatedTime datetime NOT NULL,
  UNIQUE(appName, appType, pdDedupKey, pdCreatedTime)
);
