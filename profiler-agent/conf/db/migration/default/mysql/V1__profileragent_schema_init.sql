-- HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
--
-- (c) 2016-2018 Hortonworks, Inc. All rights reserved.
--
-- This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
-- Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
-- to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
-- properly licensed third party, you do not have any rights to this code.
--
-- If this code is provided to you under the terms of the AGPLv3:
-- (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
-- (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
--   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
-- (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
--   FROM OR RELATED TO THE CODE; AND
-- (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
--   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
--   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
--   OR LOSS OR CORRUPTION OF DATA.

CREATE SCHEMA IF NOT EXISTS profileragent;

CREATE TABLE IF NOT EXISTS profileragent.profilers (
  id  BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  version VARCHAR(50) NOT NULL,
  jobtype VARCHAR(255) NOT NULL,
  assettype VARCHAR(255) NOT NULL,
  profilerconf TEXT NOT NULL,
  user VARCHAR(255) NOT NULL,
  description TEXT,
  created TIMESTAMP DEFAULT now() NOT NULL
);

ALTER TABLE profileragent.profilers ADD CONSTRAINT profiler_name_constraint UNIQUE(name, version);

CREATE TABLE IF NOT EXISTS profileragent.profilerinstances (
  id  BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  displayname VARCHAR(255) NOT NULL,
  profilerid BIGINT NOT NULL,
  version INT NOT NULL,
  profilerconf TEXT NOT NULL,
  jobconf TEXT NOT NULL,
  active BOOLEAN DEFAULT TRUE,
  owner VARCHAR(255) NOT NULL,
  queue VARCHAR(255) NOT NULL,
  description TEXT,
  created TIMESTAMP DEFAULT now() NOT NULL,
  FOREIGN KEY(profilerid) REFERENCES profilers (id)
);

ALTER TABLE profileragent.profilerinstances  ADD CONSTRAINT profilerinstance_name_constraint UNIQUE(name, version);

CREATE TABLE IF NOT EXISTS profileragent.asset_selector_definitions (
  id  BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL UNIQUE,
  profiler_instance_name VARCHAR(255) NOT NULL ,
  selector_type VARCHAR(255) NOT NULL,
  config TEXT NOT NULL,
  source_definition TEXT NOT NULL,
  pick_filters TEXT NOT NULL ,
  drop_filters TEXT NOT NULL,
  lastupdated TIMESTAMP DEFAULT now() NOT NULL
);

CREATE TABLE IF NOT EXISTS profileragent.jobs (
  id  BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY,
  profilerinstanceid BIGINT NOT NULL,
  profilerinstancename VARCHAR(255) NOT NULL,
  status VARCHAR(255) NOT NULL ,
  jobtype VARCHAR(255) NOT NULL,
  conf TEXT NOT NULL,
  details TEXT NOT NULL,
  description TEXT,
  jobuser VARCHAR(255) NOT NULL,
  queue VARCHAR(255) NOT NULL,
  submitter VARCHAR(255) NOT NULL,
  start TIMESTAMP DEFAULT now() NOT NULL ,
  lastupdated TIMESTAMP DEFAULT now() NOT NULL,
  end TIMESTAMP DEFAULT now(),
  FOREIGN KEY(profilerinstanceid) REFERENCES profilerinstances (id)
);

CREATE TABLE IF NOT EXISTS profileragent.assetjobshistory (
  id BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY,
  profilerinstancename VARCHAR(255) NOT NULL,
  assetid VARCHAR(1024) NOT NULL,
  jobid  BIGINT NOT NULL ,
  status VARCHAR(255) NOT NULL ,
  lastupdated TIMESTAMP DEFAULT now() NOT NULL,
  FOREIGN KEY(jobid) REFERENCES jobs (id)
);

CREATE TABLE IF NOT EXISTS profileragent.dataset_asset (
  id BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY,
  datasetname VARCHAR(255) NOT NULL,
  assetid VARCHAR(1024) NOT NULL,
  priority  BIGINT NOT NULL DEFAULT 10000,

  CONSTRAINT unique_dsname_assetid_constraint UNIQUE (datasetname, assetid)
);