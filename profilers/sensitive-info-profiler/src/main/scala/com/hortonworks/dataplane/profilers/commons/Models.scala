/*
 *   HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 *   (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 *   This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 *   Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 *   to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 *   properly licensed third party, you do not have any rights to this code.
 *
 *   If this code is provided to you under the terms of the AGPLv3:
 *   (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 *   (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *     LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 *   (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *     FROM OR RELATED TO THE CODE; AND
 *   (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *     DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *     DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *     OR LOSS OR CORRUPTION OF DATA.
 */

package com.hortonworks.dataplane.profilers.commons

import com.hortonworks.dataplane.profilers.commons.Models.DryRunOutPutStatus.DryRunOutPutStatus
import com.hortonworks.dataplane.profilers.commons.parse.{AtlasInfo, HiveAsset, ProfilerInput}


object Models {

  case class InputArgs(profilerInput: ProfilerInput, atlasInfo: AtlasInfo,
                       tables: Seq[HiveAsset], sampleSize: Long,
                       path: String, configRootPath: String, dslLibraryPath: Option[String])


  case class DryRunDSLAndTag(dsl: String, tags: List[String])

  case class DryRunRule(name: String, data: List[String], dsls: List[DryRunDSLAndTag])

  case class DryRunSettings(dryRunRules: List[DryRunRule])

  case class DataAndTags(value: String, tags: List[String])

  case class DryRunResult(name: String, data: List[DataAndTags])

  object DryRunOutPutStatus extends Enumeration {
    type DryRunOutPutStatus = Value
    val SUCCESSFUL, FAILURE = Value
  }


  case class Cause(topLevelReasons: List[String], stackTrace: String)

  case class DryRunOutPutSchema(status: DryRunOutPutStatus, cause: Option[Cause], results: Option[List[DryRunResult]])

}
