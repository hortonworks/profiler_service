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

package register

import java.io.{File, FileFilter}

import javax.inject.{Inject, Singleton}
import domain.{AssetSelectorDefinition, Profiler, ProfilerInstance}
import job.submitter.ProfilerSubmitterManager
import play.api.libs.json.Json
import play.api.{Configuration, Logger}
import repo.{AssetSelectorDefinitionRepo, ProfilerInstanceRepo, ProfilerRepo}

import scala.concurrent.Future
import scala.io.Source
import scala.language.postfixOps

@Singleton
class DefaultProfilerManager @Inject()(profilerRepo: ProfilerRepo,
                                       profilerInstanceRepo: ProfilerInstanceRepo,
                                       config: Configuration, submitterManager: ProfilerSubmitterManager,
                                       selectorDefinitionRepo: AssetSelectorDefinitionRepo) {

  private val hdpStackName = config.getString("hdp.stack.name").getOrElse("hdp2")

  private val isAutoRegistrationEnable = config.getBoolean("profiler.autoregister").getOrElse(true)

  private val profilerDirectory = config.getString("profiler.dir").getOrElse("/usr/dss/profilers")

  private val profilerHdfsDirectory = config.getString("profiler.hdfs.dir").getOrElse("/apps/dpprofiler/profilers")

  private val profilerUser = config.getString("profiler.user").getOrElse("dpprofiler")

  private val ProfilerDirVariable = "{{PROFILER_DIR}}"

  private val ProfilerUserVariable = "{{PROFILER_USER}}"

  private val secured = config.getBoolean("dpprofiler.secured").getOrElse(false)

  import domain.JsonFormatters._

  import scala.concurrent.ExecutionContext.Implicits.global
  import sys.process._

  if (isAutoRegistrationEnable) {
    Logger.info("Auto Registering Profiler")
    Logger.info(s"Getting profilers from : ${profilerDirectory}")
    registerAll()
  } else Logger.info("Auto register of profiler is disabled")

  private def runKinitCommand(): Int = {
    if (secured) {
      val keytab = config.getString("dpprofiler.kerberos.keytab").get
      val principal = config.getString("dpprofiler.kerberos.principal").get
      val kinitCmd = s"kinit -kt ${keytab} ${principal}"
      Logger.info(s"Running kinit: $kinitCmd")
      kinitCmd !
    } else 0
  }

  private def uploadProfiler(profilerDir: File, name: String, version: String): Boolean = {
    runKinitCommand()
    val fromDir = s"${profilerDir.getAbsolutePath}/lib"
    val toDir = getProfilerHdfsToDir(name, version)
    val makeDir = s"hdfs dfs -mkdir -p ${toDir}"
    Logger.info(s"Executing : ${makeDir}")
    val result1: Int = makeDir !;
    if (result1 != 0) {

      Logger.error(s"Failed to run command $makeDir")
    }
    val cmd = s"hdfs dfs -put -f $fromDir $toDir"
    Logger.info(s"Executing : ${cmd}")
    val result: Int = cmd !;
    if (result != 0) {
      Logger.error(s"Failed to run command $cmd")
    }
    result == 0
  }

  private def getProfilerHdfsToDir(name: String, version: String) = {
    s"$profilerHdfsDirectory/$name/$version"
  }

  private def getProfilerHdfsDir(name: String, version: String) = {
    s"$profilerHdfsDirectory/$name/$version/lib"
  }

  private def profilerInstanceIfNotExists(profilerInstance: ProfilerInstance): Future[Option[ProfilerInstance]] = {
    profilerInstanceRepo.findByNameOpt(profilerInstance.name).map {
      p =>
        Logger.info(s"Profiler Instance with ${profilerInstance.name} exists : ${p.isDefined}")
        p match {
          case Some(pr) => None
          case None => Some(profilerInstance)
        }
    }
  }

  private def saveProfilerIfNeeded(profilerDir: File, profiler: Profiler): Future[Profiler] = {
    profilerRepo.findByNameAndVersion(profiler.name, profiler.version).flatMap {
      case Some(p) => Future.successful(p)
      case None =>
        if (uploadProfiler(profilerDir, profiler.name, profiler.version)) {
          profilerRepo.save(profiler)
        }
        else {
          val message = s"Failed to upload profiler: ${profiler.name} jars to hdfs"
          Logger.info(message)
          Future.failed(new Exception(message))
        }
    }
  }

  private def getProfilerInstance(file: File, profilerHdfsDir: String, profilerId: Long): ProfilerInstance = {
    val content = Source.fromFile(file).mkString
      .replace(ProfilerDirVariable, profilerHdfsDir)
      .replace(ProfilerUserVariable, profilerUser)
    Json.parse(content).as[ProfilerInstance]
      .copy(profilerId = profilerId)
  }

  private def getSelectorDefinition(file: File): AssetSelectorDefinition = {
    val content = Source.fromFile(file).mkString
    Json.parse(content).as[AssetSelectorDefinition]
  }

  def getProfiler(file: File): Profiler = {
    val content = Source.fromFile(new File(s"${file.getAbsolutePath}/manifest/meta.json")).mkString
    Json.parse(content).as[Profiler]
  }

  def getProfilerAfterSubstitution(file: File, profilerHdfsDir: String): Profiler = {
    val content = Source.fromFile(new File(s"${file.getAbsolutePath}/manifest/meta.json")).mkString
      .replace(ProfilerDirVariable, profilerHdfsDir)
      .replace(ProfilerUserVariable, profilerUser)
    Json.parse(content).as[Profiler]
  }

  private def registerProfiler(profilerDir: File): Future[Seq[ProfilerInstance]] = {
    val profilerDirForStack = new File(s"${profilerDir.getAbsolutePath}/${hdpStackName}")
    val tempProfiler = getProfiler(profilerDirForStack)
    val profilerHdfsDir = getProfilerHdfsDir(tempProfiler.name, tempProfiler.version)
    val profiler = saveProfilerIfNeeded(profilerDirForStack, getProfilerAfterSubstitution(profilerDirForStack, profilerHdfsDir))
    profiler.flatMap(p => saveProfilerInstances(profilerDirForStack, p))
      .map {
        pi =>
          saveSelectors(profilerDirForStack)
          pi
      }
  }

  private def saveSelectors(profilerDir: File) = {
    val selectors = new File(s"${profilerDir.getAbsolutePath}/manifest").listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = pathname.isFile && pathname.getName().endsWith("selector.conf")
    }).map(f => getSelectorDefinition(f))

    val selectorFutures: Seq[Future[Option[AssetSelectorDefinition]]] = selectors.map {
      selector =>
        selectorDefinitionRepo.findByName(selector.name).flatMap {
          case Some(s) => Future.successful(None)
          case None =>
            Logger.info(s"Saving selector : ${selector}")
            selectorDefinitionRepo.save(selector).map {
              savedSelector =>
                submitterManager.addSelectorDefinition(savedSelector)
                Some(savedSelector)
            }
        }
    }

    Future.sequence(selectorFutures).map(_.filter(_.isDefined).map(_.get))
  }

  private def saveProfilerInstances(profilerDir: File, profiler: Profiler): Future[Seq[ProfilerInstance]] = {

    val profilerInstances = new File(s"${profilerDir.getAbsolutePath}/manifest").listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = pathname.isFile && pathname.getName().endsWith("profiler.conf")
    }).map(f => getProfilerInstance(f, getProfilerHdfsDir(profiler.name, profiler.version), profiler.id.get))

    Logger.info(s"Registering profiler ${profiler}")

    val profilerInstancesToRegister: Future[Seq[ProfilerInstance]] = for {
      ps <- Future.sequence(profilerInstances.toSeq.map(profilerInstanceIfNotExists))
    } yield ps.filter(_.isDefined).map(_.get)

    profilerInstancesToRegister.flatMap {
      ps =>
        val si: Seq[Future[ProfilerInstance]] = ps.map {
          p =>
            Logger.info(s"Registering profiler instance ${p.name} : ${p.version}")
            profilerInstanceRepo.save(p).map {
              e =>
                submitterManager.addProfilerInstance(e)
                e.profilerInstance
            }
        }
        Future.sequence(si)
    }

  }

  private def registerAll() = {
    val profilerDir = new File(profilerDirectory)

    if (!profilerDir.exists()) throw new IllegalArgumentException(s"Profiler dir : ${profilerDirectory} not exists")

    val profilers = profilerDir.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = pathname.isDirectory
    })

    Future.sequence(profilers.toSeq.map(registerProfiler)).onFailure {
      case e =>
        Logger.error(s"Exception while registering profilers. ${e.getMessage}", e)
    }
  }

}
