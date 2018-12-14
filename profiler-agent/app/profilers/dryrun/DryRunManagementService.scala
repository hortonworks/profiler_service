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


package profilers.dryrun

import java.util.UUID

import akka.actor.{ActorSystem, Cancellable}
import commons.CommonUtils
import domain.{JobTask, Profiler, ProfilerAndProfilerInstance}
import javax.inject.{Inject, Singleton}
import job.runner.JobDataBuilder
import livy.interactor.{LivyInteractor, LivyInteractorException}
import livy.session.models.LivyRequests.SubmitBatchJob
import livy.session.models.LivyResponses.{Batch, StateOfBatchJob}
import org.apache.hadoop.fs.Path
import play.api.{Configuration, Logger}
import play.api.libs.json.{JsNull, JsObject, JsValue, Json}
import profilers.dryrun.errorhandling.{DryRunNotFoundException, FailedToSubmitDryRunToLivyException, ProfilerMissingException}
import profilers.dryrun.models.Requests.{ClearDryRun, GetDryRun, TriggerDryRun}
import profilers.dryrun.models.Responses.{DryRun, DryRunStatus}
import profilers.dryrun.models.{Cause, DryRunInstance, DryRunOutPutSchema, DryRunOutPutStatus}
import profilers.dryrun.parsers.DryRunOutPutParser._
import profilers.metadata.hdfs.HDFSFileHandler
import repo.{DryRunRepo, ProfilerInstanceRepo, ProfilerRepo}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class DryRunManagementService @Inject()(configuration: Configuration,
                                        livyInteractor: LivyInteractor,
                                        profilerRepo: ProfilerRepo,
                                        profilerInstanceRepo: ProfilerInstanceRepo,
                                        dryRunRepo: DryRunRepo,
                                        jobDataBuilder: JobDataBuilder,
                                        actorSystem: ActorSystem)(implicit exec: ExecutionContext) {
  private val extraJars: List[String] = {
    val extraJarsPropertyValue: String = configuration.getString("dpprofiler.extra.jars").getOrElse("").trim

    if (extraJarsPropertyValue.isEmpty) {
      Nil
    }
    else {
      extraJarsPropertyValue.split(",").toList
    }
  }

  val activeStates = Set(StateOfBatchJob.recovering,
    StateOfBatchJob.idle, StateOfBatchJob.running, StateOfBatchJob.busy, StateOfBatchJob.shutting_down)


  private val scheduledRefresh: Cancellable =
    actorSystem.scheduler.schedule(
      0 seconds,
      4 seconds)(checkStatusOfJobsAndPopulateResults())


  private def processAndUpdateDryRun(dryRun: DryRunInstance): Future[Unit] = {
    livyInteractor.getBatchJob(dryRun.applicationId)
      .flatMap(
        batchJob =>
          updateDryRun(dryRun, batchJob)
      ).map(
      newDryRunOpt =>
        newDryRunOpt.foreach(
          newDryRun => Logger.info(s"updated dry run :$newDryRun , prev_status:${dryRun.dryRunStatus}")
        )
    )
  }


  private def updateDryRun(dryRunInstance: DryRunInstance, job: Batch): Future[Option[DryRunInstance]] = {
    val newDryRunInstance: DryRunInstance = job.state match {
      case StateOfBatchJob.not_started if dryRunInstance.dryRunStatus != DryRunStatus.SUBMITTED =>
        dryRunInstance.copy(dryRunStatus = DryRunStatus.SUBMITTED)
      case StateOfBatchJob.starting if dryRunInstance.dryRunStatus != DryRunStatus.SUBMITTED =>
        dryRunInstance.copy(dryRunStatus = DryRunStatus.SUBMITTED)
      case x if activeStates.contains(x) && dryRunInstance.dryRunStatus != DryRunStatus.RUNNING =>
        dryRunInstance.copy(dryRunStatus = DryRunStatus.RUNNING)
      case StateOfBatchJob.error if dryRunInstance.dryRunStatus != DryRunStatus.FAILED =>
        val cause = Cause(List("Spark Job for dry run failed"), job.log.mkString("\n"))
        dryRunInstance.copy(dryRunStatus = DryRunStatus.FAILED, cause = Json.toJson(cause))
      case StateOfBatchJob.not_found if dryRunInstance.dryRunStatus != DryRunStatus.FAILED =>
        val cause = Cause(List("Information about dry run lost"), job.log.mkString("\n"))
        dryRunInstance.copy(dryRunStatus = DryRunStatus.FAILED, cause = Json.toJson(cause))
      case StateOfBatchJob.dead if dryRunInstance.dryRunStatus != DryRunStatus.FAILED =>
        val cause = Cause(List("Internal error "), job.log.mkString("\n"))
        dryRunInstance.copy(dryRunStatus = DryRunStatus.FAILED, cause = Json.toJson(cause))
      case StateOfBatchJob.success if !Set(DryRunStatus.FAILED, DryRunStatus.SUCCESSFUL).contains(dryRunInstance.dryRunStatus) =>
        val handler = HDFSFileHandler(CommonUtils.fileSystem, new Path(dryRunInstance.outputFile))
        if (handler.exists && handler.isFile) {
          val schema = Json.parse(handler.readContent).as[DryRunOutPutSchema]
          handler.deleteFile
          schema.status match {
            case DryRunOutPutStatus.FAILURE =>
              val cause = schema.cause.getOrElse(Cause(List("Results are not available in output file"), "Not Available"))
              dryRunInstance.copy(dryRunStatus = DryRunStatus.FAILED, cause = Json.toJson(cause))
            case DryRunOutPutStatus.SUCCESSFUL =>
              dryRunInstance.copy(dryRunStatus = DryRunStatus.SUCCESSFUL, results = schema.results.getOrElse(JsNull))
          }
        }
        else {
          val cause = Cause(List(s"Failed to collect dry run output from ${handler.path}"), "Not Available")
          dryRunInstance.copy(dryRunStatus = DryRunStatus.FAILED, cause = Json.toJson(cause))
        }
      case _ =>
        dryRunInstance
    }
    if (dryRunInstance != newDryRunInstance)
      dryRunRepo.update(newDryRunInstance).map(_ => Some(newDryRunInstance))
    else
      Future.successful(None)
  }

  private def checkStatusOfJobsAndPopulateResults(): Unit = {
    Logger.info("Triggering scheduled status check of dry runs")
    val dryRunsInSubmittedState: Future[Seq[DryRunInstance]] = dryRunRepo.findAllWithStatus(DryRunStatus.SUBMITTED)
    val dryRunsInRunningState: Future[Seq[DryRunInstance]] = dryRunRepo.findAllWithStatus(DryRunStatus.RUNNING)
    val dryRunsToLookThrough = Future.sequence(List(dryRunsInRunningState, dryRunsInSubmittedState)).map(
      _.flatten.toList
    )
    dryRunsToLookThrough.flatMap(
      dryRuns => Future.sequence(dryRuns.map(processAndUpdateDryRun))
    )
  }


  private def profilersWithDryRunSchema: Future[Map[String, Profiler]] = {
    profilerRepo.findAll().map(
      _.flatMap(p => p.dryRunSchema.map(_ => p.name -> p)).toMap
    )
  }


  private def getRandomUniqueId(profiler: String, instance: String): String =
    s"${UUID.randomUUID().toString}_${profiler}_${instance}_${System.currentTimeMillis()}"

  private def getOutputPath(id: String, rootPath: String): String = {
    new Path(rootPath, s"${id}_dryrun_results.json").toString
  }

  private def getSettingsPath(id: String, rootPath: String): String = {
    new Path(rootPath, s"${id}_dryrun_settings.json").toString
  }


  def getStatus(request: GetDryRun): Future[DryRun] =
    dryRunRepo.findOne(request.id).flatMap {
      case None => Future.failed(DryRunNotFoundException(s"id: ${request.id}"))
      case Some(dryRunInstance) =>
        Future.successful(DryRun(dryRunInstance.id, dryRunInstance.profiler, dryRunInstance.instance, dryRunInstance.dryRunStatus, dryRunInstance.results, dryRunInstance.cause))
    }


  def clearDryRun(request: ClearDryRun): Future[Unit] = {
    dryRunRepo.findOne(request.id).flatMap {
      case None => Future.failed(DryRunNotFoundException(s"id: ${request.id}"))
      case Some(dryRunInstance) =>
        Future.successful(dryRunInstance)
    } map (
      dryRun => {
        val handler = HDFSFileHandler(CommonUtils.fileSystem, new Path(dryRun.outputFile))
        handler.deleteFile
        dryRunRepo.deleteOne(request.id)
      }
      )
  }


  def saveSettings(settingsPath: String, settings: JsValue): Future[Unit] = {
    val settingsAsString = Json.stringify(settings)
    val handler = HDFSFileHandler(CommonUtils.fileSystem, new Path(settingsPath))
    handler.createFileIfNotExist(settingsAsString)
  }


  def triggerDryRun(triggerDryRun: TriggerDryRun): Future[DryRun] = {
    Logger.info(s"Triggering dry run $triggerDryRun")
    val probableProfilerInstanceAndProfiler: Future[ProfilerAndProfilerInstance] = profilersWithDryRunSchema.flatMap(
      profilersHavingDryRunSchema => profilersHavingDryRunSchema.get(triggerDryRun.profiler) match {
        case None => Future.failed(ProfilerMissingException(s"No profiler with name ${triggerDryRun.profiler} exists or no dry run schema is define"))
        case Some(profiler) => Future.successful(profiler)
      }
    ).flatMap(
      _ => {
        profilerInstanceRepo.findByNameOpt(triggerDryRun.instance).flatMap {
          case None => Future.failed(ProfilerMissingException(s"No profiler instance with name ${triggerDryRun.instance} " +
            s"for profiler ${triggerDryRun.profiler} exists"))
          case Some(instance) => Future.successful(instance)
        }
      }
    )
    val eventualBatchJob: Future[(String, String, Batch)] = probableProfilerInstanceAndProfiler.flatMap(
      profilerInstanceAndProfiler => {
        val randomId: String = getRandomUniqueId(triggerDryRun.profiler, triggerDryRun.instance)
        val handler = HDFSFileHandler(CommonUtils.fileSystem, new Path(profilerInstanceAndProfiler.profiler.dryRunSchema.get.outputPath))
        handler.mkDirIfNotExists.flatMap(
          _ => {
            val hdfsFilePathForDryRunOutPutPath: String = getOutputPath(randomId, profilerInstanceAndProfiler.profiler.dryRunSchema.get.outputPath)
            val settingsPath = getSettingsPath(randomId, profilerInstanceAndProfiler.profiler.dryRunSchema.get.outputPath)
            saveSettings(settingsPath, triggerDryRun.dryRunSettings).flatMap(
              _ =>
                launchDryRun(profilerInstanceAndProfiler, randomId, hdfsFilePathForDryRunOutPutPath, settingsPath).map(
                  (randomId, hdfsFilePathForDryRunOutPutPath, _)
                ) recoverWith {
                  case exception: LivyInteractorException =>
                    Future.failed(FailedToSubmitDryRunToLivyException("Failed to submit dry run to livy", exception))
                }
            )

          }
        )

      }
    )

    eventualBatchJob.flatMap(
      bathJob => {
        val dryRunInstance = DryRunInstance(None, triggerDryRun.profiler, triggerDryRun.instance, DryRunStatus.SUBMITTED, bathJob._2, JsNull, JsNull, None, bathJob._3.id)
        dryRunRepo.save(dryRunInstance).map(x =>
          DryRun(x.id, triggerDryRun.profiler, triggerDryRun.instance, DryRunStatus.SUBMITTED, JsNull, JsNull))
      }
    )
  }


  private def createBatchJobRequestFromProfilerConfigurations(dryRunId: String,
                                                              profilerConfig: JsObject,
                                                              args: JsValue, queue: String): SubmitBatchJob = {
    val file = (profilerConfig \ "file").as[String]
    val proxyUser = (profilerConfig \ "proxyUser").asOpt[String]
    val className = (profilerConfig \ "className").as[String]
    val directDependencies: List[String] = (profilerConfig \ "jars").asOpt[List[String]].getOrElse(List.empty[String])
    val jars: List[String] = directDependencies ::: extraJars
    val files = (profilerConfig \ "files").asOpt[List[String]]
    val driverMemory = (profilerConfig \ "driverMemory").asOpt[String]
    val driverCores = Some(1)
    val executorMemory = (profilerConfig \ "executorMemory").asOpt[String]
    val executorCores = Some(1)
    val numExecutors = Some(1)
    val archives = (profilerConfig \ "archives").asOpt[List[String]]
    val conf = (profilerConfig \ "conf").asOpt[Map[String, String]]
    val pyFiles = (profilerConfig \ "pyFiles").asOpt[List[String]]
    SubmitBatchJob(s"dry run $dryRunId", file, proxyUser, className, List(args.toString()),
      Some(jars), files, driverMemory, driverCores, executorMemory, executorCores, numExecutors, archives, Some(queue), conf, pyFiles)
  }

  private def launchDryRun(profilerInstanceAndProfiler: ProfilerAndProfilerInstance
                           , dryRunId: String,
                           hdfsFilePathForDryRunOutPut: String,
                           dryRunSettingsFile: String): Future[Batch] = {
    val defaultArgs = jobDataBuilder.getJson(profilerInstanceAndProfiler, JobTask(profilerInstanceAndProfiler.profilerInstance.name))
    val dryRunSpecificInputs = Json.obj(
      "is_dry_run" -> true,
      "dry_run_settings_file" -> dryRunSettingsFile,
      "dry_run_output_path" -> hdfsFilePathForDryRunOutPut
    )
    val modifiedArgs = defaultArgs ++ dryRunSpecificInputs
    val batchJobRequest = createBatchJobRequestFromProfilerConfigurations(dryRunId,
      profilerInstanceAndProfiler.profiler.profilerConf, modifiedArgs, profilerInstanceAndProfiler.profilerInstance.queue)
    livyInteractor.submitBatchJob(batchJobRequest)
  }

}
