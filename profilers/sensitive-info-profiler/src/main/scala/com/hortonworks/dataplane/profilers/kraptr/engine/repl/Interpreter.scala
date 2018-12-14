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

package com.hortonworks.dataplane.profilers.kraptr.engine.repl

import com.hortonworks.dataplane.profilers.kraptr.behaviour.Decider
import com.hortonworks.dataplane.profilers.kraptr.common.KraptrContext
import com.typesafe.scalalogging.LazyLogging
import javax.script.{ScriptEngine, ScriptEngineManager}
import org.apache.hadoop.fs.FileSystem

import scala.tools.nsc.Settings
import scala.util.{Failure, Success, Try}

class Interpreter(context: KraptrContext) extends LazyLogging {
  val contextClassLoader: ClassLoader = Thread.currentThread.getContextClassLoader
  val genericEngine: ScriptEngine = new ScriptEngineManager().getEngineByName("scala")
  val settings: Settings = genericEngine.asInstanceOf[scala.tools.nsc.interpreter.IMain].settings
  settings.usejavacp.value = true
  settings.embeddedDefaults[DummyClass]

  Thread.currentThread.setContextClassLoader(contextClassLoader)

  //Workaround for loading hdfs paths in classpath
  logger.info("Setting up classpath for interpreter")

  logger.info(s"Default classpath of interpretor obtained " +
    s"from inbuilt classloader is ${settings.classpath.value}")

  private val hdfsClassPathTransformer =
    new HDFSClassPathTransformer(FileSystem.get(context.spark.sparkContext.hadoopConfiguration))

  private val tryHdfsPaths: Try[List[String]] = hdfsClassPathTransformer.processHDFSDependenciesInClasspath(settings.classpath.value)
  tryHdfsPaths match {
    case Success(hdfsPaths) =>
      hdfsPaths.foreach(classPathEntry => {
        logger.info(s"Appending $classPathEntry to interpreter classpath")
        settings.classpath.append(classPathEntry
        )
      })
    case Failure(exception) =>
      logger.error("Failed to load jars in hdfs", exception)
  }

  context.dslLibraryPath.foreach(dslLibraryPath => {
    logger.info(s"Searching for jars in dsl library path $dslLibraryPath")
    hdfsClassPathTransformer.searchInHDFSLibraryPath(dslLibraryPath) match {
      case Success(hdfsPaths) =>
        hdfsPaths.foreach(classPathEntry => {
          logger.info(s"Appending $classPathEntry to interpreter classpath")
          settings.classpath.append(classPathEntry
          )
        })
      case Failure(exception) =>
        logger.error("Failed to load jars in hdfs", exception)
    }
  })

  logger.info(s"Classpath of interpreter is ${settings.classpath.value}")


  def define(code: String, errorMessage: String = s"Failed to load code"): Try[Unit] = {
    val contextClassLoader: ClassLoader = Thread.currentThread.getContextClassLoader
    val triedLoadCode: Try[Unit] = Try(genericEngine.eval(code))
    triedLoadCode match {
      case Failure(error) =>
        logger.error(s"$errorMessage , code:\n $code", error)
      case Success(_) =>
        logger.info(s"Defined :\n $code")
    }
    Thread.currentThread.setContextClassLoader(contextClassLoader)
    triedLoadCode
  }


  def createBinding[A](instance: A, name: String): Try[Unit] = {
    val contextClassLoader: ClassLoader = Thread.currentThread.getContextClassLoader
    val tryCreatingBinding = Try(
      genericEngine.put(name, instance)
    )
    tryCreatingBinding match {
      case Failure(error) =>
        logger.error(s"Failed to create binding for instance  $name: $instance", error)
      case Success(_) =>
        logger.info(s"Loaded binding $name")
    }
    Thread.currentThread.setContextClassLoader(contextClassLoader)
    tryCreatingBinding

  }

  def buildBehaviour(dsl: String): Try[Decider] = {
    val contextClassLoader: ClassLoader = Thread.currentThread.getContextClassLoader
    val triedLoadCode: Try[Decider] = Try(genericEngine.eval(dsl).asInstanceOf[Decider])
    triedLoadCode match {
      case Failure(error) =>
        logger.error(s"Failed to build behaviour , dsl:\n $dsl", error)
      case Success(_) =>
        logger.info(s"Loaded dsl:\n $dsl")
    }
    Thread.currentThread.setContextClassLoader(contextClassLoader)
    triedLoadCode
  }

  def close(): Unit = {
    val contextClassLoader: ClassLoader = Thread.currentThread.getContextClassLoader
    genericEngine.asInstanceOf[scala.tools.nsc.interpreter.IMain].close()
    Thread.currentThread.setContextClassLoader(contextClassLoader)
  }
}
