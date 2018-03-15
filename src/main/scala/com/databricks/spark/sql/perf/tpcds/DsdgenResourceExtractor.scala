/*
 * Copyright 2018 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf.tpcds

import java.io.{File, FileOutputStream}
import java.nio.file.{Files, Paths}

import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.SystemUtils

/**
 * Extracts the platform-specific executable from the resource stream.
 */
object DsdgenResourceExtractor {
  private val dsdgenBinaryResources = Seq(
    "dsdgen",
    "tpcds.idx"
  )

  def extractAndGetTempDsdgenDir(dirName: String): String = {
    val resourceDir = if (SystemUtils.IS_OS_LINUX) {
      "dsdgen-kit/linux"
    } else if (SystemUtils.IS_OS_MAC_OSX) {
      "dsdgen-kit/osx"
    } else {
      throw new Exception(s"No dsdgen binary for platform: ${System.getProperty("os.name")}")
    }

    val tempDirPath = Files.createDirectory(Paths.get(dirName))
    val tempDireFile = new File(tempDirPath.toString)

    emitDsdgenResourceDirectory(resourceDir, tempDirPath.toString)
    tempDirPath.resolve("dsdgen").toFile.setExecutable(true)

    // If this file is already present (e.g. created by different thread), ignore
    tempDireFile.renameTo(new File(dirName))
    tempDireFile.deleteOnExit()

    dirName
  }

  def emitDsdgenResourceDirectory(resourceDir: String, localDir: String): Unit = {
    dsdgenBinaryResources.foreach(r => emitResourceFile(s"$resourceDir/$r", s"$localDir/$r"))
  }

  def emitResourceFile(resourceFilePath: String, localFilePath: String): Unit = {
    val file = new File(localFilePath)
    file.createNewFile()
    file.deleteOnExit()

    val in = this.getClass.getClassLoader.getResourceAsStream(resourceFilePath)
    val out = new FileOutputStream(file)

    try {
      IOUtils.copy(in, out)
    } finally {
      in.close()
      out.close()
    }
  }
}
