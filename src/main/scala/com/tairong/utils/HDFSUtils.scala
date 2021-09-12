/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.tairong.utils

import java.io.File
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger

import scala.io.Source

object HDFSUtils {
  private[this] val LOG = Logger.getLogger(this.getClass)

  def getFileSystem(namenode: String = null): FileSystem = {
    val conf = new Configuration()
    if (namenode != null) {
      conf.set("fs.default.name", namenode)
      conf.set("fs.defaultFS", namenode)
    }
    FileSystem.get(conf)
  }

  def list(path: String): List[String] = {
    val system = getFileSystem()
    try {
      system.listStatus(new Path(path)).map(_.getPath.getName).toList
    } finally {
      system.close()
    }
  }

  def exists(path: String): Boolean = {
    val system = getFileSystem()
    try {
      system.exists(new Path(path))
    } finally {
      system.close()
    }
  }

  def getContent(path: String): String = {
    val system      = getFileSystem()
    val inputStream = system.open(new Path(path))
    try {
      Source.fromInputStream(inputStream).mkString
    } finally {
      system.close()
    }
  }

  def saveContent(path: String,
                  content: String,
                  charset: Charset = Charset.defaultCharset()): Unit = {
    val system       = getFileSystem()
    val outputStream = system.create(new Path(path))
    try {
      outputStream.write(content.getBytes(charset))
    } finally {
      outputStream.close()
    }
  }

  def upload(localPath: String, remotePath: String, namenode: String = null): Unit = {
    try {
      val localFile = new File(localPath)
      if (!localFile.exists() || localFile.length() <= 0) {
        return
      }
    } catch {
      case e: Throwable =>
        LOG.warn("check for empty local file error, but you can ignore this check error. " +
                   "If there is empty  file in your hdfs, please delete it manually",
                 e)
    }
    val system = getFileSystem(namenode)
    try {
      system.copyFromLocalFile(new Path(localPath), new Path(remotePath))
    } finally {
      system.close()
    }
  }

  def downLoad(srcPath:String,targetPath:String,namenode: String = null): Unit = {
    LOG.info(s"begin to download csv files from $srcPath to $targetPath")
    val src = new Path(srcPath)
    val dst = new Path(targetPath)
    val localFile = new File(targetPath)
    try {
      if (localFile.exists()) {
        localFile.delete()
      }
    } catch {
      case e: Throwable => {
        LOG.warn(s"local file $localFile delete failure", e)
        localFile.delete()
      }
    }
    val system = getFileSystem(namenode)
    try {
      system.copyToLocalFile(false, src, dst, true)
    } finally {
      system.close()
    }
  }

  def delete(file:String,namenode: String = null) = {
    val system = getFileSystem(namenode)
    if(system.exists(new Path(file))){
      system.delete(new Path(file),true)
    }
  }

  def create(path:String,namenode: String = null) = {
    try {
      val system = getFileSystem(namenode)
      if (!system.exists(new Path(path))) {
        system.mkdirs(new Path(path))
      }
    } catch {
      case e: Throwable => {
        LOG.error("[mkdir] 创建文件目录失败,path=[{}]", path, e)
      }
    }
  }
}
