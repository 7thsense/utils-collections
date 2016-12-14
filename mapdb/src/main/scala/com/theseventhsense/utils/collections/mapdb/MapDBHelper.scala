package com.theseventhsense.utils.collections.mapdb

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicLong

import com.theseventhsense.utils.collections.mapdb.MapDBOffHeapMap._
import org.mapdb.{DBMaker, DB}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by erik on 12/29/15.
  */
object MapDBHelper {

  val openCount = new AtomicLong()

  case class DBWrapper(path: Path, handle: DB)

  def db: DBWrapper = {
    val tmpPath = Files.createTempFile("MapDBHelper", ".tmp")
//    logger.debug(s"Creating tmpFile $tmpPath")
    val handle = DBMaker
    //      .newMemoryDirectDB()
      .newFileDB(tmpPath.toFile)
      .closeOnJvmShutdown()
      .deleteFilesAfterClose()
      .mmapFileEnable()
      .mmapFileCleanerHackDisable()
      .transactionDisable()
      //.asyncWriteEnable()
      //      .commitFileSyncDisable()
      //.freeSpaceReclaimQ(1)
      //      .asyncWriteFlushDelay(10)
      //      .asyncWriteQueueSize(10*32*1024)
      //      .allocateStartSize(100 * 1024*1024)  // 100MB
      //      .allocateIncrement(20 * 1024*1024)  // 20MB
//      .cacheSoftRefEnable()
      //        .cacheHardRefEnable()
      //.cacheDisable()
      .cacheLRUEnable()
      //.cacheSize(1024)
      //.cacheSize(1024 * 1024)
      .compressionEnable()
      .make()
    val count = openCount.incrementAndGet
//    logger.debug(s"Created tmpFile $tmpPath - $count open")
    DBWrapper(tmpPath, handle)
  }

  def release(db: DBWrapper): Long = {
    if (db != persistentDb && !db.handle.isClosed) {
      val sizeInMb = Files.size(db.path).toDouble / (1024D * 1024D)
      db.handle.close()
      val count = openCount.decrementAndGet()
//      logger.debug(s"Closing db file ${db.path} of $sizeInMb MB - $count still open")
      count
    } else {
      openCount.get
    }
  }

  def getOrCreateHSet[T](name: String, db: DBWrapper = db): mutable.Set[T] =
    db.handle.getHashSet(name).asInstanceOf[java.util.Set[T]].asScala
}
