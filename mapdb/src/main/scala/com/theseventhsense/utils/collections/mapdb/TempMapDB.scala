package com.theseventhsense.utils.collections.mapdb

import java.io.File
import java.nio.file.Files
import java.util.UUID

import org.mapdb.{ DB, DBMaker }

object TempMapDB {
  def apply(): TempMapDB = new TempMapDB(tempDbFile, UUID.randomUUID().toString)

  def tempDbFile: File = {
    val workspace = Files.createTempDirectory(this.getClass.getName)
      .toAbsolutePath
    workspace.resolve("temp.mapdb.db").toFile
  }

  def delete(f: File): Boolean = {
    f.delete() &&
      new File(f.toString + ".p").delete() &&
      new File(f.toString + ".t").delete() &&
      f.getParentFile.delete()
  }

  private def create(file: File, key: String): DB = {
    DBMaker
      .newFileDB(file)
      .asyncWriteEnable()
      .compressionEnable()
      .encryptionEnable(key)
      .mmapFileEnableIfSupported()
      .make()
  }

}

class TempMapDB(file: File, key: String) {

  import TempMapDB._

  val db = {
    val localDb = create(file, key)
    delete(file)
    localDb
  }

  def close(): Unit = {
    db.close()
    super.finalize()
  }
}
