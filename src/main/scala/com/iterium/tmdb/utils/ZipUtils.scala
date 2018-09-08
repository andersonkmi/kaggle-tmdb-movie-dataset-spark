package com.iterium.tmdb.utils

import java.io.{ IOException, FileOutputStream, FileInputStream, File }
import java.util.zip.{ ZipEntry, ZipInputStream }

object ZipUtils {
  def unZipIt(zipFile: String, outputFolder: String): Unit = {

    val buffer = new Array[Byte](1024)

    try {
      //output directory
      val folder = new File(outputFolder)
      if (!folder.exists()) {
        folder.mkdir()
      }

      //zip file content
      val zis: ZipInputStream = new ZipInputStream(new FileInputStream(zipFile))
      //get the zipped file list entry
      var ze: ZipEntry = zis.getNextEntry

      while (ze != null) {

        val fileName = ze.getName
        val newFile = new File(outputFolder + File.separator + fileName)

        System.out.println("file unzip : " + newFile.getAbsoluteFile)

        //create folders
        new File(newFile.getParent).mkdirs()

        val fos = new FileOutputStream(newFile)

        var len: Int = zis.read(buffer)

        while (len > 0) {

          fos.write(buffer, 0, len)
          len = zis.read(buffer)
        }

        fos.close()
        ze = zis.getNextEntry
      }

      zis.closeEntry()
      zis.close()

    } catch {
      case e: IOException => println("exception caught: " + e.getMessage)
    }

  }
}
