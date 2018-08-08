package com.iterium.tmdb.utils

import java.nio.file.FileSystems.getDefault

object FileUtils {
  def buildFilePath(folder: String, fileName: String): String = {
    s"$folder" + getDefault.getSeparator + fileName
  }
}
