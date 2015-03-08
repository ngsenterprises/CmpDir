
package com.ngs.cmp

import com.ngs.cmp.actors.CmpSupervisor
import com.ngs.cmp.actors.CmpJobMsg

import java.io.File
import scala.collection.mutable.HashMap

import akka.actor.ActorSystem



object CmpDirMain extends App {

  val sys = ActorSystem("DirCmpSystem")
  val master = sys.actorOf(CmpSupervisor.props, name = "CmpSupervisor")
  val mapParms = new HashMap[String, String]()

  def cmpfilename(fBas: File, fTrg: File): Boolean = {
    //println("cmpfilename")
    val sep = File.separator + File.separator

    val basToks = fBas.getAbsolutePath.split(sep)
    val trgToks = fTrg.getAbsolutePath.split(sep)

    val baslen = basToks.length
    val trglen = trgToks.length

    if (0 < basToks.length && 0 < trgToks.length)
      basToks.last.compareTo(trgToks.last) == 0
    else
      false

  } //-----------------------------------------------------

  def genCmpJobsFromDir(fsrc: File, ftrg: File): Boolean = {
    if (fsrc.isDirectory && ftrg.isDirectory) {
      val sep = File.separator + File.separator
      val mapTrg = ftrg.listFiles.map(f => (f.getAbsolutePath.split(sep).last, Some(f))).toMap
      var list = fsrc.listFiles

      while (!list.isEmpty) {
        val src = list.head
        //fsrc.listFiles.foreach(src => {
        val trg = mapTrg.apply(src.getAbsolutePath.split(sep).last)
        trg match {
          case Some(f) => {
            if (f.isDirectory && src.isDirectory)
              genCmpJobsFromDir(src, f)
            else if (f.isFile && src.isFile) {
              master ! new CmpJobMsg(src, f, mapParms)
            }
            else {
              println("[" + src.getPath + "] is " + (if (src.isDirectory) " is dir." else " is file."))
              println("[" + f.getPath + "] is " + (if (f.isDirectory) " is dir." else " is file."))
            }
          }
          case _ => ()
        }
        list = list.tail
      }
      //})
      true
    }
    false
  } //-----------------------------------------

  if (2 <= args.length) {

    var curFileType = "glb"
    val filenames = args.filter(!_.contains("=")).toList
    //println(files.toList)
    var cmds = args.toList.drop(filenames.length)

    while (!cmds.isEmpty) {
      val p = cmds.head.split('=')
      if (p.length == 2) {
        if (p(0).compareToIgnoreCase("ftype") != 0) {
          mapParms += ((curFileType + "." + p(0)) -> p(1))
          mapParms += (curFileType -> curFileType)
        }
        else curFileType = p(1)
      }
      cmds = cmds.tail
    }

    val fbasis = new File(filenames.head)
    if (fbasis.isDirectory) {
      var list = filenames.tail
      while (!list.isEmpty) {
        val trg = new File(list.head)
        if (trg.isDirectory)
          genCmpJobsFromDir(fbasis, trg)
        else {
          println("[" + fbasis.getPath + "] is " + (if (fbasis.isDirectory) " is dir." else " is file."))
          println("[" + trg.getPath + "] is " + (if (trg.isDirectory) " is dir." else " is file."))
        }
        list = list.tail
      }
    }
    else if (fbasis.isFile) {
      var list = filenames.tail
      while (!list.isEmpty) {
        val fn = list.head
        val trg = new File(fn)
        if (trg.isFile)
          master ! new CmpJobMsg(fbasis, trg, mapParms)
        else {
          println("[" + fbasis.getPath + "] is " + (if (fbasis.isDirectory) " is dir." else " is file."))
          println("[" + trg.getPath + "] is " + (if (trg.isDirectory) " is dir." else " is file."))
        }
      }
      list = list.tail
    }
  }
}
