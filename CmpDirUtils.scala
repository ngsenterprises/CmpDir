package com.ngs.cmp.cmpUtils

//import com.ngs.cmp.actors._
import com.ngs.cmp.cmpActors.CmpJobMsg
import com.ngs.cmp.cmpActors.cmdParm

//import com.ngs.cmp.actors.CmpFileActor

import java.io.File
import akka.actor.ActorRef
import com.typesafe.config.{ConfigFactory, Config}
import scala.collection.immutable.{List, HashMap}
//import scala.collection.mutable.{ListBuffer}
import scala.util.{Try,Success,Failure}


/**
 * Created by ngsmith on 8/30/2015.
 */

object CmpDirUtils {

  var nextId = 0

  def getNextId(): Int = {
    nextId += 1
    nextId
  }



  def GenCmpJobs(supervisor: ActorRef): Unit = {

    def nextDir(dBasis: File, dCmp: File, cmds: cmdParm, supervisor: ActorRef): Unit = {

      val basisSplit = dBasis.listFiles.partition(_.isFile)
      val cmpSplit = dCmp.listFiles.partition(_.isFile)
      val cmpFileMap = cmpSplit._1.foldLeft(HashMap[String, File]()) { (ac, f) => ac + (f.getName -> f)}
      val cmpDirMap = cmpSplit._2.foldLeft(Map[String, File]()) { (ac, f) => ac + (f.getName -> f)}

      basisSplit._1.foreach { f => {
        cmpFileMap.get(f.getName) match {
          case None => ()
          case Some(fcmp: File) => supervisor ! new CmpJobMsg(f, fcmp, cmds, getNextId)
        }
      }}

      basisSplit._2.foreach { d => {
        cmpDirMap.get(d.getName) match {
          case None => ()
          case Some(dcmp: File) => nextDir(d, dcmp, cmds, supervisor)
        }
      }}
    }//...........................................................

    val files = getCfgStringListOrElse("cmp-files", List[String]())
    val max_lines = getCfgIntOrElse("cmp-max-errors", Int.MaxValue)
    val max_errors = getCfgIntOrElse("cmp-max-errors", Int.MaxValue)
    val cmds = new cmdParm(getCfgBooleanOrElse("cmp-skip-hdr", false),
      getCfgIntOrElse("cmp-start-line", 0),
      if (0 < max_lines) max_lines else Int.MaxValue,
      if (0 < max_errors) max_errors else Int.MaxValue)

    if (files.length < 2)
      println("GenCmpJobs: Must have at least 2 files to compare")
    else {
      val fBasis = new File(files.head)
      val fCmps = files.tail.foldLeft(List[File]()) { (ac, s) => (new File(s)) :: ac }

      if (fBasis.isFile)
        fCmps.foreach { f => if (f.isFile) supervisor ! new CmpJobMsg(fBasis, f, cmds, getNextId) }
      else
        fCmps.foreach { d => nextDir(fBasis, d, cmds, supervisor) }
    }
  }

  def getCfgStringListOrElse(skey: String, sldef: List[String]): List[String] = {
    val cfg = ConfigFactory.load("application.conf")
    var strListRef:java.util.List[String] = new java.util.LinkedList[String]()
    try {
      strListRef = cfg.getStringList(skey)
    } catch {
      case x: Exception if (NonFatal(x)) => strListRef = new java.util.LinkedList[String]()
      case x: Throwable => throw x
    }

    if (strListRef == null) List[String]() else {
      val maxIndex = strListRef.size()
      var index = 0
      val buf = new scala.collection.mutable.ListBuffer[String]()
      while (index < maxIndex) {
        buf += strListRef.get(index)
        index += 1
      }
      buf.toList
    }
  }

  def getCfgIntOrElse(skey: String, idef: Int): Int = {
    val cfg = ConfigFactory.load("application.conf")
    Try(cfg.getInt(skey)) match {
      case Failure( _ ) => idef
      case Success(ival: Int) => ival
    }
  }

  def getCfgBooleanOrElse(skey: String, bdef: Boolean): Boolean = {
    val cfg = ConfigFactory.load("application.conf")
    Try(cfg.getBoolean(skey)) match {
      case Failure( _ ) => bdef
      case Success(bval: Boolean) => bval
    }
  }


}//end CmpDirUtils
