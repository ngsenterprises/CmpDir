
package com.ngs.cmp.Actors

import java.io.{FileWriter, File}

import akka.actor.SupervisorStrategy.{Escalate, Resume}
import akka.actor.{OneForOneStrategy, Actor, Props, ActorRef}
import akka.routing.{RoundRobinRoutingLogic, Router, ActorRefRoutee}
import com.typesafe.config.ConfigFactory
import scala.io.Source
import scala.util.control.NonFatal
import scala.collection.mutable.{Queue, HashMap, ListBuffer}


sealed trait CmpMsg
case class CmpJobMsg(bas: File, cmp: File, cmds: HashMap[String, String]) extends CmpMsg
case class CmpJobCompleteMsg(bas: File, cmp: File) extends CmpMsg
case object StopSystemMsg extends CmpMsg


object CmpFileActor {
  def props = Props(classOf[CmpFileActor])
}

class CmpFileActor extends Actor {

  def cmpFiles(bas: File, trg: File, cmds: HashMap[String, String]): Long = {
    println("cmpFiles [" +bas.getAbsolutePath +"]" +" [" +trg.getAbsolutePath +"]")
    var error_count = 0
    //if (bas.getName.compareTo("readme.txt") == 0)
    //  println
    if (bas.isFile && trg.isFile) {
      val srcBas = Source.fromFile(bas).getLines
      val srcTrg = Source.fromFile(trg).getLines
      val wrt = new FileWriter(trg.getAbsolutePath + ".cmp")

      var line_count = 0
      //var error_count = 0

      val lineBas = new StringBuilder
      val lineTrg = new StringBuilder
      val suffix = bas.getAbsolutePath.split('.').last

      val skip_hdr = cmds.contains("glb.fhdr") || cmds.contains(suffix + ".fhdr")
      val start_line = cmds.getOrElse("glb.fstart", cmds.getOrElse(suffix + ".fstart", "0")).toLong
      val max_lines = cmds.getOrElse("glb.flines", cmds.getOrElse(suffix + ".flines", Long.MaxValue.toString)).toLong
      val max_errors = cmds.getOrElse("glb.ferror", cmds.getOrElse(suffix + ".ferror", Long.MaxValue.toString)).toLong

      def getBasStr: Boolean = {
        if (srcBas.hasNext) {
          lineBas.clear()
          lineBas.append(srcBas.next)
          true
        } else false
      }
      def getTrgStr: Boolean = {
        if (srcTrg.hasNext) {
          lineTrg.clear()
          lineTrg.append(srcTrg.next)
          true
        } else false
      }

      var bBas = getBasStr
      var bTrg = getTrgStr

      //skip header
      if (skip_hdr) {
        var done = false
        while (bBas && !done) {
          if (0 < lineBas.length && lineBas(0) != '#')
            done = true
          else
            bBas = getBasStr
        }
        done = false
        while (bTrg && !done) {
          if (0 < lineTrg.length && lineTrg(0) != '#')
            done = true
          else
            bTrg = getTrgStr
        }
      }

      //start line
      while (line_count < start_line && bBas && bTrg) {
        line_count += 1
        bBas = getBasStr
        bTrg = getTrgStr
      }

      while (line_count < max_lines && error_count < max_errors && bBas && bTrg) {
        if (lineBas.compare(lineTrg.toString) != 0) {
          wrt.write(line_count.toString + "\n")
          wrt.write("[" + lineBas + "]\n")
          wrt.write("[" + lineTrg + "]\n")
          error_count += 1
        }
        line_count += 1
        bBas = getBasStr
        bTrg = getTrgStr
      }
      wrt.close
    }
    error_count
  }

  def receive = {
    case CmpJobMsg(bas, trg, cmds) => {
      val orgSender = sender
      cmpFiles(bas, trg, cmds)
      orgSender ! CmpJobCompleteMsg(bas, trg)
    }
  }
}

object CmpSupervisor {
  def props = Props(classOf[CmpSupervisor])
}

class CmpSupervisor extends Actor {
  val config = ConfigFactory.load()
  val numberOfActors = config.getInt("app.akka.numberOfActors")
  var activeJobs = 0
  val jobQue = new Queue[CmpJobMsg]()
  var startTime = System.currentTimeMillis()

  override def supervisorStrategy =
    OneForOneStrategy() {
      case e: Exception if (NonFatal(e)) => {
        println("supervisorStrategy RESUME " +e.toString)
        Resume
      }
      case e => {
        println("supervisorStrategy ESCALATE " +e.toString)
        Escalate
      }
    }

  val router = {
    val routees = Vector.fill(5) {
      val r = context.actorOf(CmpFileActor.props)
      context.watch(r)
      ActorRefRoutee(r)
    }
    Router(RoundRobinRoutingLogic(), routees)
  }

  def receive = {
    case CmpJobMsg(src, trg, hm) => {
      if (activeJobs < numberOfActors) {
        activeJobs += 1
        router.route(new CmpJobMsg(src, trg, hm), self)
      } else jobQue += new CmpJobMsg(src, trg, hm)
    }

    case CmpJobCompleteMsg(src, trg) => {
      println("JobComplete [" +src.getName +"][" +trg.getName +"]")
      //println("active " +activeJobs +" jobQue " +jobQue.size)
      activeJobs -= 1
      if (jobQue.isEmpty && activeJobs == 0)
        self ! StopSystemMsg
      else if (!jobQue.isEmpty && activeJobs < numberOfActors) {
        activeJobs += 1
        router.route(jobQue.dequeue(), self)
        //println("jobQue 2 " +jobQue.size.toString)
      }
    }


    case StopSystemMsg => {
      val completeTime = System.currentTimeMillis() -startTime
      println("StopSystemMsg time " +completeTime.toString)
      context.stop(self)
      System.exit(0)
    }
  }
}





object util {

  val fsep = File.separator +File.separator

  def cmpFileName(fbas: File, ftrg: File): Boolean = {
    println("cmpfilename +[" +fbas.getName +"][" +ftrg.getName +"]")
    val fn = fbas.getName
    if (0 < fn.length && fn.compareTo(ftrg.getName) == 0)
      true
    else
      false
  } //-----------------------------------------------------

  def parseCmdArgs(args : Array[String],
                   files: ListBuffer[String],
                   parms: HashMap[String, String])
  {
    println("parseCmdArgs")
    if (2 <= args.length) {
      var curFileType = "glb"
      files ++= args.filter(!_.contains("=")).toList
      var cmds = args.toList.drop(files.length)

      //iterate command list
      while (!cmds.isEmpty) {
        val cmd = cmds.head.split('=')
        if (cmd.length == 2) {
          if (cmd(0).compareToIgnoreCase("ftype") != 0) {
            parms += ((curFileType + "." + cmd(0)) -> cmd(1))
            parms += (curFileType -> curFileType)
          }
          else curFileType = cmd(1)
        }
        cmds = cmds.tail
      }
    }



  }//--------------------------------------

  def GenCmpJobs(files: List[String],
                 cmds: HashMap[String, String],
                 supervisor: ActorRef)
  {
    def go(fsrc: File, ftrg: File,
           cmds: HashMap[String, String],
           supervisor: ActorRef)
    {
      if (fsrc.isDirectory && ftrg.isDirectory) {
        val mapTrg = ftrg.listFiles.map(f => (f.getName, Some(f))).toMap
        var srcList = fsrc.listFiles

        while (!srcList.isEmpty) {
          val src = srcList.head
          val trg = mapTrg.apply(src.getName)
          trg match {
            case Some(f) => {
              if (f.isDirectory && src.isDirectory)
                go(src, f, cmds, supervisor)
              else if (f.isFile && src.isFile)
                supervisor ! new CmpJobMsg(src, f, cmds)
              else {
                println("[" + src.getPath + "] is " + (if (src.isDirectory) " is dir." else " is file."))
                println("[" + f.getPath + "] is " + (if (f.isDirectory) " is dir." else " is file."))
              }
            }
            case _ => {
              println("could not find " + src.getName + " in target list.")
            }
          }
          srcList = srcList.tail
        }
      }
    }

    if (2 <= files.length) {
      val fsrc = new File(files.head)
      var flist = files.tail
      while (!flist.isEmpty) {
        val ftrg = new File(flist.head)
        go(fsrc, ftrg, cmds, supervisor)
        flist = flist.tail
      }
    }
  }

}
