
package com.ngs.cmp.Actors

import java.io.{FileWriter, File}

import akka.actor.SupervisorStrategy.{Escalate, Resume, Restart}
import akka.actor.{OneForOneStrategy, Actor, Props, ActorRef}
import akka.routing.{RoundRobinRoutingLogic, Router, ActorRefRoutee}
import com.typesafe.config.ConfigFactory
import scala.collection.mutable
import scala.io.Source
import scala.util.control.NonFatal
import scala.collection.mutable.{ListBuffer, Queue, HashMap}
import scala.annotation._


sealed trait CmpMsg
case class CmpJobMsg(bas: File, cmp: File, cmds: HashMap[String, String], jobId: Int) extends CmpMsg
case class CmpJobCompleteMsg(bas: File, cmp: File, jobId: Int) extends CmpMsg
case object StopSystemMsg extends CmpMsg

case class CmpJob(bas: File, trg: File)


object CmpFileActor {
  def props = Props(classOf[CmpFileActor])
}

class CmpFileActor extends Actor {

  def cmpFiles(bas: File, trg: File, cmds: HashMap[String, String]): Long = {
   //println("cmpFiles [" +bas.getAbsolutePath +"]" +" [" +trg.getAbsolutePath +"]")

    var error_count = 0

    try {
      if (bas.isFile && trg.isFile) {
        val srcBas = Source.fromFile(bas).getLines
        val srcTrg = Source.fromFile(trg).getLines
        val wrt = new FileWriter(trg.getAbsolutePath + ".cmp")

        var line_count = 0
        val lineBas = new StringBuilder
        val lineTrg = new StringBuilder
        val suffix = bas.getAbsolutePath.split('.').last

        val skip_hdr = cmds.contains("glb.fhdr") || cmds.contains(suffix + ".fhdr")
        val start_line = cmds.getOrElse("glb.fstart", cmds.getOrElse(suffix + ".fstart", "0")).toLong
        val max_lines = cmds.getOrElse("glb.flines", cmds.getOrElse(suffix + ".flines", Long.MaxValue.toString)).toLong
        val max_errors = cmds.getOrElse("glb.ferror", cmds.getOrElse(suffix + ".ferror", Long.MaxValue.toString)).toLong

        def getBasStr: Boolean = {
          try {
            if (srcBas.hasNext) {
              lineBas.clear()
              lineBas.append(srcBas.next)
              true
            } else false
          } catch {
            case NonFatal(e) => false
            case e:Throwable => throw e
          }
        }
        def getTrgStr: Boolean = {
          try {
            if (srcTrg.hasNext) {
              lineTrg.clear()
              lineTrg.append(srcTrg.next)
              true
            } else false
          } catch {
            case NonFatal(e) => false
            case e:Throwable => throw e
          }
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
    } catch {
      case NonFatal(e) => {
        println("exception: " +e.toString)
      }
    }
    error_count
  }

  def receive = {
    case CmpJobMsg(bas, trg, cmds, jobId) => {
      val orgSender = sender
      //println("start job: " +jobId.toString)
      cmpFiles(bas, trg, cmds)
      orgSender ! CmpJobCompleteMsg(bas, trg, jobId)
    }
  }
}

object CmpSupervisor {
  def props = Props(classOf[CmpSupervisor])
}

class CmpSupervisor extends Actor {
  //val config = ConfigFactory.load()
  val numberOfActors = 7//config.getInt("app.akka.numberOfActors")
  var activeJobs = 0
  val jobQue = new Queue[CmpJobMsg]()
  var startTime = System.currentTimeMillis()

  override def supervisorStrategy =
    OneForOneStrategy() {
      case e: Exception if (NonFatal(e)) => {
        println("supervisorStrategy RESTART." +e.toString)
        Restart
      }
      case e => {
        println("supervisorStrategy ESCALATE." +e.toString)
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
    case CmpJobMsg(src, trg, hm, jobId) => {
      //println("CmpJobMsg src [" +src.getName +"] " +"trg [" +trg.getName +"]")
      if (activeJobs < numberOfActors) {
        activeJobs += 1
        router.route(new CmpJobMsg(src, trg, hm, jobId), self)
      } else jobQue += new CmpJobMsg(src, trg, hm, jobId)
    }

    case CmpJobCompleteMsg(src, trg, jobId) => {
      //println("JobComplete (" +jobId.toString +")")//[" +src.getName +"][" +trg.getName +"]")
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

  var nextId = 0

  def getNextId(): Int = {
    nextId += 1
    nextId
  }

  def parseCmdArgs(args: Array[String],
                   files: ListBuffer[String],
                   parms: HashMap[String, String]) {
    if (2 <= args.length) {
      var curFileType = "glb"
      parms += (curFileType -> curFileType)
      files ++= args.filter(!_.contains("=")).toList
      var cmds = args.toList.drop(files.length)

      //iterate command list
      while (!cmds.isEmpty) {
        val cmd = cmds.head.split('=')
        if (cmd.length == 2) {
          if (cmd(0).compareToIgnoreCase("ftype") == 0) {
            curFileType = cmd(1)
            parms += (curFileType -> curFileType)
          }
          else
            parms += ((curFileType + "." + cmd(0)) -> cmd(1))
        }
        cmds = cmds.tail
      }
    }
  } //--------------------------------------


  def GenCmpJobs(files: List[String],
                 cmds: HashMap[String, String],
                 supervisor: ActorRef) {
    if (2 <= files.length) {

      val fbasis = new File(files.head)
      val fileCmps = for (fn <- files.tail) yield (new File(fn))

      if (fbasis.isFile)
        fileCmps.foreach(f => if (f.isFile) supervisor ! new CmpJobMsg(fbasis, f, cmds, getNextId))
      else {
        var fcmps = fileCmps
        val basDirSplitList = fbasis.listFiles.partition(f => f.isFile)
        while (!fcmps.isEmpty) {
          val fcmp = fcmps.head
          if (fcmp.isDirectory) {
            val cmpSplit = fcmp.listFiles.partition(f => f.isFile)
            val fMap = cmpSplit._1.foldLeft(new HashMap[String, File]) { (acc, f) => (acc += (f.getName -> f))}
            val dMap = cmpSplit._2.foldLeft(new HashMap[String, File]) { (acc, f) => (acc += (f.getName -> f))}

            basDirSplitList._1.foreach(f => fMap.get(f.getName) match {
              case Some(cf) => supervisor ! new CmpJobMsg(f, cf, cmds, getNextId)
              case _ => ()
            })

            var dirJobs = basDirSplitList._2.foldLeft(new ListBuffer[CmpJob]) {
              (acc, d) => dMap.get(d.getName) match {
                  case Some(df) => acc += new CmpJob(d, df)
                  case _ => acc
            }}

            while (!dirJobs.isEmpty) {

              val dj = dirJobs.remove(0)

              val bSplitList = dj.bas.listFiles.partition(f => f.isFile)
              val cSplitList = dj.trg.listFiles.partition(f => f.isFile)
              val f_map = cSplitList._1.foldLeft(HashMap[String, File]()) { (acc, f) => (acc += (f.getName -> f))}
              val d_map = cSplitList._2.foldLeft(HashMap[String, File]()) { (acc, f) => (acc += (f.getName -> f))}

              bSplitList._1.foreach(f => f_map.get(f.getName) match {
                case Some(cf) => supervisor ! new CmpJobMsg(f, cf, cmds, getNextId)
                case _ => ()
              })

              dirJobs ++= bSplitList._2.foldLeft(new ListBuffer[CmpJob]) {
                (acc, d) => d_map.get(d.getName) match {
                  case Some(df) => acc += new CmpJob(d, df)
                  case _ => acc
                }}
            }
          }
          fcmps = fcmps.tail
        }
      }
    }
  }//--------------------------------------------
}
