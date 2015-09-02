package com.ngs.cmp.cmpActors

import java.io.{File, FileWriter}

import akka.actor.{Actor, ActorRef, Props, OneForOneStrategy, SupervisorStrategy}
import akka.actor.SupervisorStrategy._
import akka.actor.SupervisorStrategy.restart

import akka.routing.{ActorRefRoutee, Router, RoundRobinRoutingLogic}




import scala.collection.immutable.{HashMap, List}
import scala.io.Source
import scala.util.control.NonFatal
import scala.collection.mutable.{ListBuffer, Queue}


sealed trait CmpMsg
case class CmpJobMsg(bas: File, cmp: File, cmds: cmdParm, jobId: Int) extends CmpMsg
case class CmpJobCompleteMsg(bas: File, cmp: File, jobId: Int) extends CmpMsg
case object StopSystemMsg extends CmpMsg

case class CmpJob(bas: File, trg: File)
case class cmdParm(
  skipHdr: Boolean,
  startLine: Int,
  maxLines: Int,
  maxErrors: Int
)


object CmpFileActor {
  def props = Props(classOf[CmpFileActor])
}

class CmpFileActor extends Actor {

  def cmpFiles(bas: File, trg: File, cmds: cmdParm): Long = {
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

//        val skip_hdr = cmds.contains("glb.fhdr") || cmds.contains(suffix + ".fhdr")
//        val start_line = cmds.getOrElse("glb.fstart", cmds.getOrElse(suffix + ".fstart", "0")).toLong
//        val max_lines = cmds.getOrElse("glb.flines", cmds.getOrElse(suffix + ".flines", Long.MaxValue.toString)).toLong
//        val max_errors = cmds.getOrElse("glb.ferror", cmds.getOrElse(suffix + ".ferror", Long.MaxValue.toString)).toLong

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
        if (cmds.skipHdr) {
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
        while (line_count < cmds.startLine && bBas && bTrg) {
          line_count += 1
          bBas = getBasStr
          bTrg = getTrgStr
        }

        while (line_count < cmds.maxLines && error_count < cmds.maxErrors && bBas && bTrg) {
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
        println("supervisorStrategy RESTART. " +e.toString)
        restart
      }
      case e: Throwable => {
        println("supervisorStrategy ESCALATE. " +e.toString)
        escalate
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


