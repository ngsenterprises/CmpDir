
package com.ngs.cmp.actors

import java.io.File
import java.io.FileWriter

import scala.util.control._
import scala.collection.mutable.HashMap
import scala.io.Source
import scala.collection.mutable.Queue



import _root_.akka.actor.{ Actor, Props }
import akka.actor.SupervisorStrategy._
import akka.actor.OneForOneStrategy
import akka.routing.ActorRefRoutee
import akka.routing.Router
import akka.routing.RoundRobinRoutingLogic

import com.typesafe.config.ConfigFactory


sealed trait CmpMsg
case class StartSystemMsg(files: List[String], parms: HashMap[String, String]) extends CmpMsg
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
    if (bas.getName.compareTo("readme.txt") == 0)
      println
    try {
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
    } catch {
      case e: Exception => {
        println
        println(e)
        println(bas.getAbsolutePath +" and ")
        println(trg.getAbsolutePath)
        println(" are not comparable.")
        println
      }
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

  //println("CmpSupervisor")

  val config = ConfigFactory.load()
  val numberOfActors = config.getInt("app.akka.numberOfActors")
  var activeJobs = 0
  //println("numberOfActors = " +numberOfActors)
  val jobQue = new Queue[CmpJobMsg]()
  var startTime = System.currentTimeMillis()


  override def supervisorStrategy =
    OneForOneStrategy() {
      case e: Exception if (NonFatal(e)) => Stop
      case _ => Escalate
    }

  val router = {
    val routees = Vector.fill(5) {
      //println("routees")
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
      //println("JobComplete [" +src.getName +"][" +trg.getName +"]")
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

    case StartSystemMsg(files, cmds) => {
    //println("StartSystemMsg")
    }
  }
}
