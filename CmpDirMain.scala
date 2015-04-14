

package com.ngs.cmp

import com.ngs.cmp.Actors._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import akka.actor.ActorSystem

object CmpMain extends App {

  val sys = ActorSystem("DirCmpSystem")
  val supervisor = sys.actorOf(CmpSupervisor.props, name = "CmpSupervisor")
  val parms = new HashMap[String, String]
  val files = new ListBuffer[String]

  util.parseCmdArgs(args, files, parms)
  util.GenCmpJobs(files.toList, parms, supervisor)
}
