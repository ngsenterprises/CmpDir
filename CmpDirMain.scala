

package com.ngs.cmp

import com.ngs.cmp.Actors._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import akka.actor.ActorSystem

object CmpMain extends App {

  val sys = ActorSystem("DirCmpSystem")
  val supervisor = sys.actorOf(CmpSupervisor.props, name = "CmpSupervisor")
  val parms = new HashMap[String, String]()
  val files = new ListBuffer[String]()

  util.parseCmdArgs(args, files, parms)
  //files.foreach(fname => println("[" +fname +"]"))
  //parms.foreach(kv => println("key[" +kv._1 +"] value [" +kv._2 +"]"))
  util.GenCmpJobs(files.toList, parms, supervisor)
}
