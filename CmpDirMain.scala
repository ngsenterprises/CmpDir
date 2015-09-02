
package com.ngs.cmp.cmpMain
/**
 * Created by ngsmith on 8/29/2015.
 */

import com.typesafe.config.ConfigFactory
import akka.actor._
import akka.actor.ActorSystem

import com.ngs.cmp.cmpUtils._
import com.ngs.cmp.cmpUtils
import com.ngs.cmp.cmpActors._
import com.ngs.cmp.cmpActors.CmpSupervisor

import scala.collection.mutable.{HashMap, ListBuffer}

object CmpMain extends App {
  val system = ActorSystem("CmpSupervisor")
  val supervisor = system.actorOf(CmpSupervisor.props)

  cmpUtils.CmpDirUtils.GenCmpJobs(supervisor)
}

