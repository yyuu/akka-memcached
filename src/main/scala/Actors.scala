package com.klout.akkamemcache

import akka.actor._
import akka.util.ByteString
import java.net.InetSocketAddress
import akka.dispatch.Future
import com.klout.akkamemcache.Protocol._ 
import scala.collection.mutable.HashMap
import com.klout.akkamemcache.Protocol._

class MemcachedIOActor extends Actor {
    def ascii(bytes: ByteString): String = bytes.decodeString("US-ASCII").trim
    implicit val ec = ActorSystem()
    

    val port = 11211

     var connection: IO.SocketHandle = _

     var getsMap:HashMap[ActorRef,Set[String]] = new HashMap

    override def preStart {
        connection = IOManager(context.system) connect new InetSocketAddress(port)
    }

    def loadGetToMap(actor: ActorRef, key: String ){
        getsMap.get(actor) match {
            case Some(existingKeys) => getsMap += ((actor,existingKeys + key))
            case _ => getsMap += ((actor,Set(key)))
        }
    }

    val iteratee = Iteratees.processLine 

    def receive = {
        case raw: ByteString =>
            println("Raw: "+raw)
            connection write raw

        case get @ GetCommand(key) =>
            //println("Get: " + key)
            loadGetToMap(sender, key)
            connection write get.toByteString
            //sender ! Found(key, "testResult")

        case delete @ DeleteCommand(key) => 
            println("Delete: " + key)
            connection write delete.toByteString

        case set @ SetCommand(key,payload,ttl) =>
            println("Set: " + key)
            connection write set.toByteString

        case IO.Read(socket, bytes) =>
            println("reading: " + ascii(bytes))
            iteratee(IO Chunk bytes)
            iteratee.map{data =>  
                Iteratees.processLine
            }

        case IO.NewClient(server) => {
            println("New Client")
            val socket = server.accept()
            iteratee.flatMap(_ => Iteratees.processLine)
        }

    }

}

sealed trait GetResult

case class NotFound(key: String) extends GetResult

case class Found(key: String, value: String) extends GetResult

class MemcachedClientActor extends Actor {
    implicit val ec = ActorSystem()
    var originalSender: ActorRef = _
    val ioActor = Tester.ioActor


    def receive = {
        case command: GetCommand => {
            originalSender = sender
            ioActor ! command
        }
        case result: GetResult => originalSender ! result
    }
}

