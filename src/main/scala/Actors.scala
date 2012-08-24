package com.klout.akkamemcache

import akka.actor._
import akka.util.ByteString
import java.net.InetSocketAddress
import akka.dispatch.Future
import com.klout.akkamemcache.Protocol._


object Messages {
    case class CommandWithActor(actor:ActorRef, command: Command)
}

class MemcachedIOActor extends Actor {
    implicit val ec = ActorSystem()
    import Messages._

    val port = 11211

    var connection: IO.SocketHandle = _

    var getsMap:Map[ActorRef,Set[String]] = Map.empty()

    override def preStart {
        connection = IOManager(context.system) connect new InetSocketAddress(port)
    }

    def loadGetToMap(actor: ActorRef, key: String ){
        getsMap.get(actor) match {
            case Some(existingKeys) => getsMap += ((actor,existingKeys + key))
            case _ => getsMap += ((actor,Set(key)))
        }
    }

    val state = IO.IterateeRef.Map.async[IO.Handle]()(context.dispatcher)

    def receive = {
        case CommandWithActor(actor, get @ GetCommand(key)) =>
            println("Get: " + key)
            loadGetToMap(actor, key)
            connection write get.toByteString

        case delete @ DeleteCommand(key) => 
            println("Delete: " + key)
            connection write delete.toByteString

        case set @ SetCommand(key,payload,ttl) =>
            println("Set: " + key)
            connection write set.toByteString

        case IO.Read(handle, bytes) =>
            println("reading: " + bytes)
            println(Iteratees.readLine(bytes))
    }

}

class MemCachedClientActor extends Actor {
    val ioActor = RealMemcachedClient.actor

    var originalSender: ActorRef = _
    
    def recieve = {
        case command @ GetCommand => {
            originalSender = sender
            ioActor ! command
        }
        case result @ GetResult => originalSender ! result
    }
}

sealed trait GetResult

case class NotFound(key: String) extends GetResult

case class Found(key: String, value: String) extends GetResult