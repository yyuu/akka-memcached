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

    var currentMap: HashMap[ActorRef, Set[(String, Option[ByteString])]] = new HashMap
    var nextMap: HashMap[ActorRef, Set[(String, Option[ByteString])]] = new HashMap

    override def preStart {
        connection = IOManager(context.system) connect new InetSocketAddress(port)
    }

    /**
     * Adds this get request, along with the requesting actor, to the IOActor's
     * internal state. If there is a get currently in progress, the request is
     * placed in a queued map, and will be executed after the current request
     * is completed
     */
    def loadGetsToMap(actor: ActorRef, keys: Set[String]) {
        val map = if (awaitingGetResponse) nextMap else currentMap
        val valuesToMap: Set[(String, Option[ByteString])] = keys map { key => (key -> None) }
        map.get(actor) match {
            case Some(existingKeys) => map += ((actor, existingKeys ++ valuesToMap))
            case _                  => map += ((actor, valuesToMap))
        }
    }

    /**
     * Sends NotFound messages for any keys in the current map that do not yet
     * have a value. This should be used after memcached has sent END
     */
    def sendNotFoundMessages() {
        val missingKeys: List[(ActorRef, String)] = currentMap.flatMap {
            case (actor, keys) => keys.flatMap{
                case (key, None) => Some((actor, key))
                case _           => None
            }
        }.toList
        missingKeys.foreach {
            case (actor, key) => {
                //println("Not found: " + actor + key)
                actor ! NotFound(key)
            }
        }
    }

    /**
     * Writes the command to memcached to get the keys from the currentMap, if
     * writing is allowed.
     */
    def writeGetCommandToMemcachedIfPossible() {
        if (!awaitingGetResponse) {
            val keys = currentMap.flatMap {
                case (actor, keys) => keys.map(_._1)
            }.toSet

            if (keys.size > 0) {
                connection.write(GetCommand(keys).toByteString)
                awaitingGetResponse = true
            } else {
                awaitingGetResponse = false
            }
        }
    }

    /**
     * Places the values from nextMap into currentMap. If nextMap contains any
     * keys that have been defined in currentMap, this will send the appropriate
     * Found messages for those keys
     */
    def swapMaps() {
        val foundKeys: HashMap[String, ByteString] = currentMap.flatMap{
            case (actor, keys) =>
                keys.flatMap {
                    case (key, Some(value)) => Some((key, value))
                    case _                  => None
                }
        }
        currentMap = nextMap

        currentMap.flatMap{
            case (actor, keys) =>
                keys.flatMap {
                    case (key, valueOption) => foundKeys.get(key) match {
                        case Some(value) => {
                            actor ! Found(key, value)
                            None
                        }
                        case _ => Some((key, valueOption))
                    }
                }
        }
        nextMap = HashMap.empty
        //println("The current map is: " + currentMap)
    }

    /**
     * This is triggered when Memcached sends an END. Any key in the current get
     * map that does not have a value at this point was not found by the client.
     */
    def getCommandCompleted() {
        awaitingGetResponse = false
        sendNotFoundMessages()
        swapMaps()
        writeGetCommandToMemcachedIfPossible()
    }

    val iteratee = Iteratees.processLine

    var awaitingGetResponse = false

    def receive = {
        case raw: ByteString =>
            println("Raw: " + raw)
            connection write raw

        case get @ GetCommand(keys) =>
            loadGetsToMap(sender, keys)
            writeGetCommandToMemcachedIfPossible()
            awaitingGetResponse = true

        case delete @ DeleteCommand(keys) =>
            connection write delete.toByteString

        case set @ SetCommand(key, payload, ttl) =>
            connection write set.toByteString

        case IO.Read(socket, bytes) =>
            iteratee(IO Chunk bytes)
            iteratee.map{ data =>
                Iteratees.processLine
            }

        case found: Found => {
            val requestingActors = currentMap.filter{
                case (actor, keys) =>
                    keys.map(_._1).contains(found.key)
            }.map(_._1)
            requestingActors foreach { actor =>
                actor ! found
            }

            currentMap = currentMap.map{
                case (actor, keys) =>
                    val newKeys = keys.map{
                        case (found.key, None) => (found.key, Some(found.value))
                        case other             => other
                    }
                    (actor, newKeys)
            }
        }

        case Finished => getCommandCompleted()
    }

}

sealed trait GetResult {
    def key: String
}

case class Found(key: String, value: ByteString) extends GetResult

case class NotFound(key: String) extends GetResult

class MemcachedClientActor extends Actor {
    implicit val ec = ActorSystem()
    var originalSender: ActorRef = _
    val ioActor = Tester.ioActor

    var getMap: HashMap[String, Option[GetResult]] = new HashMap

    def maybeSendResponse() = {
        if (!getMap.values.toList.contains(None)) originalSender ! getMap.values.flatten
    }

    def receive = {
        case command @ GetCommand(keys) => {
            originalSender = sender
            getMap ++= keys.map {
                key =>
                    key -> None
            }
            ioActor ! command
        }
        case result: GetResult => {
            getMap -= result.key
            getMap += ((result.key, Some(result)))
            maybeSendResponse()
        }
    }
}
