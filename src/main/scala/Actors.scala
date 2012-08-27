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

    var currentMap: HashMap[ActorRef, Set[PotentialResult]] = new HashMap
    var nextMap: HashMap[ActorRef, Set[PotentialResult]] = new HashMap

    override def preStart {
        connection = IOManager(context.system) connect new InetSocketAddress(port)
    }

    /**
     * Adds this get request, along with the requesting actor, to the IOActor's
     * internal state. If there is a get currently in progress, the request is
     * placed in a queued map, and will be executed after the current request
     * is completed
     */
    def enqueueCommand(actor: ActorRef, keys: Set[String]) {
        val map = if (awaitingGetResponse) nextMap else currentMap
        val results: Set[PotentialResult] = keys.map(NotYetFound)
        map.get(actor) match {
            case Some(existingResults) => map += ((actor, existingResults ++ results))
            case _                     => map += ((actor, results))
        }
    }

    /**
     * Sends NotFound messages for any keys in the current map that do not yet
     * have a value. This should be used after memcached has sent END
     */
    def sendNotFoundMessages() {
        val missingKeys: List[(ActorRef, String)] = currentMap.flatMap {
            case (actor, results) => results.flatMap{
                case NotYetFound(key) => Some((actor, key))
                case _                => None
            }
        }.toList
        missingKeys.foreach {
            case (actor, key) => {
                actor ! NotFound(key)
            }
        }
    }

    /**
     * Sends Found messages for all actors who are interested in the found object.
     * Updates the ioActor's map to indicate that the keys have been found
     */
    def sendFoundMessages(found: Found) {
        val requestingActors = currentMap.flatMap{
            case (actor, results) if (results.exists(_.key == found.key)) => Some(actor)
            case _ => None
        }

        //println("requesting actors: " + requestingActors)

        requestingActors foreach { actor =>
            actor ! found
        }

        currentMap = currentMap.map{
            case (actor, results) =>
                val newKeys = results.map{
                    case NotYetFound(key) if key == found.key => found
                    case other                                => other
                }
                (actor, newKeys)
        }
    }

    /**
     * Writes the command to memcached to get the keys from the currentMap, if
     * writing is allowed.
     */
    def writeGetCommandToMemcachedIfPossible() {
        if (!awaitingGetResponse) {
            val missingKeys = currentMap.flatMap {
                case (actor, potentialResults) => potentialResults.map(_.key)
            }.toSet

            if (missingKeys.size > 0) {
                connection.write(GetCommand(missingKeys).toByteString)
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
            case (actor, results) =>
                results.flatMap {
                    case Found(key, value) => Some((key, value))
                    case _                 => None
                }
        }
        currentMap = nextMap

        currentMap.flatMap{
            case (actor, results) =>
                results.flatMap {
                    case NotYetFound(key) => foundKeys.get(key) match {
                        case Some(value) => {
                            actor ! Found(key, value)
                            None
                        }
                        case _ => Some(NotYetFound(key))
                    }
                    case _ => None
                }
        }
        nextMap = HashMap.empty
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
            enqueueCommand(sender, keys)
            writeGetCommandToMemcachedIfPossible()
            awaitingGetResponse = true

        case command: Command =>
            connection write command.toByteString

        /* Response from Memcached */
        case IO.Read(socket, bytes) =>
            iteratee(IO Chunk bytes)
            iteratee.map{ data =>
                Iteratees.processLine
            }

        /* A single get result has been returned */
        case found: Found => sendFoundMessages(found)

        /* A multiget has completed */
        case Finished     => getCommandCompleted()
    }

}

/* Stores the result of a Memcached get that has already been executed */
sealed trait GetResult {
    def key: String
}

/* Stores the result of a Memcached get that may not yet have been executed */
sealed trait PotentialResult {
    def key: String
}

case class Found(key: String, value: ByteString) extends GetResult with PotentialResult

/* This indicates a cache miss */
case class NotFound(key: String) extends GetResult

/* This class indicates that no result is currently available for
 * the given key. This is either a cache miss, or Memcache has not
 * yet responded */
case class NotYetFound(key: String) extends PotentialResult

class MemcachedClientActor extends Actor {
    implicit val ec = ActorSystem()
    var originalSender: ActorRef = _
    val ioActor = Tester.ioActor

    var getMap: HashMap[String, Option[GetResult]] = new HashMap

    /**
     * Sends the response back if the memcache command has completed
     */
    def maybeSendResponse() = {
        if (!getMap.values.toList.contains(None)) originalSender ! getMap.values.flatten
    }

    def receive = {

        /* This is a command to fetch results from Memcached */
        case command @ GetCommand(keys) => {
            originalSender = sender
            getMap ++= keys.map {
                key =>
                    key -> None
            }
            ioActor ! command
        }

        /* This is a partial result from Memcached. This actor will continue to accept 
         * messages until it has recieved all of the results it needs */
        case result: GetResult => {
            getMap -= result.key
            getMap += ((result.key, Some(result)))
            maybeSendResponse()
        }
    }
}
