package com.klout.akkamemcache

import akka.actor._
import akka.util.ByteString
import java.net.InetSocketAddress
import akka.dispatch.Future
import com.klout.akkamemcache.Protocol._
import scala.collection.mutable.HashMap
import com.klout.akkamemcache.Protocol._

/**
 * This actor instantiates a pool of MemcachedIOActors. It receives commands from
 * RealMemcachedClient and MemcachedClientActors and distributes them to the appropriate
 * MemcachedIO actor depending on what server each key is mapped to.
 */
class PoolActor(hosts: List[(String, Int)]) extends Actor {
    var actors: List[ActorRef] = _

    override def preStart {
        actors = hosts map {
            case (host, port) =>
                context.actorOf(Props(new MemcachedIOActor(host, port)), name = "Memcached_IO_Actor_for_" + host)
        }
    }
    println("PoolActor was initiated with hosts: " + hosts)
    def receive = {
        /**
         * For GetCommands, send a reference to the requesting actor to the
         * IoActor so that the MemcachedIoActor can return the result directly
         * to the requester. Because SetCommands and DeleteCommands are fire-and-forget,
         * the MemcachedIoActor does not need to know the sender.
         */
        case command: Command =>
            command.consistentSplit(actors) foreach {
                case (actor, command: GetCommand) => actor ! (command, sender)
                case (actor, command: Command)    => actor ! command
            }
    }
}

/**
 * This actor is responsible for all communication to and from a single memcached server
 */
class MemcachedIOActor(host: String, port: Int) extends Actor {
    def ascii(bytes: ByteString): String = bytes.decodeString("US-ASCII").trim

    var connection: IO.SocketHandle = _

    /* Contains the pending results for a Memcache multiget that is currently
     * in progress */
    var currentMap: HashMap[ActorRef, Set[PotentialResult]] = new HashMap

    /* Contains the pending results for the next Memcached multiget */
    var nextMap: HashMap[ActorRef, Set[PotentialResult]] = new HashMap

    /**
     * Opens a single connection to the Memcached server
     */
    override def preStart {
        connection = IOManager(context.system) connect new InetSocketAddress(host, port)
    }

    /**
     * Adds this get request, along with the requesting actor, to the IOActor's
     * internal state. If there is a get currently in progress, the request is
     * placed in a queued map, and will be executed after the current request
     * is completed
     */
    def enqueueCommand(actor: ActorRef, keys: Set[String]) {
        val map = if (awaitingResponseFromMemcached) nextMap else currentMap
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
        val missingKeys: List[(ActorRef, String)] = currentMap.toList.flatMap {
            case (requestingActor, results) => results.flatMap{
                case NotYetFound(key) => Some((requestingActor, key))
                case found            => None
            }
        }
        missingKeys.foreach {
            case (requestingActor, key) => {
                requestingActor ! NotFound(key)
            }
        }
    }

    /**
     * Sends Found messages for all actors who are interested in the found object.
     * Updates the IoActor's map to indicate that the keys have been found
     */
    def sendFoundMessages(found: Found) {
        val requestingActors = currentMap.flatMap{
            case (actor, results) if (results.exists(_.key == found.key)) => Some(actor)
            case _ => None
        }

        requestingActors foreach { actor =>
            actor ! found
        }

        currentMap = currentMap.map{
            case (requestingActor, results) =>
                val newKeys = results.map{
                    case NotYetFound(key) if key == found.key => found
                    case other                                => other
                }
                (requestingActor, newKeys)
        }
    }

    /**
     * Writes a multiget command that contains all of the keys from currentMap
     * into Memcached
     */
    def writeGetCommandToMemcachedIfPossible() {
        if (!awaitingResponseFromMemcached) {
            val missingKeys = currentMap.flatMap {
                case (actor, potentialResults) => potentialResults.map(_.key)
            }.toSet

            if (missingKeys.size > 0) {
                connection.write(GetCommand(missingKeys).toByteString)
                awaitingResponseFromMemcached = true
            } else {
                awaitingResponseFromMemcached = false
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
                    case notFound          => None
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
     * This is triggered when Memcached sends an END. At this point, any PotentialResults
     * in currentMap that are NotYetFound are cache misses.
     */
    def getCommandCompleted() {
        awaitingResponseFromMemcached = false
        sendNotFoundMessages()
        swapMaps()
        writeGetCommandToMemcachedIfPossible()
    }

    // This Iteratee processes the response from Memcached
    val iteratee = IO.IterateeRef.async(new Iteratees(self).processLine)(context.dispatcher)

    var awaitingResponseFromMemcached = false

    def receive = {
        case raw: ByteString =>
            connection write raw

        /**
         * GetCommand issued from the PoolActor, on behalf of a MemcachedClientActor
         */
        case (GetCommand(keys), recipient: ActorRef) =>
            enqueueCommand(recipient, keys)
            writeGetCommandToMemcachedIfPossible()
            awaitingResponseFromMemcached = true

        case command: Command =>
            connection write command.toByteString

        /* Response from Memcached */
        case IO.Read(socket, bytes) =>
            iteratee(IO Chunk bytes)

        /* A single get result has been returned */
        case found: Found => {
            sendFoundMessages(found)
        }

        /* A multiget has completed */
        case Finished => getCommandCompleted()
    }

}

class MemcachedClientActor(poolActor: ActorRef) extends Actor {
    implicit val ec = ActorSystem()
    var originalSender: ActorRef = _

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
            poolActor ! command
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