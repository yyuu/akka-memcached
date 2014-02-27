/**
 * Copyright (C) 2012 Klout Inc. <http://www.klout.com>
 */

package com.klout.akkamemcached

import ActorTypes._

import akka.actor._
import akka.actor.SupervisorStrategy._
import scala.concurrent.Future
import akka.event.Logging
import akka.routing._
import akka.util.ByteString
import scala.concurrent.duration._
import com.google.common.hash.Hashing._
import com.klout.akkamemcached.Protocol._
import java.io._
import java.net.InetSocketAddress
import java.net.URLEncoder._
import scala.collection.JavaConversions._
import scala.collection.mutable.{ HashMap, LinkedHashSet }
import scala.util.Random

/**
 * These types are used to make the code more understandable.
 */
object ActorTypes {
    type RequestingActorRef = ActorRef
    type IoActorRouterRef = ActorRef
    type PoolActorRef = ActorRef
}

/**
 * This actor instantiates the pool of MemcachedIOActors and routes requests
 * from the MemcachedClient to the IOActors.
 */
class PoolActor(hosts: List[(String, Int)], connectionsPerServer: Int) extends Actor {

    val hashFunction = goodFastHash(32)

    /**
     * Maps memcached servers to a pool of IOActors, one for each connection.
     */
    var ioActorMap: Map[String, IoActorRouterRef] = _

    /**
     * RequestMap maps the requesting actor to the results that will be recieved from memcached.
     */
    val requestMap: HashMap[RequestingActorRef, HashMap[String, Option[GetResult]]] = new HashMap()

    /**
     * Updates the requestMap to add the result from Memcached to any actor
     * that requested it.
     */
    def updateRequestMap(result: GetResult) = {
        requestMap foreach {
            case (actor, resultMap) => {
                resultMap foreach {
                    case (key, resultOption) if key == result.key => resultMap update (key, Some(result))
                    case _                                        => Unit
                }
            }
        }
    }

    /**
     * If the all of the results from Memcached have been returned for a given actor, this
     * function will send the results to the actor and remove the actor from the requestMap
     */
    def sendResponses() {
        val responsesToSend = requestMap filterNot {
            case (actor, resultMap) => resultMap.values.toList.contains(None)
        }
        responsesToSend foreach {
            case (actor, resultMap) =>
                actor ! GetResponse(resultMap.values.flatten.toList)
                requestMap -= actor
        }
    }

    /**
     * Instantiate the actors for the Memcached clusters. Each host is mapped to a set
     * of actors. Each IoActor owns one connection to the server.
     */
    override def preStart {
        ioActorMap =
            hosts.map {
                case (host, port) =>
                    val ioActors = (1 to connectionsPerServer).map {
                        num =>
                            /**
                             * Sleeps for 30ms, so the IOManager does not throw an InvalidActorName exception
                             * when all the IoActors are created simultaneously
                             */
                            Thread.sleep(30)
                            context.actorOf(Props(new MemcachedIOActor(host, port, self)),
                                name = encode("Memcached IoActor for " + host + " " + num, "UTF-8"))
                    }.toList
                    val router = RoundRobinRouter(routees = ioActors)
                    val routingActor = context.actorOf(Props(new MemcachedIOActor(host, port, self)) withRouter router,
                        name = encode("Memcached IoActor Router for " + host, "UTF-8"))
                    host -> routingActor
            } toMap
    }

    /**
     * Splits the given command into subcommands that are sent to the
     * appropriate IoActors.
     */
    def forwardCommand(command: Command) = {
        val hostCommandMap = command match {
            case SetCommand(keyValueMap, ttl) =>
                val splitKeyValues = keyValueMap groupBy {
                    case (key, value) =>
                        val hashCode = hashFunction hashString key
                        hosts(consistentHash(key.hashCode, hosts.size))
                }
                splitKeyValues map {
                    case (host, keyValueMap) => (host, SetCommand(keyValueMap, ttl))
                }

            case GetCommand(keys) =>
                val splitKeys = keys groupBy (key => hosts(consistentHash(key.hashCode, hosts.size)))
                splitKeys map {
                    case (host, keys) => (host, GetCommand(keys))
                }

            case command: DeleteCommand =>
                val splitKeys = command.keys groupBy (key => hosts(consistentHash(key.hashCode, hosts.size)))
                splitKeys map {
                    case (host, keys) => (host, DeleteCommand(keys: _*))
                }

        }

        hostCommandMap foreach {
            case ((host, port), command) => ioActorMap(host) ! command
        }
    }

    def receive = {
        /**
         * For GetCommands, this will save the requester to the requestMap so the
         * result can be returned to the requester.
         */
        case command @ GetCommand(keys) =>
            val keyResultMap = keys.map {
                key => key -> None
            }.toList
            requestMap += ((sender, HashMap(keyResultMap: _*)))
            forwardCommand(command)

        /* Route a SetCommand or DeleteCommand to the correct IoActor */
        case command: Command => forwardCommand(command)

        /**
         * Update the requestMap for any actors that were requesting this result, and
         * send responses to the actors if their request has been fulfilled.
         */
        case result: GetResult =>
            updateRequestMap(result)
            sendResponses()

        case GetResults(results) =>
            results foreach updateRequestMap
            sendResponses()

    }

    /**
     * Restart all of the actors if an exception is thrown
     */
    override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 5 seconds) {
        case _ => Restart
    }
}

/**
 * This actor is responsible for all communication to and from a single memcached server
 * using a single connection.
 */
class MemcachedIOActor(host: String, port: Int, poolActor: PoolActorRef) extends Actor {

    /**
     * The initial amount of time that the client will wait before trying
     * to reconnect to the memcached server after losing a connection
     */
    var reconnectDelayMillis = 1000

    /**
     * The number of consecutive failed attempts to connect to the Memcached server
     */
    var reconnectAttempts = 20

    /**
     * The maximum amount of time that the client will sleep before trying to
     * reconnect to the Memcached server
     */
    val maxReconnectDelayMillis = 16000

    /**
     * The maximum number of times that the client will try to reconnect to the memcached server
     * before aborting
     */
    val maxReconnectAttempts = 3

    val log = Logging(context.system, this)
    var connection: IO.SocketHandle = _

    /**
     * The maximum number of keys that can be queried in a single multiget
     */
    val maxKeyLimit = 1000

    /**
     * Contains the pending results for a Memcache multiget that is currently
     * in progress
     */
    val currentSet: LinkedHashSet[String] = new LinkedHashSet()

    /**
     * Contains the pending results for the next Memcached multiget
     */
    val nextSet: LinkedHashSet[String] = new LinkedHashSet()

    /**
     * Opens a single connection to the Memcached server
     */
    override def preStart {
        connection = IOManager(context.system) connect new InetSocketAddress(host, port)
        log.debug("IoActor starting on " + host + ":" + port)
    }

    /**
     * Adds this get request to the IOActor's internal state. If there is a get currently
     * in progress, the request is placed in a queued map, and will be executed after the
     * current request is completed
     */
    def enqueueCommand(keys: Set[String]) {
        /* Remove duplicate keys */
        val newKeys = keys diff (nextSet ++ currentSet)
        val set = if (awaitingResponseFromMemcached) nextSet else currentSet

        set ++= newKeys
    }

    /**
     * Writes a multiget command that contains all of the keys from currentMap
     * into Memcached
     */
    def writeGetCommandToMemcachedIfPossible() {
        if (!awaitingResponseFromMemcached) {
            if (currentSet.size > 0) {
                connection write GetCommand(currentSet toSet).toByteString
                awaitingResponseFromMemcached = true
            } else {
                awaitingResponseFromMemcached = false
            }
        }
    }

    /**
     * This is triggered when Memcached sends an END. At this point, any keys remaining
     * in currentSet are cache misses.
     */
    def getCommandCompleted() {
        awaitingResponseFromMemcached = false

        /* Send "NotFound" messages for any keys remaining in the current set */
        poolActor ! GetResults(currentSet.map(NotFound).toSet)

        empty(currentSet)

        /* Copy the queued instructions to the current set */
        currentSet ++= (nextSet take maxKeyLimit)

        /* Remove those instructions from the queue */
        nextSet --= (nextSet take maxKeyLimit)

        /* Write the current set of get requests */
        writeGetCommandToMemcachedIfPossible()
    }

    /**
     * This Iteratee processes the responses from Memcached and sends messages back to
     * the IoActor whenever it has parsed a result
     */
    val iteratee = IO.IterateeRef.async(new Iteratees(self).processInput)(context.dispatcher)

    /**
     * If this actor is awaiting a response from Memcached, then it will not
     * make any requests until the response is returned
     */
    var awaitingResponseFromMemcached = false

    /**
     * Attempts to reconnect to the Memcached client after having lost the connection. Tries
     * to open a new connection and requests the version command to Memcached to determine
     * whether or not the connection is valid. If there have been too many failed connection
     * attempts, this actor will throw an exception to indicate it's failure.
     */
    def attemptReconnect() = {
        if (reconnectAttempts == maxReconnectAttempts) {
            log error "Cannot connect to " + host + " after " + reconnectAttempts + " failed attempts. Abandoning this connection"
        } else {
            log warning "Connection to " + host + " lost. Waiting " + reconnectDelayMillis + " ms, then retrying"

            Thread.sleep(reconnectDelayMillis)

            /* Attempt to reconnect */
            connection = IOManager(context.system) connect new InetSocketAddress(host, port)

            /* Write the version command over the connection, hoping to get a response */
            connection write VersionCommand.byteString

            /* Increase the amount of time that the actor will wait before trying to reconnect again */
            reconnectDelayMillis = min(reconnectDelayMillis * 2, maxReconnectDelayMillis)
            reconnectAttempts += 1
        }

    }

    /**
     * Resets reconnectAttempts and reconnectDelayMillis to their default values
     */
    def resetReconnectCounters() = {
        if (reconnectAttempts > 0) {
            log warning "Connection to " + host + " re-established"
            reconnectAttempts = 0
            reconnectDelayMillis = 1000
        }
    }

    def receive = {
        case raw: ByteString => connection write raw

        /**
         * Adds the keys for the getcommand to a queue for writing to Memcached,
         * and issues the command if the actor is not currently waiting for a
         * response from Memcached
         */
        case GetCommand(keys) =>
            enqueueCommand(keys)
            writeGetCommandToMemcachedIfPossible()

        /**
         * Immediately writes a command to Memcached
         */
        case command: Command => connection write command.toByteString

        /**
         * Reads data from Memcached. The iteratee will send the result
         * of this read to this actor as a Found or Finished message.
         */
        case IO.Read(socket, bytes) => {
            resetReconnectCounters()
            iteratee(IO Chunk bytes)
        }

        case IO.Closed(socket, cause) => attemptReconnect()

        case IO.NewClient(server) => {
            println("NEW CLIENT")
        }

        /**
         * A single key-value pair has been returned from Memcached. Sends
         * the result to the poolActor and removes the key from the set of keys
         * that don't currently have a value
         */
        case found @ Found(key, value) =>
            poolActor ! found
            currentSet -= key

        /**
         * A get command has finished. This will send the appropriate message
         * to the poolActor and make another command if necessary
         */
        case Finished => getCommandCompleted()

        case Alive => {
            resetReconnectCounters()
            awaitingResponseFromMemcached = false
            writeGetCommandToMemcachedIfPossible()
        }
    }

    def min(a: Int, b: Int) = {
        if (a < b) a else b
    }

    def empty[T](set: LinkedHashSet[T]) {
        set --= set
    }

}
