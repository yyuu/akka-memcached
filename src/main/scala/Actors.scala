package com.klout.akkamemcache

import akka.actor._
import akka.util.ByteString
import java.net.InetSocketAddress
import akka.dispatch.Future
import com.klout.akkamemcache.Protocol._
import scala.collection.mutable.{ HashMap, HashSet }
import com.klout.akkamemcache.Protocol._
import scala.collection.JavaConversions._
/**
 * This actor instantiates a pool of MemcachedIOActors. It receives commands from
 * RealMemcachedClient and MemcachedClientActors and distributes them to the appropriate
 * MemcachedIO actor depending on what server each key is mapped to.
 */
class PoolActor(hosts: List[(String, Int)]) extends Actor {
    var ioActors: List[ActorRef] = _

    var requestMap: HashMap[ActorRef, HashMap[String, Option[GetResult]]] = new HashMap()

    /**
     * Sends the response back if the memcache command has completed
     */
    def updateRequestMap(result: GetResult) = {
        requestMap = requestMap map {
            case (actor, resultMap) => {
                val newResultMap = resultMap map {
                    case (key, resultOption) if key == result.key => (key, Some(result))
                    case other                                    => other
                }
                (actor, newResultMap)
            }
        }
    }

    def sendResponses() {
        val responsesToSend = requestMap.flatMap{
            case (actor, resultMap) if (!resultMap.values.toList.contains(None)) => Some(actor, resultMap)
            case other => None
        }
        responsesToSend foreach {
            case (actor, responses) =>
                actor ! responses.values.flatten
                requestMap -= actor
        }
    }

    override def preStart {
        ioActors = hosts map {
            case (host, port) =>
                context.actorOf(Props(new MemcachedIOActor(host, port, self)), name = "Memcached_IO_Actor_for_" + host)
        }
    }

    def forwardCommand(command: Command) = {
        command.consistentSplit(ioActors) foreach {
            case (ioActor, command) => ioActor ! command
        }
    }

    def receive = {
        /**
         * For GetCommands, send a reference to the requesting actor to the
         * IoActor so that the MemcachedIoActor can return the result directly
         * to the requester. Because SetCommands and DeleteCommands are fire-and-forget,
         * the MemcachedIoActor does not need to know the sender.
         */
        case command @ GetCommand(keys) =>
            val keyResultMap = keys.map {
                key => key -> None
            }.toList
            requestMap += ((sender, HashMap(keyResultMap: _*)))
            forwardCommand(command)
        case command: Command => forwardCommand(command)
        case result: GetResult => {
            updateRequestMap(result)
            sendResponses()
        }
        case results: Set[GetResult] => {
            results foreach updateRequestMap
            sendResponses()
        }
    }
}

/**
 * This actor is responsible for all communication to and from a single memcached server
 */
class MemcachedIOActor(host: String, port: Int, poolActor: ActorRef) extends Actor {
    def ascii(bytes: ByteString): String = bytes.decodeString("US-ASCII").trim

    var connection: IO.SocketHandle = _

    /* Contains the pending results for a Memcache multiget that is currently
     * in progress */
    var currentSet: HashSet[String] = new HashSet()

    /* Contains the pending results for the next Memcached multiget */
    var nextSet: HashSet[String] = new HashSet()

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
    def enqueueCommand(keys: Set[String]) {
        val set = if (awaitingResponseFromMemcached) nextSet else currentSet
        set ++= keys
    }

    /**
     * Writes a multiget command that contains all of the keys from currentMap
     * into Memcached
     */
    def writeGetCommandToMemcachedIfPossible() {
        if (!awaitingResponseFromMemcached) {
            if (currentSet.size > 0) {
                connection.write(GetCommand(currentSet.toSet).toByteString)
                awaitingResponseFromMemcached = true
            } else {
                awaitingResponseFromMemcached = false
            }
        }
    }

    /**
     * This is triggered when Memcached sends an END. At this point, any PotentialResults
     * in currentMap that are NotYetFound are cache misses.
     */
    def getCommandCompleted() {
        awaitingResponseFromMemcached = false
        poolActor ! currentSet.map(NotFound).toSet
        currentSet = nextSet
        nextSet = HashSet.empty
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
        case GetCommand(keys) =>
            enqueueCommand(keys)
            writeGetCommandToMemcachedIfPossible()
            awaitingResponseFromMemcached = true

        case command: Command =>
            connection write command.toByteString

        /* Response from Memcached */
        case IO.Read(socket, bytes) =>
            iteratee(IO Chunk bytes)

        /* A single get result has been returned */
        case found @ Found(key, value) => {
            poolActor ! found
            currentSet -= key
        }

        /* A multiget has completed */
        case Finished => getCommandCompleted()
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

case class Found(key: String, value: ByteString) extends GetResult

/* This indicates a cache miss */
case class NotFound(key: String) extends GetResult
