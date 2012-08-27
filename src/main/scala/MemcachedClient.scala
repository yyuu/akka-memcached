package com.klout.akkamemcache

import akka.dispatch.Future
import akka.actor._
import akka.util.Duration
import akka.util.duration._
import akka.pattern.ask
import akka.util.Timeout
import akka.util.ByteString

import com.klout.akkamemcache.Protocol._

trait MemcachedClient {

    val DefaultDuration = 1 hour

    def set[T: Serializer](key: String, value: T, ttl: Duration = DefaultDuration)

    def mset[T: Serializer](values: Map[String, T], ttl: Duration = DefaultDuration)

    def get[T: Deserializer](key: String): Future[Option[T]]

    def mget[T: Deserializer](keys: Set[String]): Future[Map[String, T]]

    def delete(keys: String*)

}

class RealMemcachedClient extends MemcachedClient {
    implicit val timeout = Timeout(30 seconds) // needed for `?` below

    val system = ActorSystem()

    val ioActor = system.actorOf(Props[MemcachedIOActor])

    override def set[T: Serializer](key: String, value: T, ttl: Duration) {
        mset(Map(key -> value))
    }

    override def mset[T: Serializer](values: Map[String, T], ttl: Duration) {
        val commands = values.map {
            case (key, value) => {
                SetCommand(key, Serializer.serialize(value), ttl.toSeconds)
            }
        }
        ioActor ! commands
    }

    override def get[T: Deserializer](key: String): Future[Option[T]] = {
        mget(Set(key)).map(_.get(key))
    }

    override def mget[T: Deserializer](keys: Set[String]): Future[Map[String, T]] = {
        val actor = system.actorOf(Props[MemcachedClientActor])
        val command = GetCommand(keys)
        (actor ? command).map{
            case result: List[GetResult] => result.flatMap {
                case Found(key, value) => Some(key, Deserializer.deserialize[T](value))
                case NotFound(key)     => None
            }.toMap
            case other => throw new Exception("Invalid result returned: " + other)
        }
    }

    override def delete(keys: String*) {
        val commands = DeleteCommand(keys: _*)
        ioActor ! commands
    }

}

object Tester {
    implicit val timeout = Timeout(30 seconds) // needed for `?` below
    import akka.util.ByteString

    val system = ActorSystem()

    val ioActor = system.actorOf(Props[MemcachedIOActor])
    val client = new RealMemcachedClient

    def doCommand(command: Command)(implicit timeout: Timeout) {
        command match {
            case get: GetCommand => {
                val actor = system.actorOf(Props[MemcachedClientActor])
                (actor ? command).map(result => println("Result: " + result))
            }
            case other: Command => {
                ioActor ! command
            }
        }
    }

    def ascii(bytes: ByteString): String = bytes.decodeString("US-ASCII").trim

    implicit val x: Deserializer[String] = new Deserializer[String] {
        def deserialize(bytes: ByteString): String = ascii(bytes)
    }

    def main(args: Array[String]) {
        client.mget[String](Set("blah", "blah2", "blah3")).map(println)
        client.get[String]("blah").map(println)
        // doCommand(GetCommand(Set("blah2")))
        // doCommand(GetCommand(Set("blah3")))
        // doCommand(SetCommand("blah2", ByteString("abc"), 0))
        // doCommand(DeleteCommand("blah4"))
    }
}
