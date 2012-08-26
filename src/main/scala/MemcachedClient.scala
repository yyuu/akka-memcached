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
        ioActor ! SetCommand(key, Serializer.serialize(value), ttl.toSeconds)
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
        val actor = system.actorOf(Props[MemcachedClientActor])
        (actor ? GetCommand(key)) mapTo manifest[Option[ByteString]] map (_ map (Deserializer.deserialize[T]))
    }

    override def mget[T: Deserializer](keys: Set[String]): Future[Map[String, T]] = {
        val actor = system.actorOf(Props[MemcachedClientActor])
        val commands = keys.map(GetCommand(_))
        (actor ? commands) mapTo manifest[Map[String, ByteString]] map (_ mapValues (Deserializer.deserialize[T]))
    }

    override def delete(keys: String*) {
        val commands = keys.map(DeleteCommand(_))
        ioActor ! commands
    }

}

object Tester {
    implicit val timeout = Timeout(30 seconds) // needed for `?` below
    import akka.util.ByteString

    val system = ActorSystem()

    val ioActor = system.actorOf(Props[MemcachedIOActor])

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

    def main(args: Array[String]) {
        doCommand(GetCommand("blah"))
        doCommand(GetCommand("blah2"))
        doCommand(GetCommand("blah3"))
        doCommand(SetCommand("blah2", ByteString("abc"), 0))
        doCommand(DeleteCommand("blah4"))
    }
}
