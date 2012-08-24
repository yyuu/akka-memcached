package com.klout.akkamemcache

import akka.dispatch.Future
import akka.actor._
import akka.util.Duration
import akka.util.duration._
import akka.pattern.ask
import akka.util.Timeout

import com.klout.akkamemcache.MemcachedIOActor
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
    
    val system = ActorSystem()

    val actor = system.actorOf(Props[MemcachedClientActor])

    override def set[T: Serializer](key: String, value: T, ttl: Duration) {
    }

    override def mset[T: Serializer](values: Map[String, T], ttl: Duration) {
    }

    override def get[T: Deserializer](key: String): Future[Option[T]] = {
        null 
    }

    override def mget[T: Deserializer](keys: Set[String]): Future[Map[String, T]] = {
        null
    }

    override def delete(keys: String*) {
    }

}

object Tester {
    implicit val timeout = Timeout(1 seconds) // needed for `?` below
    import akka.util.ByteString

    val system = ActorSystem()

    val ioActor = system.actorOf(Props[MemcachedIOActor])
    val actor = system.actorOf(Props[MemcachedClientActor])

    def rawMemCached(string: String)(implicit timeout: Timeout):Future[Any] = {
        actor ? ByteString(string)
    }

    def main(args: Array[String]){
        (actor ? GetCommand("blah")).map(result => println("Result: " + result))
        //(actor ? GetCommand("blah")).map(result => println("Result: " + result))
        //(actor ? GetCommand("blah")).map(result => println("Result: " + result))
        //Tester.rawMemCached("get blah\r\n").map(result => println("Result: "+ result))
    }
}