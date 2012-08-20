import akka.dispatch.Future
import akka.actor._
import akka.util.Duration
import akka.util.duration._

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
    import Messages._
    import akka.util.ByteString

    val system = ActorSystem()

    val actor = system.actorOf(Props[MemcachedClientActor])

    def write(string: String) {
        actor ! Request(ByteString(string))
    }

}
