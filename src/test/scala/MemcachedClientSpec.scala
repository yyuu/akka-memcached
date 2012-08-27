package test

import org.specs2.execute.PendingUntilFixed
import org.specs2.mutable._
import akka.util.duration._
import akka.util.Duration
import akka.actor._
import akka.util.ByteString
import akka.dispatch.Await
import scala.reflect.BeanProperty
import scala.collection.mutable.HashMap
import com.klout.akkamemcache._
import akka.util.Timeout
import akka.pattern.ask
import scala.util.Random

object GiveMeTheState // Command to give the state to the test

class FakeIoActor extends Actor {
    var state: HashMap[String, Any] = new HashMap()
    def receive = {
        case Finished          => state += (("Finished" -> true))
        case GiveMeTheState    => sender ! state
        case key: String       => sender ! state.get(key)
        case kv: (String, Any) => state += kv
        case found: Found      => state += (found.key -> found.value)
    }
}

class MemcachedClientSpec extends Specification with PendingUntilFixed {
    implicit val timeout = Timeout(Duration("30 seconds")) // needed for `?` below
    val system = ActorSystem()
    val fakeIoActor = system.actorOf(Props[FakeIoActor])
    val iteratee = new Iteratees(fakeIoActor).processLine

    sequential
    "The Iteratee" should {
        "send the Finished object to the IOActor when it recieves an END from Memcached" in {
            val bytes = ByteString("END\r\n")
            iteratee(IO Chunk bytes)
            Await.result((fakeIoActor ? "Finished"), Duration("1 second")).asInstanceOf[Option[Boolean]] must_== Some(true)
        }
        "properly parse a single result" in {
            val command = ByteString("VALUE testKey 0 10 88064\r\nabcdefghij\r\nEND\r\n")
            iteratee(IO Chunk command)
            Await.result((fakeIoActor ? "testKey"), Duration("1 second")).asInstanceOf[Option[ByteString]] must_== Some(ByteString("abcdefghij"))
        }
        "properly parse multiple results" in {
            val command1 = ByteString("VALUE testKey2 0 5 88064\r\n01234\r\n")
            val command2 = ByteString("VALUE testKey3 0 7 88064\r\n5678910\r\nEND\r\n")
            val commands = command1 ++ command2
            iteratee(IO Chunk commands)
            Await.result((fakeIoActor ? "testKey2"), Duration("1 second")).asInstanceOf[Option[ByteString]] must_== Some(ByteString("01234"))
            Await.result((fakeIoActor ? "testKey3"), Duration("1 second")).asInstanceOf[Option[ByteString]] must_== Some(ByteString("5678910"))
        }
        "parse some very large results" in {
            val generator = new Random()
            var bytes1: Array[Byte] = new Array(1000000)
            var bytes2: Array[Byte] = new Array(500000)
            Random.nextBytes(bytes1)
            Random.nextBytes(bytes2)
            val command1 = ByteString("VALUE random1 0 1000000 12343\r\n") ++ ByteString(bytes1) ++ ByteString("\r\n")
            val command2 = ByteString("VALUE random2 0 500000 12343\r\n") ++ ByteString(bytes2) ++ ByteString("\r\nEND\r\n")
            val commands = command1 ++ command2
            iteratee(IO Chunk commands)
            Await.result((fakeIoActor ? "random1"), Duration("1 second")).asInstanceOf[Option[ByteString]] must_== Some(ByteString(bytes1))
            Await.result((fakeIoActor ? "random2"), Duration("1 second")).asInstanceOf[Option[ByteString]] must_== Some(ByteString(bytes2))
        }
        "make sure that it has the valid information in the state" in {
            /* The state contains the five kv pairs, and the "finished" indicator */
            val state = Await.result((fakeIoActor ? GiveMeTheState), Duration("1 second")).asInstanceOf[HashMap[String, Any]]
            state.size must_== 6
            state.get("Finished") must_== Some(true)
            state.get("testKey") must beSome
            state.get("testKey2") must beSome
            state.get("testKey3") must beSome
            state.get("random1") must beSome
            state.get("random2") must beSome
        }
    }
}