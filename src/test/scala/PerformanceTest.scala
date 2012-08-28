package test
import java.io._
import com.klout.akkamemcache._
import org.jboss.serial.io._
import junit.framework._
import net.spy.memcached.ConnectionFactory
import net.spy.memcached.transcoders._
import net.spy.memcached.{ MemcachedClient => SpyMemcachedClient }
import java.net.InetSocketAddress
import scala.collection.JavaConversions._
import com.clarkware.junitperf._
import akka.dispatch.Await
import akka.util.duration._

class JbossSerializingTranscoder extends SerializingTranscoder {

    override def serialize(o: AnyRef): Array[Byte] = {
        Option(o) match {
            case None => throw new NullPointerException("Can't serialize null")
            case Some(o) =>
                try {
                    val bos = new ByteArrayOutputStream
                    val os = new JBossObjectOutputStream(bos)
                    using (bos, os) {
                        os writeObject o
                        bos toByteArray
                    }
                } catch {
                    case e: IOException => throw new IllegalArgumentException("Non-serializable object", e);
                }
        }
    }

    override def deserialize(in: Array[Byte]): AnyRef = {
        Option(in) match {
            case Some(in) =>
                try {

                    val bis = new ByteArrayInputStream(in)
                    val is = new JBossObjectInputStream(bis)
                    using(bis, is) {
                        is readObject
                    }
                } catch {
                    case e: Exception =>
                        e match {
                            case io: IOException            => {}
                            case ce: ClassNotFoundException => {}
                        }
                        super.deserialize(in)
                }
            /*null*/
            case None => null
        }
    }
}

object PerformanceTest {

    val bigMapAkka = (1 to 1000).map(num => "Akka" + num.toString -> ("String #" + num)) toMap
    val bigListAkka = (1 to 1000).map("Akka String #" + _)
    val bigMapOfListsAkka = (1 to 100).map(num => "Akka" + num.toString -> (bigListAkka take num)) toMap

    val bigMapSpy = (1 to 1000).map(num => "Spy" + num.toString -> ("String #" + num)) toMap
    val bigListSpy = (1 to 1000).map("Spy String #" + _)
    val bigMapOfListsSpy = (1 to 100).map(num => "Spy" + num.toString -> (bigListSpy take num)) toMap

    val akkaClient = new RealMemcachedClient()

    val spyClient = {
        import net.spy.memcached.{ MemcachedClient => SpyMemcachedClient }
        val addr = new InetSocketAddress("localhost", 11211)
        val connfactory: ConnectionFactory = new net.spy.memcached.DefaultConnectionFactory() {
            override def getDefaultTranscoder: Transcoder[Object] = new JbossSerializingTranscoder
        }
        new SpyMemcachedClient(connfactory, List(addr))
    }

    class AkkaMemcachedTests(testName: String) extends TestCase(testName) {
        def akkaSetAndGetSingleString() {
            akkaClient.set("akkatest", "Test", 0 seconds)
            Await.result(akkaClient.get[String]("akkatest"), 5 seconds)
        }
        def akkaSetAndGetManyStrings() {
            akkaClient.mset(bigMapAkka, 0 seconds)
            Await.result(akkaClient.mget[String](bigMapAkka.keys.toSet), 5 seconds)
        }
        def akkaSetAndGetSingleBigObject() {
            akkaClient.set("akkaBigObject", bigMapAkka, 0 seconds)
            Await.result(akkaClient.get[Map[String, String]]("akkaBigObject"), 5 seconds)
        }
        def akkaSetAndGetManyBigObjects() {
            akkaClient.mset(bigMapOfListsAkka, 0 seconds)
            Await.result(akkaClient.mget[List[String]](bigMapOfListsAkka.keys.toSet), 5 seconds)
        }
    }

    class SpyMemcachedTests(testName: String) extends TestCase(testName) {
        def spySetAndGetSingleString() {
            spyClient.set("spytest", 0, "Test")
            spyClient.get("spytest")
        }
        def spySetAndGetManyStrings() {
            bigMapSpy.foreach {
                case (key, value) => spyClient.set(key, 0, value)
            }
            spyClient.getBulk(bigMapSpy.keys)
        }
        def spySetAndGetSingleBigObject() {
            spyClient.set("spyBigObject", 0, bigMapSpy)
            spyClient.get("spyBigObject")
        }
        def spySetAndGetManyBigObjects() {
            bigMapOfListsSpy foreach {
                case (key, value) => spyClient.set(key, 0, value)
            }
            spyClient.getBulk(bigMapOfListsSpy.keys)
        }
    }
    def suite: Test = {
        val suite = new TestSuite
        suite addTest new TimedTest(new AkkaMemcachedTests("akkaSetAndGetSingleString"), 30000)
        suite addTest new TimedTest(new SpyMemcachedTests("spySetAndGetSingleString"), 30000)

        suite addTest new TimedTest(new AkkaMemcachedTests("akkaSetAndGetManyStrings"), 30000)
        suite addTest new TimedTest(new SpyMemcachedTests("spySetAndGetManyStrings"), 30000)

        suite addTest new TimedTest(new AkkaMemcachedTests("akkaSetAndGetSingleBigObject"), 30000)
        suite addTest new TimedTest(new SpyMemcachedTests("spySetAndGetSingleBigObject"), 30000)

        suite addTest new TimedTest(new AkkaMemcachedTests("akkaSetAndGetManyBigObjects"), 30000)
        suite addTest new TimedTest(new SpyMemcachedTests("spySetAndGetManyBigObjects"), 30000)
        suite
    }

}

class PerformanceTest extends TestCase {

    def main(args: Array[String]) {
        junit.textui.TestRunner run PerformanceTest.suite
    }
}