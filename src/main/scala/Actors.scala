import akka.actor._
import akka.util.ByteString
import java.net.InetSocketAddress


object Messages {

    case class Request(bytes: ByteString)

}

class MemcachedClientActor extends Actor {
    import Messages._

    val port = 11211

    var connection: IO.SocketHandle = _

    override def preStart {
        connection = IOManager(context.system) connect new InetSocketAddress(port)
    }

    val state = IO.IterateeRef.Map.async[IO.Handle]()(context.dispatcher)

    def receive = {

         case IO.Read(handle, bytes) =>
            println("reading: " + bytes)
            state(handle)(IO Chunk bytes)

         case Request(bytes) =>
            println("writing: " + bytes)
            connection write bytes
            state(connection) flatMap (_ => Iteratees.processLine)

    }

}

