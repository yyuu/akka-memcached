import akka.actor.IO
import akka.util.ByteString

object Iteratees {
    import Constants._

    def ascii(bytes: ByteString): String = bytes.decodeString("US-ASCII").trim

    val readLine: IO.Iteratee[Option[String]] = IO take 5 flatMap {
        case Value => processValue map (Some(_))
        case _ => IO takeUntil CRLF map (_ => None)
    }

    val processValue: IO.Iteratee[String] =
        for {
            key <- IO takeUntil Space
            _ <- IO takeUntil Space
            length <- IO takeUntil Space map (ascii(_).toInt)
            _ <- IO takeUntil CRLF
            value <- IO take length
            _ <- IO takeUntil CRLF
            _ <- IO takeUntil CRLF
        } yield "key: [%s], length: [%d], value: [%s]" format (key, length, value)


    val processLine: IO.Iteratee[Unit] = readLine map {
        case Some(thing) => println("something: "  + thing)
        case _ => println("error")
    }


}

object Constants {

    val Space = ByteString(" ")

    val CRLF = ByteString("\r\n")

    val Value = ByteString("VALUE")

    val Error = ByteString("ERROR")

    val End = ByteString("END")



}

object Protocol {
    import Messages._

    sealed trait Command {
        def toRequest: Request
    }

    case class SetCommand(values: Map[String, Array[Byte]], ttlSeconds: Long) extends Command {
        override def toRequest: Request = null
    }

    case class DeleteCommand(keys: Set[String]) extends Command {
        override def toRequest: Request = null
    }

    case class GetComand(keys: Set[String]) extends Command {
        override def toRequest: Request = null
    }

}
