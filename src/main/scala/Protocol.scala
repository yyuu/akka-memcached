package com.klout.akkamemcache

import akka.actor.IO
import akka.util.ByteString

// Object sent to the IOActor indicating that a multiget request is complete.
object Finished

object Iteratees {
    import Constants._

    def ascii(bytes: ByteString): String = bytes.decodeString("US-ASCII").trim

    val ioActor = Tester.ioActor
    val whiteSpace = Set(' ', '\r').map(_.toByte)

    def continue(byte: Byte): Boolean = {
        !whiteSpace.contains(byte)
    }

    val readLine: IO.Iteratee[Option[Found]] = {
        (IO takeWhile continue) flatMap {
            case Value =>
                processValue

            case Error => {
                IO takeUntil CRLF map { _ =>
                    println("An error occurred")
                    None
                }
            }

            case End => {
                IO takeUntil CRLF map { _ =>
                    ioActor ! Finished
                    None
                }
            }

            case other => {
                IO takeUntil CRLF map {
                    data =>
                        println("Unexpected output from Memcached: " + ascii(other) + ". " + ascii(data))
                        None
                }
            }
        }
    }

    val processValue: IO.Iteratee[Option[Found]] =
        for {
            _ <- IO take 1
            key <- IO takeUntil Space
            id <- IO takeUntil Space
            length <- IO takeUntil Space map (ascii(_).toInt)
            cas <- IO takeUntil CRLF
            value <- IO take length
            newline <- IO takeUntil CRLF
        } yield {
            //println ("key: [%s], length: [%d], value: [%s]".format(key, length, value))
            Some(Found(ascii(key), value))
        }

    val processLine: IO.Iteratee[Unit] = {
        IO repeat {
            readLine map {
                case Some(found) => {
                    ioActor ! found
                }
                case _ => {}
            }
        }
    }

}

object Constants {

    val Space = ByteString(" ")

    val CRLF = ByteString("\r\n")

    val Value = ByteString("VALUE")

    val Deleted = ByteString("DELETED")

    val Stored = ByteString("STORED")

    val Error = ByteString("ERROR")

    val NotFound = ByteString("NOT_FOUND")

    val End = ByteString("END")

}

object Protocol {
    import Constants._

    trait Command {
        def toByteString: ByteString
    }

    case class SetCommand(key: String, payload: ByteString, ttl: Long) extends Command {
        override def toByteString = ByteString("set " + key + " " + ttl + " 0 " + payload.size + " noreply") ++ CRLF ++ payload ++ CRLF
    }

    case class DeleteCommand(keys: String*) extends Command {
        override def toByteString = {
            val command = keys.map {
                "delete " + _ + " noreply" + "\r\n"
            } mkString ""
            ByteString(command)
        }
    }

    case class GetCommand(keys: Set[String]) extends Command {
        override def toByteString = ByteString("gets " + (keys mkString " ")) ++ CRLF
    }

}
