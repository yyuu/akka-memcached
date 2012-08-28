package com.klout.akkamemcache

import akka.actor.IO
import akka.util.ByteString
import akka.actor._

// Object sent to the IOActor indicating that a multiget request is complete.
object Finished

class Iteratees(ioActor: ActorRef) {
    import Constants._

    def ascii(bytes: ByteString): String = bytes.decodeString("US-ASCII").trim

    val whiteSpace = Set(' ', '\r').map(_.toByte)

    def continue(byte: Byte): Boolean = {
        !whiteSpace.contains(byte)
    }

    val readLine = {
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

    val processValue = {
        for {
            whitespace <- IO takeUntil Space;
            key <- IO takeUntil Space;
            id <- IO takeUntil Space;
            length <- IO takeUntil Space map (ascii(_).toInt);
            cas <- IO takeUntil CRLF;
            value <- IO take length;
            newline <- IO takeUntil CRLF
        } yield {
            val found = Found(ascii(key), value)
            IO Done found
        }
    }

    val processLine = {
        IO repeat {
            readLine map {
                case IO.Done(found) => {
                    ioActor ! found
                }
                case _ => {}
            }
        }
    }

}

object Constants {

    val Error = ByteString("ERROR")

    val Space = ByteString(" ")

    val CRLF = ByteString("\r\n")

    val Value = ByteString("VALUE")

    val End = ByteString("END")

}

object Protocol {
    import Constants._

    trait Command {
        def toByteString: ByteString
    }

    case class SetCommand(key: String, payload: ByteString, ttl: Long) extends Command {
        override def toByteString = {
            if (key.size == 0) throw new RuntimeException("A key is required")
            if (payload.size == 0) throw new RuntimeException("Payload size must be greater than 0")
            if (ttl < 0) throw new RuntimeException("ttl must be greater than or equal to 0")
            ByteString("set " + key + " 0 " + ttl + " " + payload.size + " noreply") ++ CRLF ++ payload ++ CRLF
        }
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
        override def toByteString = {
            if (keys.size > 0) ByteString("gets " + (keys mkString " ")) ++ CRLF
            else ByteString()
        }
    }

}
