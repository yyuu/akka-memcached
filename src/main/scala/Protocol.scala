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

    val readLine: IO.Iteratee[Option[Found]] = {
        (IO takeWhile continue) flatMap {
            case Value =>
                println("about to process value")
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
            foo <- IO takeUntil Space;
            val a = println("foo: " + foo)
            key <- IO takeUntil Space;
            val b = println("key: " + key)
            id <- IO takeUntil Space;
            val c = println("id: " + id)
            length <- IO takeUntil Space map (ascii(_).toInt);
            val d = println("length: " + length)
            cas <- {
                println("about to try to take cas")
                val result = IO takeUntil CRLF;
                println("done taking cas: " + result)
                result
            }
            val e = println("cas: " + cas)
            value <- IO take length;
            val x = println("value: " + value)
            newline <- IO takeUntil CRLF
        } yield {
            //println ("key: [%s], length: [%d], value: [%s]".format(ascii(key), length, value))
            val found = Some(Found(ascii(key), value))
            println("found:" + found)
            IO Done found
        }

    val processLine: IO.Iteratee[Unit] = {
        IO repeat {
            println("repeating")
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
