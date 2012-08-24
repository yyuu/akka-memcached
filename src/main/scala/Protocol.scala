package com.klout.akkamemcache

import akka.actor.IO
import akka.util.ByteString
import com.klout.akkamemcache.{GetResult, Found}


object Iteratees {
    import Constants._

    def ascii(bytes: ByteString): String = bytes.decodeString("US-ASCII").trim

    val ioActor = Tester.ioActor

    val readLine: IO.Iteratee[GetResult] = {
        IO takeUntil Space flatMap {
            case Value => {
                //println("Value!")
                processValue
            }
            case other => {
                println("Other!")
                IO takeUntil CRLF map (_ => NotFound("Key"))
            }
        }
    }

    val processValue: IO.Iteratee[GetResult] =
        for {
            key <- IO takeUntil Space
            id  <- IO takeUntil Space
            length <- IO takeUntil CRLF map (ascii(_).toInt)
            value <- IO take length
            newline <- IO takeUntil CRLF
            end <- IO takeUntil CRLF
        } yield {
            // println ("key: [%s], length: [%d], value: [%s]".format(key, length, value))
            Found(ascii(key),value)
        }

    val processLine: IO.Iteratee[Unit] = {
        IO repeat{
            readLine map {
                case getResult:GetResult => {
                    ioActor ! getResult
                }
            }
        }
    }


}

object Constants {

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
        override def toByteString = ByteString("set " + key + " " + ttl + " 0 " + payload.size) ++ CRLF ++ payload ++ CRLF
    }

    case class DeleteCommand(key: String) extends Command {
        override def toByteString = ByteString("delete "+key ) ++ CRLF
    }

    case class GetCommand(key: String) extends Command {
        override def toByteString = ByteString("get " + key) ++ CRLF
    }

}
