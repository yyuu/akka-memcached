package com.klout.akkamemcache
import akka.util.ByteString
trait Serializer[T] {

    def serialize(t: T): ByteString

}

object Serializer {

    implicit val strings: Serializer[String] = new Serializer[String] {
        def serialize(string: String): ByteString = ByteString(string)
    }

    def serialize[T: Serializer](t: T): ByteString = implicitly[Serializer[T]] serialize t
}

trait Deserializer[T] {

    def deserialize(bytes: ByteString): T

}

object Deserializer {

    def ascii(bytes: ByteString): String = bytes.decodeString("US-ASCII").trim

    implicit val strings: Deserializer[String] = new Deserializer[String] {
        def deserialize(bytes: ByteString): String = ascii(bytes)
    }

    def deserialize[T: Deserializer](bytes: ByteString): T = implicitly[Deserializer[T]] deserialize bytes
}