package com.klout.akkamemcache
import akka.util.ByteString
trait Serializer[T] {

    def serialize(t: T): ByteString

}

object Serializer {

    def serialize[T: Serializer](t: T): ByteString = implicitly[Serializer[T]] serialize t
}

trait Deserializer[T] {

    def deserialize(bytes: ByteString): T

}

object Deserializer {

    def deserialize[T: Deserializer](bytes: ByteString): T = implicitly[Deserializer[T]] deserialize bytes
}