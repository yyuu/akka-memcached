package com.klout.akkamemcache

trait Serializer[T] {

    def serialize(t: T): Array[Byte]

}

trait Deserializer[T] {

    def deserialize(bytes: Array[Byte]): T

}
