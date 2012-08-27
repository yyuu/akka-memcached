package com.klout.akkamemcache

import org.specs2.execute.PendingUntilFixed
import org.specs2.mutable._
import akka.util.duration._
import akka.util.Duration
import akka.dispatch.Await

class MemcachedClientSpec extends Specification with PendingUntilFixed {

    val client = new RealMemcachedClient()
    val noTTL = Duration("0 seconds")
    val timeout = Duration("1 seconds")

    sequential
    "The memcached client" should {
        "be able to set some values, and get them later" in {
            client.set("key1", "value1", noTTL)
            client.set("key2", "value2", noTTL)
            val value1 = Await.result(client.get("key1"), timeout)
            val value2 = Await.result(client.get("key2"), timeout)
            value1 must_== Some("value1")
            value2 must_== Some("value2")
        }
        "be able to use multiget to get values, and miss nonexistent values" in {
            val valueMap = Await.result(client.mget(Set("key1", "key2", "key3")), timeout)
            valueMap.get("key1") must_== Some("value1")
            valueMap.get("key2") must_== Some("value2")
            valueMap.get("key3") must beNone
        }
        "be able to change existing values" in {
            client.set("key1", "value3", noTTL)
            client.set("key2", "value4", noTTL)
            val valueMap = Await.result(client.mget(Set("key1", "key2")), timeout)
            valueMap.get("key1") must_== Some("value3")
            valueMap.get("key2") must_== Some("value4")
        }
        "be able to delete a value" in {
            client.delete("key1")
            val value1 = Await.result(client.get("key1"), timeout)
            value1 must beNone
        }
        "be able to set multiple values" in {
            client.mset(Map("key4" -> "value4", "key5" -> "value5"), noTTL)
            val valueMap = Await.result(client.mget(Set("key4", "key5")), timeout)
            valueMap.get("key4") must_== Some("value4")
            valueMap.get("key5") must_== Some("value5")
        }
        "be able to delete multiple values" in {
            client.delete("key4", "key5")
            val valueMap = Await.result(client.mget(Set("key4", "key5")), timeout)
            valueMap.get("key4") must beNone
            valueMap.get("key5") must beNone
        }
    }
}