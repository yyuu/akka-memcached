package com.klout.akkamemcache

import org.specs2.execute.PendingUntilFixed
import org.specs2.mutable._
import akka.util.duration._
import akka.util.Duration
import akka.dispatch.Await
import scala.reflect.BeanProperty

case class TestClass(@BeanProperty test1: String,
                     @BeanProperty test2: Map[String, Int])

class MemcachedClientSpec extends Specification with PendingUntilFixed {

    val client = new RealMemcachedClient()
    val noTTL = Duration("0 seconds")
    val timeout = Duration("1 seconds")

    sequential
    "The memcached client" should {
        "set some values, and get them later" in {
            client.set("key1", "value1", noTTL)
            client.set("key2", "value2", noTTL)
            val value1 = Await.result(client.get[String]("key1"), timeout)
            val value2 = Await.result(client.get[String]("key2"), timeout)
            value1 must_== Some("value1")
            value2 must_== Some("value2")
        }
        "use multiget to get values, and miss nonexistent values" in {
            val valueMap = Await.result(client.mget(Set("key1", "key2", "key3")), timeout)
            valueMap.get("key1") must_== Some("value1")
            valueMap.get("key2") must_== Some("value2")
            valueMap.get("key3") must beNone
        }
        "change existing values" in {
            client.set("key1", "value3", noTTL)
            client.set("key2", "value4", noTTL)
            val valueMap = Await.result(client.mget(Set("key1", "key2")), timeout)
            valueMap.get("key1") must_== Some("value3")
            valueMap.get("key2") must_== Some("value4")
        }
        "delete a value" in {
            client.delete("key1")
            val value1 = Await.result(client.get("key1"), timeout)
            value1 must beNone
        }
        "set multiple values" in {
            client.mset(Map("key4" -> "value4", "key5" -> "value5"), noTTL)
            val valueMap = Await.result(client.mget(Set("key4", "key5")), timeout)
            valueMap.get("key4") must_== Some("value4")
            valueMap.get("key5") must_== Some("value5")
        }
        "delete multiple values" in {
            client.delete("key4", "key5")
            val valueMap = Await.result(client.mget(Set("key4", "key5")), timeout)
            valueMap.get("key4") must beNone
            valueMap.get("key5") must beNone
        }
        "use valid TTLs" in {
            client.set("key6", "value6", Duration("2 seconds"))
            Thread.sleep(1000)
            val value6 = Await.result(client.get("key6"), timeout)
            value6 must_== Some("value6")
            Thread.sleep(1000)
            val value6None = Await.result(client.get("key6"), timeout)
            value6None must beNone
        }
        "handle many gets,sets, and deletes" in {
            (1 to 10).foreach { _ =>
                val keys = (1 to 100).map(_.toString)
                val sets = (1 to 100).map{ num => num.toString -> (num + 1).toString }.toMap
                client.mset(sets, noTTL)
                val results = Await.result(client.mget[String](keys.toSet), timeout)
                results.forall {
                    case (key, value) => value.toInt must_== key.toInt + 1
                }
                client.delete(keys: _*)
            }
        }
        "serialize and deserialize maps and lists" in {
            val map = ('a' to 'z').map{
                letter => letter -> letter.toInt
            }.toMap
            val list = 1 to 100
            client.mset(Map("map1" -> map, "list1" -> list), noTTL)
            val results = Await.result(client.mget(Set("map1", "list1")), timeout)
            results.get("map1") must_== Some(map)
            results.get("list1") must_== Some(list)
        }
        "serialize case classes" in {
            val testObject = TestClass("Foo", Map("Bar1" -> 1, "Bar2" -> 2))
            client.set("testObject", testObject, noTTL)
            val result = Await.result(client.get("testObject"), timeout)
            result must_== Some(testObject)
        }

    }
}