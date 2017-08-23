package com.databricks.spark.sql.perf.mllib.fpm

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.random.{PoissonGenerator, RandomDataGenerator}

import scala.collection.mutable.ArrayBuffer

class FrequentItemSetGenerator(
    val numItems: Int,
    val averageSizeOfItemSet: Int)
  extends RandomDataGenerator[Array[String]] {

  assert(averageSizeOfItemSet >= 3)
  assert(numItems > 10)

  private val rng = new java.util.Random()
  private val itemSetSizeRng = new PoissonGenerator(averageSizeOfItemSet - 2)
  private val itemRng = new PoissonGenerator(numItems / 2)

  def nextPoissonValueWithCond(rng: PoissonGenerator)(condition: Int => Boolean): Int = {
    var value: Int = 0
    do {
      value = rng.nextValue().toInt
    } while (!condition(value))
    value
  }

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
    itemSetSizeRng.setSeed(seed)
    itemRng.setSeed(seed)
  }

  override def nextValue(): Array[String] = {
    // 1. generate size of itemset
    val size = nextPoissonValueWithCond(itemSetSizeRng)(_ >= 1)
    val arrayBuff = new ArrayBuffer[Int](size + 2)

    // 2. generate items in the itemset
    var i = 0
    while (i < size) {
      val nextVal = nextPoissonValueWithCond(itemRng) { item: Int =>
        item >= 0 && item < numItems && !arrayBuff.contains(item)
      }
      arrayBuff.append(nextVal)
      i += 1
    }

    // 3 generate associate-rules by adding two computed items

    // 3.1 add a new item = (firstItem + numItems / 2) % numItems
    val newItem1 = (arrayBuff(0) + numItems / 2) % numItems
    if (!arrayBuff.contains(newItem1)) {
      arrayBuff.append(newItem1)
    }
    // 3.2 add a new item = (firstItem + secondItem) % numItems
    if (arrayBuff.size >= 2) {
      val newItem2 = (arrayBuff(0) + arrayBuff(1)) % numItems
      if (!arrayBuff.contains(newItem2)) {
        arrayBuff.append(newItem2)
      }
    }
    arrayBuff.map(_.toString).toArray
  }

  override def copy(): FrequentItemSetGenerator
    = new FrequentItemSetGenerator(numItems, averageSizeOfItemSet)
}
