package com.databricks.spark.sql.perf.mllib.data

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.mllib.random.{PoissonGenerator, RandomDataGenerator}

class ItemSetGenerator(
    val numItems: Int,
    val avgItemSetSize: Int)
  extends RandomDataGenerator[Array[String]] {

  assert(avgItemSetSize > 2)
  assert(numItems > 2)

  private val rng = new java.util.Random()
  private val itemSetSizeRng = new PoissonGenerator(avgItemSetSize - 2)
  private val itemRng = new PoissonGenerator(numItems / 2.0)

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
    itemSetSizeRng.setSeed(seed)
    itemRng.setSeed(seed)
  }

  override def nextValue(): Array[String] = {
    // 1. generate size of itemset
    val size = DataGenUtil.nextPoisson(itemSetSizeRng, v => v >= 1 && v <= numItems).toInt
    val arrayBuff = new ArrayBuffer[Int](size + 2)

    // 2. generate items in the itemset
    var i = 0
    while (i < size) {
      val nextVal = DataGenUtil.nextPoisson(itemRng, (item: Double) => {
        item >= 0 && item < numItems && !arrayBuff.contains(item)
      }).toInt
      arrayBuff.append(nextVal)
      i += 1
    }

    // 3 generate association rules by adding two computed items

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

  override def copy(): ItemSetGenerator
    = new ItemSetGenerator(numItems, avgItemSetSize)
}
