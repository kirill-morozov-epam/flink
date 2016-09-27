/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.scala.batch.sql

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.table.{Row, TableEnvironment, ValidationException}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.Assert._

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class SelectITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  @Test
  def testSelectStarFromTable(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
      "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
      "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"

    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSelectArray(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

//        val sqlQuery = "SELECT *,array['werwe','xcvcxv'] FROM MyTable"

//        val sqlQuery = "SELECT * FROM MyTable, unnest(array['werwe','xcvcxv'])"
//        val sqlQuery = "select * from MyTable"

//    val sqlQuery = "select * from unnest(array(SELECT * FROM MyTable))"

//    val sqlQuery = "select * from unnest(array['werwe','xcvcxv']), unnest(array['vcbcbvvc','liooloi'])"
    val sqlQuery = "select * from unnest(array['werwe','xcvcxv']) as t(c), unnest(array['werwe','liooloi']) as t1(c1) where c=c1"

    //    val sqlQuery = "select * from nullif(10,10)"
//        val sqlQuery = "select * from MyTable,(select * from values (1.0, 2.0))"

    //"A=[1, 2]" expected
//    val sqlQuery = "select array(SELECT * FROM MyTable) from (values (1))"

    //    val sqlQuery = "SELECT 1 as a, 2 as b, 3 as a, 4 as B FROM (VALUES (6))"

//        val sqlQuery = "SELECT * FROM (values (11.0, 12.0), (21.0, 22.0))"


    //    val sqlQuery = "SELECT array[10,20] FROM MyTable"
//        val sqlQuery = "SELECT * FROM MyTable"

//    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("MyTable", ds)


    val result = tEnv.sql(sqlQuery)

    tEnv.explain(result)

    val expected = "1,1,Hi\n"

    val resultsDS = result.toDataSet[Row].collect()

//    val ds1 = result.toDataSet[Row]
//
//    val results = resultsDS.collect()

    //    assertEquals(results(0).productElement(0)(0),"")
    //    results(0).productElement(0)(1)

//    val fff = (Tuple)(results.asJava.get(0))
    println()
//    println(results(0).productElement(0).)
//    TestBaseUtils.compareResultAsText(results.asJava, expected)
//    TestBaseUtils.compareResult(results.asJava, expected)
  }

  @Test
  def testSelectArray(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

//        val sqlQuery = "SELECT *,array['werwe','xcvcxv'] FROM MyTable"

//        val sqlQuery = "SELECT * FROM MyTable, unnest(array['werwe','xcvcxv'])"
//        val sqlQuery = "select * from MyTable"

//    val sqlQuery = "select * from unnest(array(SELECT * FROM MyTable))"

//    val sqlQuery = "select * from unnest(array['werwe','xcvcxv']), unnest(array['vcbcbvvc','liooloi'])"
    val sqlQuery = "select * from unnest(array['werwe','xcvcxv']) as t(c), unnest(array['werwe','liooloi']) as t1(c1) where c=c1"

    //    val sqlQuery = "select * from nullif(10,10)"
//        val sqlQuery = "select * from MyTable,(select * from values (1.0, 2.0))"

    //"A=[1, 2]" expected
//    val sqlQuery = "select array(SELECT * FROM MyTable) from (values (1))"

    //    val sqlQuery = "SELECT 1 as a, 2 as b, 3 as a, 4 as B FROM (VALUES (6))"

//        val sqlQuery = "SELECT * FROM (values (11.0, 12.0), (21.0, 22.0))"


    //    val sqlQuery = "SELECT array[10,20] FROM MyTable"
//        val sqlQuery = "SELECT * FROM MyTable"

//    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("MyTable", ds)


    val result = tEnv.sql(sqlQuery)

    tEnv.explain(result)

    val expected = "1,1,Hi\n"

    val resultsDS = result.toDataSet[Row].collect()

//    val ds1 = result.toDataSet[Row]
//
//    val results = resultsDS.collect()

    //    assertEquals(results(0).productElement(0)(0),"")
    //    results(0).productElement(0)(1)

//    val fff = (Tuple)(results.asJava.get(0))
    println()
//    println(results(0).productElement(0).)
//    TestBaseUtils.compareResultAsText(results.asJava, expected)
//    TestBaseUtils.compareResult(results.asJava, expected)
  }

  @Test
  def testSelectStarFromDataSet(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
      "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
      "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"

    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSimpleSelectAll(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable1,MyTable2 where a=a1"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable1", ds)
    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a1, 'b1, 'c1)
    tEnv.registerTable("MyTable2", ds1)


    val result = tEnv.sql(sqlQuery)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
      "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
      "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"

    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSelectWithNaming(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT _1 as a, _2 as b FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "1,1\n" + "2,2\n" + "3,2\n" + "4,3\n" + "5,3\n" + "6,3\n" + "7,4\n" +
      "8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" + "15,5\n" +
      "16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n"

    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidFields(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT a, foo FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)

    tEnv.sql(sqlQuery)
  }

}
