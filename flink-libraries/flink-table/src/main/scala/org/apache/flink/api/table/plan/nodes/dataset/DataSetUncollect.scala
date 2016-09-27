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

package org.apache.flink.api.table.plan.nodes.dataset

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Values
import org.apache.calcite.rex.RexLiteral
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.runtime.io.ValuesInputFormat
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.api.table.typeutils.TypeConverter._
import org.apache.flink.api.table.{BatchTableEnvironment, Row}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * DataSet RelNode for a LogicalValues.
  *
  */
class DataSetUncollect(
                     cluster: RelOptCluster,
                     traitSet: RelTraitSet,
                     rowType: RelDataType,
                     convInput: RelNode,
                     tuples: ImmutableList[ImmutableList[RexLiteral]])
  extends SingleRel(cluster, traitSet, convInput)
    with DataSetRel {

  override def deriveRowType() = rowType


//  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
//    double dRows = mq.getRowCount(this).doubleValue();
//    double dCpu = 1.0D;
//    double dIo = 0.0D;
//    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
//  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetUncollect(
      cluster,
      traitSet,
      rowType,
      inputs.get(0),
      tuples
    )
  }

  override def toString: String = {
    "Values(values: (${rowType.getFieldNames.asScala.toList.mkString(\", \")}))"
  }

//  override def explainTerms(pw: RelWriter): RelWriter = {
//    super.explainTerms(pw).item("values", valuesFieldsToString)
//  }

  override def translateToPlan(
                                tableEnv: BatchTableEnvironment,
                                expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {

    val config = tableEnv.getConfig

    val inputDS = convInput.asInstanceOf[DataSetCalc].translateToPlan(tableEnv)

    val returnType = determineReturnType(
      getRowType,
      expectedType,
      config.getNullCheck,
      config.getEfficientTypeUsage).asInstanceOf[RowTypeInfo]

//    val z = new Array[Object](5);

    val ar = inputDS.collect().get(0).asInstanceOf[Row].productElement(0).asInstanceOf[Array[Object]]

    // convert List[RexLiteral] to Row
    val rows: Seq[Row] = ar.map { t =>
      val row = new Row(1)
      row.setField(0, t)
      row
    }
//
    val inputFormat = new ValuesInputFormat(rows)
    tableEnv.execEnv.createInput(inputFormat, returnType).asInstanceOf[DataSet[Any]]

//    inputDS
  }

  private def valuesFieldsToString: String = {
    rowType.getFieldNames.asScala.toList.mkString(", ")
  }

}


