/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.common.ParserContext;
import org.apache.calcite.tools.*;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for server and DDL.
 */
public class OptimizeTest {
  static final String URL = "jdbc:calcite:";
  private ParserContext parserContext;

  //static final String URL = "jdbc:calcite:model=/Users/wangweidong/bigdata/calcite/example/csv/src/test/resources/mysql.json";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  public OptimizeTest() {
    try {
      Connection connection = connect();
      this.parserContext = new ParserContext(connection, connection.createStatement());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static Connection connect() throws SQLException {
    return DriverManager.getConnection(URL,
        CalciteAssert.propBuilder()
            .set(CalciteConnectionProperty.PARSER_FACTORY,
                SqlDdlParserImpl.class.getName() + "#FACTORY")
            .set(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED,
                "true")
            .build());
  }

  @Test
  public void testOptimize() throws Exception {
    Connection c = null;
    try {
      c = connect();
      Statement s = c.createStatement();

      ParserContext parserContext = new ParserContext(c, s);

      SqlNode sqlNode = parserContext.parseStmt("create table t (i int not null)");
      Assert.assertNotNull(sqlNode);
      parserContext.executeDdl(sqlNode);

      sqlNode = parserContext.parseStmt("create materialized view mv as select * from t");
      Assert.assertNotNull(sqlNode);
      parserContext.executeDdl(sqlNode);

      sqlNode = parserContext.parseStmt("select * from (select i+1 as c1, i+2 as c2 from t) as t1 where i > 3");
      Assert.assertNotNull(sqlNode);

      RelRoot root = parserContext.getSqlToRelConverter().convertQuery(sqlNode, true, true);
      Assert.assertNotNull(root);

      Program program = parserContext.getProgram();

      RelTraitSet relTraits = root.rel.getTraitSet()
              .replace(EnumerableConvention.INSTANCE)
              .replace(root.collation)
              .simplify();

      RelNode optRelNode = program.run(parserContext.getPlanner(), root.rel,
              relTraits, parserContext.getMaterializations(), new ArrayList<>());
      Assert.assertNotNull(optRelNode);
      RelOptUtil.toString(optRelNode);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (c != null) {
        c.close();
      }
    }
  }

  private static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .defaultSchema(
                    CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL))
            .traitDefs((List<RelTraitDef>) null)
            .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  private static RelBuilder createBuilder() {
    final FrameworkConfig config = config().build();
    return RelBuilder.create(config);
  }

  @Test
  public void testRelBuilder() {
    final RelBuilder builder = createBuilder();
    final RelNode node = builder
            .scan("EMP")
            .filter(builder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, builder.field("DEPTNO"), builder.literal("10")),
                    builder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, builder.field("DEPTNO"), builder.literal("20")))
            .project(builder.field("DEPTNO"), builder.field("ENAME"))
            .build();

    System.out.println(RelOptUtil.toString(node));
    System.out.println(toSqlNode(node).toString());
  }

  @Test
  public void testBetween() {
    final RelBuilder builder = createBuilder();
    final RelNode node = builder
            .scan("EMP")
            .filter(builder.getRexBuilder().makeCall(
                    builder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN),
                    SqlStdOperatorTable.BETWEEN,
                    Arrays.asList(builder.field("DEPTNO"), builder.literal("10"), builder.literal("20"))))
            .project(builder.field("DEPTNO"), builder.field("ENAME"))
            .build();

    System.out.println(RelOptUtil.toString(node));
    System.out.println(toSqlNode(node).toString());
  }

  @Test
  public void testJoin() {
    final RelBuilder builder = createBuilder();
    final RelNode left = builder
            .scan("EMP")
            .scan("DEPT")
            .join(JoinRelType.INNER, "DEPTNO")
            .build();
    //builder.push(left)
      //      .push(builder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, builder.field("DEPTNO"), builder.literal("10")));

    LogicalJoin join = (LogicalJoin)left;

    System.out.println(toSqlNode(left).toString());
  }

  @Test
  public void testSeletNull() throws SqlParseException {
      String sql = "select cast(null as varchar) as a";
      SqlNode sqlNode = parserContext.parseStmt(sql);
      RelNode relNode = parserContext.getSqlToRelConverter().convertQuery(sqlNode, true, true).rel;
      SqlNode sqlNodeNew = toSqlNode(relNode);
      Assert.assertEquals(sqlNodeNew.toString(), "SELECT NULL AS `A`\n"
              + "FROM (VALUES  (0)) AS `t` (`ZERO`)");
  }


  private static SqlNode toSqlNode(RelNode root) {
    SqlDialect dialect = MysqlSqlDialect.DEFAULT;
    RelToSqlConverter converter = new RelToSqlConverter(dialect == null ? dialect : dialect);
    return converter.visitChild(0, root).asStatement();
  }

  public static class Debug {
    public static boolean current = false;
  }
}

// End ServerTest.java
