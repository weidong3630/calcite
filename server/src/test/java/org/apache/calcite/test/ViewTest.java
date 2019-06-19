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

import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.CalciteServer;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.*;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.mysql.cj.jdbc.Driver;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.List;

import static org.apache.calcite.test.Matchers.isLinux;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;

/**
 * Unit tests for server and DDL.
 */
public class ViewTest {
  static final String URL = "jdbc:calcite:";
  //static final String URL = "jdbc:calcite:model=/Users/wangweidong/bigdata/calcite/example/csv/src/test/resources/mysql.json";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

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
  public void testDriver() {
    Class driverClass = Driver.class;
    Assert.assertNotNull(driverClass);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3046">[CALCITE-3046]
   * CompileException when inserting casted value of composited user defined type
   * into table</a>. */
  @Test public void testCreateTable() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      boolean b = s.execute("create table tt2 (i int not null)");
      assertThat(b, is(false));
      int x = s.executeUpdate("insert into tt2 values 1");
      assertThat(x, is(1));
      x = s.executeUpdate("insert into tt2 values 3");
      assertThat(x, is(1));
      try (ResultSet r = s.executeQuery("select sum(i) from tt2")) {
        assertThat(r.next(), is(true));
        assertThat(r.getInt(1), is(4));
        assertThat(r.next(), is(false));
      }
    }
  }

  @Test
  public void testMaterializedView() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      s.execute("create table t (i int not null)");
      //s.executeUpdate("insert into t values 1");
      //s.executeUpdate("insert into t values 3");
      //s.executeUpdate("insert into t values 5");
      //boolean b = s.execute("create table mv (c1 int)");
      //boolean b =
      s.executeUpdate("create materialized view mv as select * from t");
      //assertThat(b, is(false));
      //try (ResultSet r = s.executeQuery("select sum(i) from mv")) {
      //  assertThat(r.next(), is(true));
      //  assertThat(r.getInt(1), is(9));
      //  assertThat(r.next(), is(false));
      //}

      System.setProperty("calcite_debug", "true");
      try (ResultSet r = s.executeQuery("select * from (select i from t) as t1 where i > 3")) {
        //assertThat(r.next(), is(true));
        //assertThat(r.getInt(1), is(5));
        //assertThat(r.next(), is(false));
      }
      System.setProperty("calcite_debug", "false");
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

  @Test
  public void testRelBuilder() {
    final FrameworkConfig config = config().build();
    final RelBuilder builder = RelBuilder.create(config);
    final RelNode node = builder
            .scan("EMP")
            .project(builder.field("DEPTNO"), builder.field("ENAME"))
            .build();
    System.out.println(RelOptUtil.toString(node));
  }

  @Test
  public void testOptRule() {
    final FrameworkConfig config = config().build();
    final RelBuilder builder = RelBuilder.create(config);
    final RelNode node = builder
            .scan("EMP")
            .project(builder.field("DEPTNO"), builder.field("ENAME"))
            .build();

    HepProgram hepProgram = HepProgram.builder()
            .addRuleInstance(HoloTableConvertRule.INSTANCE)
            .build();
    RelOptPlanner planner = new HepPlanner(hepProgram);
    planner.setRoot(node);
    RelNode retNode = planner.findBestExp();
    Assert.assertNotNull(retNode);
  }

  private static class HoloTableConvertRule extends RelOptRule {
    public static final HoloTableConvertRule INSTANCE =
            new HoloTableConvertRule(RelFactories.LOGICAL_BUILDER);

    /**
     * Creates a HoloTableConvertRule.
     */
    public HoloTableConvertRule(RelBuilderFactory relBuilderFactory) {
      super(
              operand(Project.class,
                      operand(TableScan.class, RelOptRule.none())),
              relBuilderFactory, "HoloTableConvertRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      System.out.println(call.toString());
    }
  }
}

// End ServerTest.java
