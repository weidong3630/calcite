/**
 * Alipay.com Inc. Copyright (c) 2004-2019 All Rights Reserved.
 */
package org.apache.calcite.test.common;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.server.CalciteServer;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @author wangweidong
 * @version $Id: ParserContext.java, v 0.1 2019年07月24日 下午4:15 wangweidong Exp $
 */
public class ParserContext {
    private static Class                  calciteConnectionImpl;
    private        Connection             connection;
    private        AvaticaStatement       statement;
    private        CalcitePrepareImpl     prepare;
    private        CalcitePrepare.Context context;
    private        RelOptPlanner          planner;
    private        SqlToRelConverter      sqlToRelConverter;
    private        SqlValidator           sqlValidator;
    private        CalciteCatalogReader   catalogReader;


    static {
        try {
            calciteConnectionImpl = Class.forName("org.apache.calcite.jdbc.CalciteConnectionImpl");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ParserContext(Connection connection, Statement statement) throws Exception {
        this.connection = connection;
        this.statement = (AvaticaStatement)statement;
        init();
    }

    private void init() throws Exception {
        Function0<CalcitePrepare> prepareFactory = (Function0<CalcitePrepare>)getField(connection, "prepareFactory");
        prepare = (CalcitePrepareImpl)prepareFactory.apply();
        context = createContext();
        planner = (RelOptPlanner)getMethod("createPlanner", CalcitePrepare.Context.class).invoke(prepare, context);

        catalogReader = getCatalogReader();
        sqlValidator = (SqlValidator)getMethod("createSqlValidator", CalcitePrepare.Context.class, CalciteCatalogReader.class).invoke(prepare, context, catalogReader);
        sqlToRelConverter = createSqlToRelConverter();
    }


    private Method getMethod(String name, Class<?>... parameterTypes) throws Exception {
        Method method = null;
        if (parameterTypes.length > 0) {
            method = prepare.getClass().getDeclaredMethod(name, parameterTypes);
        } else {
            method = prepare.getClass().getDeclaredMethod(name);
        }
        method.setAccessible(true);
        return method;
    }

    //private Object invokeMethod(Object obj, String name, Object... args) throws Exception {
    //  Method method = prepare.getClass().getDeclaredMethod(name, CalcitePrepare.Context.class);
    //  method.setAccessible(true);
    //  return method.invoke(prepare, args);
    //}

    private Object getField(Object target, String name) throws Exception {
        Field field = calciteConnectionImpl.getDeclaredField(name);
        field.setAccessible(true);
        return getField(calciteConnectionImpl, target, name);
    }

    private Object getField(Class<?> targetClass, Object target, String name) throws Exception {
        Field field = targetClass.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    public CalcitePrepare.Context createContext() throws Exception {
        CalciteServer server = (CalciteServer)getField(connection, "server");
        final CalciteServerStatement statement = server.getStatement(this.statement.handle);
        final CalcitePrepare.Context context = statement.createPrepareContext();
        return context;
    }

    public SqlParser createParser(CalcitePrepare.Context context, String sql) {
        final CalciteConnectionConfig config = context.config();
        final SqlParser.ConfigBuilder parserConfig = SqlParser.configBuilder()
                .setQuotedCasing(config.quotedCasing())
                .setUnquotedCasing(config.unquotedCasing())
                .setQuoting(config.quoting())
                .setConformance(config.conformance())
                .setCaseSensitive(config.caseSensitive());
        final SqlParserImplFactory parserFactory =
                config.parserFactory(SqlParserImplFactory.class, null);
        if (parserFactory != null) {
            parserConfig.setParserFactory(parserFactory);
        }

        return SqlParser.create(sql, parserConfig.build());
    }

    public Program getProgram() {
        return
                (planner, rel, requiredOutputTraits, materializations, lattices) -> {
                    planner.setRoot(rel);

                    for (RelOptMaterialization materialization : materializations) {
                        planner.addMaterialization(materialization);
                    }
                    for (RelOptLattice lattice : lattices) {
                        planner.addLattice(lattice);
                    }

                    final RelNode rootRel2 =
                            rel.getTraitSet().equals(requiredOutputTraits)
                                    ? rel
                                    : planner.changeTraits(rel, requiredOutputTraits);
                    assert rootRel2 != null;

                    planner.setRoot(rootRel2);
                    final RelOptPlanner planner2 = planner.chooseDelegate();
                    final RelNode rootRel3 = planner2.findBestExp();
                    assert rootRel3 != null : "could not implement exp";
                    return rootRel3;
                };
    }

    private CalciteCatalogReader getCatalogReader() {
        return new CalciteCatalogReader(
                context.getRootSchema(),
                context.getDefaultSchemaPath(),
                context.getTypeFactory(),
                context.config());
    }

    private SqlToRelConverter createSqlToRelConverter() throws Exception {
        final SqlToRelConverter.ConfigBuilder builder =
                SqlToRelConverter.configBuilder()
                        .withTrimUnusedFields(true)
                        .withExpand(false)
                        .withExplain(false);
        final RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(context.getTypeFactory()));
        return new SqlToRelConverter(null, sqlValidator, catalogReader, cluster,
                StandardConvertletTable.INSTANCE, builder.build());
    }

    public SqlNode parseStmt(String sql) throws SqlParseException {
        SqlParser sqlParser = createParser(context, sql);
        return sqlParser.parseStmt();
    }

    public void executeDdl(SqlNode node) {
        prepare.executeDdl(context, node);
    }

    public List<RelOptMaterialization> getMaterializations() throws Exception {
        Class CalcitePreparingStmt = Arrays.stream(CalcitePrepareImpl.class.getDeclaredClasses())
                .filter(tmp -> tmp.getName().equals("org.apache.calcite.prepare.CalcitePrepareImpl$CalcitePreparingStmt")).collect(Collectors.toList()).get(0);
        Constructor preparingStmtConstructor = CalcitePreparingStmt.getDeclaredConstructors()[0];
        preparingStmtConstructor.setAccessible(true);
        Object preparingStmt = preparingStmtConstructor.newInstance(this.prepare, context, getCatalogReader(),
                context.getTypeFactory(), context.getRootSchema(), EnumerableRel.Prefer.ARRAY,
                planner, EnumerableConvention.INSTANCE, StandardConvertletTable.INSTANCE);

        Method m = Prepare.class.getDeclaredMethod("getMaterializations");
        m.setAccessible(true);
        List<Prepare.Materialization> materializations =  (List<Prepare.Materialization>)m.invoke(preparingStmt);

        final List<RelOptMaterialization> materializationList = new ArrayList<>();
        for (Prepare.Materialization materialization : materializations) {
            List<String> qualifiedTableName = ((CalciteSchema.TableEntry)getField(Prepare.Materialization.class, materialization, "materializedTable")).path();
            materializationList.add(
                    new RelOptMaterialization((RelNode)getField(Prepare.Materialization.class, materialization, "tableRel"),
                            (RelNode)getField(Prepare.Materialization.class, materialization, "queryRel"),
                            (RelOptTable) getField(Prepare.Materialization.class, materialization, "starRelOptTable"),
                            qualifiedTableName));
        }

        return materializationList;
    }

    public RelNode optimize(RelNode relNode, RelOptRule rule) {
        HepProgramBuilder program = HepProgram.builder();
        program.addGroupBegin();
        program.addRuleInstance(rule);
        program.addGroupEnd();

        HepPlanner planner = new HepPlanner(program.build());
        planner.setRoot(relNode);
        return planner.findBestExp();
    }

    public CalcitePrepareImpl getPrepare() {
        return prepare;
    }

    public CalcitePrepare.Context getContext() {
        return context;
    }

    public RelOptPlanner getPlanner() {
        return planner;
    }

    public SqlToRelConverter getSqlToRelConverter() {
        return sqlToRelConverter;
    }

    public SqlValidator getSqlValidator() {
        return sqlValidator;
    }

    public Connection getConnection() {
        return connection;
    }

    public AvaticaStatement getStatement() {
        return statement;
    }
}