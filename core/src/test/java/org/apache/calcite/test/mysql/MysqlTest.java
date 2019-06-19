package org.apache.calcite.test.mysql;


import com.google.common.collect.Lists;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hsqldb.jdbc.JDBCDatabaseMetaData;
import org.hsqldb.jdbc.JDBCResultSet;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;

import sqlline.BuiltInProperty;
import sqlline.DispatchCallback;
import sqlline.SqlLine;
import sqlline.SqlLineOpts;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests that we can invoke SqlLine on a Calcite connection.
 */
public class MysqlTest {
    /**
     * Execute a script with "sqlline -f".
     *
     * @throws java.lang.Throwable On error
     * @return The stderr and stdout from running the script
     * @param args Script arguments
     */
    private static Pair<SqlLine.Status, String> run(String... args)
            throws Throwable {
        SqlLine sqlline = new SqlLine();
        sqlline.getOpts().set(BuiltInProperty.MAX_WIDTH.propertyName(), BuiltInProperty.MAX_WIDTH.defaultValue().toString());
        //Properties props = new Properties();
        //props.setProperty(BuiltInProperty.MAX_WIDTH.propertyName(), BuiltInProperty.MAX_WIDTH.defaultValue().toString());
        //SqlLineOpts sqlLineOpts = new SqlLineOpts(sqlline, props);
        //sqlline.setOpts(sqlLineOpts);
        SqlLine.Status status = sqlline.begin(args, System.in, true);
        return new Pair<>(status, status.toString());
        //Thread startThread = new Thread() {
        //    @Override
        //    public void run() {
        //        try {
        //            SqlLine.Status status = sqlline.begin(args, null, true);
        //        } catch (Exception e) {
        //            if (!Boolean.getBoolean("sqlline.system.exit")) {
        //                System.exit(-1);
        //            }
        //        }
        //    }
        //};
        //startThread.run();
        //Thread.sleep(5000);
        //
        //BufferedReader reader =
        //        new BufferedReader(new InputStreamReader(System.in));
        //
        //while (true) {
        //    // Reading data using readLine
        //    String sql = reader.readLine();
        //
        //    // Printing the read line
        //    System.out.println(sql);
        //
        //    DispatchCallback callback = new DispatchCallback();
        //    int successCount = sqlline.runCommands(Arrays.asList("select * from t;"), callback);
        //
        //    System.out.println("success count = " + successCount);
        //    //Assert.assertTrue(successCount > 0);
        //}
    }

    private static Pair<SqlLine.Status, String> runScript(File scriptFile,
                                                          boolean flag) throws Throwable {
        List<String> args = new ArrayList<>();
        //Collections.addAll(args, "-u", "jdbc:calcite:", "-n", "sa", "-p", "");
        Collections.addAll(args, "-u", "jdbc:calcite:model=/Users/wangweidong/bigdata/calcite/example/csv/src/test/resources/mysql.json");
        //if (flag) {
        //    args.add("-f");
        //    args.add(scriptFile.getAbsolutePath());
        //} else {
        //    args.add("--run=" + scriptFile.getAbsolutePath());
        //}
        return run(args.toArray(new String[0]));
    }

    /**
     * Attempts to execute a simple script file with the -f option to SqlLine.
     * Tests for presence of an expected pattern in the output (stdout or stderr).
     *
     * @param scriptText Script text
     * @param flag Command flag (--run or -f)
     * @param statusMatcher Checks whether status is as expected
     * @param outputMatcher Checks whether output is as expected
     * @throws Exception on command execution error
     */
    private void checkScriptFile(String scriptText, boolean flag,
                                 Matcher<SqlLine.Status> statusMatcher,
                                 Matcher<String> outputMatcher) throws Throwable {
        // Put the script content in a temp file
        File scriptFile = File.createTempFile("foo", "temp");
        scriptFile.deleteOnExit();
        try (PrintWriter w = Util.printWriter(scriptFile)) {
            w.print(scriptText);
        }

        Pair<SqlLine.Status, String> pair = runScript(scriptFile, flag);

        // Check output before status. It gives a better clue what went wrong.
        assertThat(pair.right, outputMatcher);
        assertThat(pair.left, statusMatcher);
        final boolean delete = scriptFile.delete();
        assertThat(delete, is(true));
    }

    public static void main(String[] args) throws Throwable {
        MysqlTest mysqlTest = new MysqlTest();
        mysqlTest.checkScriptFile("!tables", false, equalTo(SqlLine.Status.OK), equalTo(""));
    }
}

// End SqlLineTest.java
