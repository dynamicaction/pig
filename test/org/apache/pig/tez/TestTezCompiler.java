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
package org.apache.pig.tez;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.util.Properties;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.TezCompiler;
import org.apache.pig.backend.hadoop.executionengine.tez.TezLocalExecType;
import org.apache.pig.backend.hadoop.executionengine.tez.TezPlanContainer;
import org.apache.pig.backend.hadoop.executionengine.tez.TezPlanContainerPrinter;
import org.apache.pig.impl.PigContext;
import org.apache.pig.test.Util;
import org.apache.pig.test.junit.OrderedJUnit4Runner;
import org.apache.pig.test.junit.OrderedJUnit4Runner.TestOrder;
import org.apache.pig.test.utils.TestHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test cases to test the TezCompiler. VERY IMPORTANT NOTE: The tests here
 * compare results with a "golden" set of outputs. In each test case here, the
 * operators generated have a random operator key which uses Java's Random
 * class. So if there is a code change which changes the number of operators
 * created in a plan, then not only will the "golden" file for that test case
 * need to be changed, but also for the tests that follow it since the operator
 * keys that will be generated through Random will be different.
 */
@RunWith(OrderedJUnit4Runner.class)
@TestOrder({
    "testFilter",
    "testGroupBy",
    "testJoin",
    "testLimit",
    "testDistinct",
    "testSplitSingleVertex",
    "testSplitMultiVertex",
    "testMultipleGroupBySplit"
})
public class TestTezCompiler {
    private static PigContext pc;
    private static PigServer pigServer;
    private static final int MAX_SIZE = 100000;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        pc = new PigContext(new TezLocalExecType(), new Properties());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws ExecException {
        pigServer = new PigServer(pc);
    }

    @Test
    public void testFilter() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = filter a by x > 0;" +
                "c = foreach b generate y;" +
                "store c into 'file:///tmp/output';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC1.gld");
    }

    @Test
    public void testGroupBy() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = group a by x;" +
                "c = foreach b generate group, a;" +
                "store c into 'file:///tmp/output';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC2.gld");
    }

    @Test
    public void testJoin() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = load 'file:///tmp/input2' as (x:int, z:int);" +
                "c = join a by x, b by x;" +
                "d = foreach c generate a::x as x, y, z;" +
                "store d into 'file:///tmp/output';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC3.gld");
    }

    @Test
    public void testLimit() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = limit a 10;" +
                "c = foreach b generate y;" +
                "store c into 'file:///tmp/output';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC4.gld");
    }

    @Test
    public void testDistinct() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = distinct a;" +
                "c = foreach b generate y;" +
                "store c into 'file:///tmp/output';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC5.gld");
    }

    @Test
    public void testSplitSingleVertex() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "split a into b if x <= 5, c if x <= 10, d if x >10;" +
                "store b into 'file:///tmp/output/b';" +
                "store c into 'file:///tmp/output/c';" +
                "store d into 'file:///tmp/output/d';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC6.gld");
    }

    @Test
    public void testSplitMultiVertex() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "split a into b if x <= 5, c if x <= 10, d if x >10;" +
                "b1 = group b by x;" +
                "c1 = limit c 1;" +
                "d1 = filter d by x == 5;" +
                //"d2 = order d by x;" + //TODO
                //"d3 = join c1 by x, d1 by x;" +
                "store b1 into 'file:///tmp/output/b1';" +
                "store c1 into 'file:///tmp/output/c1';" +
                "store d1 into 'file:///tmp/output/d1';";
                //"store d2 into 'file:///tmp/output/d2';" +
                //"store d3 into 'file:///tmp/output/d3';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC7.gld");
    }

    @Test
    public void testMultipleGroupBySplit() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = group a by x;" +
                "c = group a by (x,y);" +
                "store b into 'file:///tmp/output/b';" +
                "store c into 'file:///tmp/output/c';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC8.gld");
    }

    private void run(PhysicalPlan pp, String expectedFile) throws Exception {
        TezCompiler comp = new TezCompiler(pp, pc);
        comp.compile();
        TezPlanContainer tezPlanContainer = comp.getPlanContainer();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        TezPlanContainerPrinter printer = new TezPlanContainerPrinter(ps, tezPlanContainer);
        printer.visit();
        String compiledPlan = baos.toString();
        System.out.println();
        System.out.println("<<<" + compiledPlan + ">>>");

        FileInputStream fis = new FileInputStream(expectedFile);
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        fis.close();
        String goldenPlan = new String(b, 0, len);
        if (goldenPlan.charAt(len-1) == '\n') {
            goldenPlan = goldenPlan.substring(0, len-1);
        }

        System.out.println("-------------");
        System.out.println("Golden");
        System.out.println("<<<" + goldenPlan + ">>>");
        System.out.println("-------------");

        String goldenPlanClean = Util.standardizeNewline(goldenPlan).trim();
        String compiledPlanClean = Util.standardizeNewline(compiledPlan).trim();
        assertEquals(TestHelper.sortUDFs(Util.removeSignature(goldenPlanClean)),
                TestHelper.sortUDFs(Util.removeSignature(compiledPlanClean)));
    }
}

