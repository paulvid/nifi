package org.apache.nifi;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.List;


public class BoilerpipeArticleExtractorTest extends AbstractHTMLTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(BoilerpipeArticleExtractor.class);
    }

    @Test
    public void testProcessor() {
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(BoilerpipeArticleExtractor.URL_PROPERTY,"https://www.nytimes.com/reuters/2018/08/17/business/17reuters-usa-tunnels-china.html");
        testRunner.enqueue("Mock FlowFile");
        testRunner.run();


        testRunner.assertTransferCount(BoilerpipeArticleExtractor.REL_SUCCESS, 1);
        testRunner.assertTransferCount(BoilerpipeArticleExtractor.REL_FAILURE, 0);


        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(BoilerpipeArticleExtractor.REL_SUCCESS);

        flowFiles.get(0).assertContentEquals(BOILERPIPE_NYT_EXTRACT);

    }

}
