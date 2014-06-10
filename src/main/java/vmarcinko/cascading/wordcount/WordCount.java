package vmarcinko.cascading.wordcount;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepStrategy;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.List;
import java.util.Properties;

public class WordCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCount(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        Configuration conf = getConf();

        doWorkflow(inputPath, outputPath, conf);

        return 0;
    }

    private void doWorkflow(String inputPath, String outputPath, Configuration conf) {
        String lineTextFieldName = "lineText";

        // create source and sink taps
        Tap linesTap = new Hfs(new TextLine(new Fields("lineOffset", lineTextFieldName), Fields.ALL), inputPath);
        Tap wordCountsTap = new Hfs(new TextDelimited(true, "\t"), outputPath, SinkMode.REPLACE);

        Pipe pipe = new Pipe("word");

        Fields wordFields = new Fields("word");
        RegexSplitGenerator wordSplitter = new RegexSplitGenerator(wordFields, "[\\W]+");
        pipe = new Each(pipe, new Fields(lineTextFieldName), wordSplitter);

        // determine the word counts
        pipe = new CountBy(pipe, wordFields, new Fields("count"));

        // connect the taps, pipes, etc., into a flow
        FlowDef flowDef = FlowDef.flowDef()
                .setName("wordCountFlow")
                .addSource(pipe, linesTap)
                .addTailSink(pipe, wordCountsTap);

        Properties properties = AppProps.appProps()
                .setName("word-count")
                .setVersion("1.0")
                .setJarClass(getClass())
                .buildProperties(conf);

        Hadoop2MR1FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);
        Flow flow = flowConnector.connect(flowDef);

        String mapReduceJobName = "Cascading Word Count: '" + inputPath + "' -> '" + outputPath + "'";
        FlowStepStrategy flowStepStrategy = constructMapReduceJobNameStrategy(mapReduceJobName);
        flow.setFlowStepStrategy(flowStepStrategy);

        flow.complete();
    }

    private FlowStepStrategy constructMapReduceJobNameStrategy(final String mapReduceJobName) {
        return new FlowStepStrategy() {
            @Override
            public void apply(Flow flow, List predecessorSteps, FlowStep flowStep) {
                Object config = flowStep.getConfig();
                if (config instanceof JobConf) {
                    ((JobConf) config).setJobName(mapReduceJobName);
                }
            }
        };
    }

}
