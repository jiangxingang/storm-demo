package com.xxx.storm.demo;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * wordCount spout
 * @author jiang
 *
 * Utils.waitForMillis(1)找不到该方法。故改成Utils.sleep(1);进行测试
 */
public class SentenceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't have a cow man",
            "i dont't think i like fleas"
    };

    private int index = 0;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("sentence"));
    }

    @Override
    public void open(Map config, TopologyContext context,
                     SpoutOutputCollector collector){
        this.collector = collector;
    }

    @Override
    public void nextTuple(){
        this.collector.emit(new Values(sentences[index]));
        index++;
        if (index >= sentences.length){
            index = 0;
        }
        Utils.sleep(1);
    }
}
