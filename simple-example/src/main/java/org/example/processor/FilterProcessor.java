package org.example.processor;


import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class FilterProcessor implements Processor<String, String> {
    private ProcessorContext pc;

    @Override
    public void init(ProcessorContext context) {
        this.pc = context;
    }

    @Override
    public void process(String key, String value) {
        if (value.length() > 5) {
            pc.forward(key, value);
        }
        pc.commit();
    }

    @Override
    public void close() {
    }
}
