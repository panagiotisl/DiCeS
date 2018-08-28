package gr.uoa.di.madgik.panagiotisl.dices;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

public class DiCeSGrouping implements CustomStreamGrouping, Serializable {

	private static final long serialVersionUID = -829509041881995910L;
	
	private Random random;
    private ArrayList<List<Integer>> choices;
    private AtomicInteger current;
    private List<Integer> all;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
    	
        all = new ArrayList<>(targetTasks);
        Collections.sort(all);
    	
        random = new Random();
        choices = new ArrayList<List<Integer>>(targetTasks.size());
        for (Integer i: targetTasks) {
            choices.add(Arrays.asList(i));
        }
        Collections.shuffle(choices, random);
        current = new AtomicInteger(0);
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
    	
    	if (values.get(1) != null) {
    		return all;
    	}
    	
    	if (values.get(0) instanceof String && ((String) values.get(0)).startsWith(DiCeSSpout.END)) {
    		return all;
    	}
    	
        int rightNow;
        int size = choices.size();
        while (true) {
            rightNow = current.incrementAndGet();
            if (rightNow < size) {
                return choices.get(rightNow);
            } else if (rightNow == size) {
                current.set(0);
                return choices.get(0);
            }
            //race condition with another thread, and we lost
            // try again
        }
    }

}
