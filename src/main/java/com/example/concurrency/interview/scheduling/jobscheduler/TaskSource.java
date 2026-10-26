package com.example.concurrency.interview.scheduling.jobscheduler;
import java.util.List;
import java.util.Set;

public interface TaskSource {
    List<Task> getNextTasks(Set<String> finishedTaskIds);
}
