package org.apache.hudi.writer;

import java.util.Random;

/**
 * @author xianghu.wang
 * @time 2020/3/27
 * @description
 */
public class TaskContext {
  private static Random random = new Random();

  public static int getPartitionId() {
    //    return random.nextInt(10);
    return 2;
  }

  public static int getStageId() {
    //    return random.nextInt(10);
    return 1;
  }

  public static int getTaskAttemptId() {
    return 1;
  }
}
