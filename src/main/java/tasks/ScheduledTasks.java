package tasks;

import operations.BatchOperation;
import operations.Memory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Created by etheodor on 16/06/2016.
 */

@Component
public class ScheduledTasks {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledTasks.class);
    JobParameters param = new JobParametersBuilder().toJobParameters();

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("reputationJob")
    Job job;

    @Scheduled( fixedDelay = 300000,initialDelay = 300000)
    public void reportCurrentTime() {
        LOG.info("Reputation Job Started:");
         try {
            BatchOperation.memory.clear();
            JobExecution je = jobLauncher.run(job, param);
             LOG.info("Updated:"+BatchOperation.memory.getUpdated());
            LOG.info("Job Execution:" + je.getStatus().toString());
            LOG.info("Reputation Job Ended Succesfully:");
        } catch (JobExecutionAlreadyRunningException e) {
            LOG.info("JobExecutionAlreadyRunningException:");
            e.printStackTrace();
        } catch (JobRestartException e) {
            LOG.info("JobRestartException:");
            e.printStackTrace();
        } catch (JobInstanceAlreadyCompleteException e) {
            LOG.info("JobInstanceAlreadyCompleteException:");
            e.printStackTrace();
        } catch (JobParametersInvalidException e) {
            LOG.info("JobParametersInvalidException:");
            e.printStackTrace();
        }
    }
}