package operations;

import domain.AssetIPCountConverter;
import domain.TotalAssetCountConverter;
import mongo.MongoDBItemReader;
import mongo.MongoDBItemWriter;
import mongo.OrionItemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
@EnableBatchProcessing()
public class BatchOperation {

    public static Memory memory = new Memory();

    @Autowired
    public JobBuilderFactory jobBuilderFactory;
    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Value("${mongodbport}")
    private Integer mongodbport;

    @Value("${mongodbhost}")
    private String mongodbhost;

    @Bean(name = "reputationJob")
    public Job getJob() {
        return jobBuilderFactory.get("reputationJob")
                .incrementer(new RunIdIncrementer())
                .start(step2()).on("*").to(step3())
                .build().build();
    }

    // Calculation of total count of requests for each asset
    @Bean(name = "step2")
    public Step step2() {
        return stepBuilderFactory.get("step2")
                .chunk(1)
                .reader(new MongoDBItemReader(mongodbhost, mongodbport, "apilog", "apiLogEntry", new TotalAssetCountConverter(), null, "{$group:{\"_id\" :  \"$asset\",\"count\" : {\"$sum\" : 1 }}}", null))
                .processor(new FuseStatisticObject()) //fuse stat in the shared memory object
                //.writer(new ConsoleItemWriter<>())
                .build();
    }

    // Calculation of total IPs for each asset
    @Bean(name = "step3")
    public Step step3() {
        return stepBuilderFactory.get("step3")
                .chunk(1)
                .reader(new MongoDBItemReader(mongodbhost, mongodbport, "apilog", "apiLogEntry", new AssetIPCountConverter(), null, "{$group: {\"_id\" :  {asset:\"$asset\",ip:\"$ip\"} , data: {   $addToSet : 1 } }}", "{$group: {\"_id\" : \"$_id.asset\", \"count\" :{$sum:1} }}"))
                .processor(new FuseStatisticObjectToDBObject()) //fuse all statistics from the shared memory to a db object for inserting to mongo
                .writer(new MongoDBItemWriter(mongodbhost, mongodbport, "apilog", "assetStatistics"))
                .build();
    }

    @Bean(name = "calculateModelJob")
    public Job calculateModel() {
        return jobBuilderFactory.get("calculateModel")
                .incrementer(new RunIdIncrementer())
                .start(step2()).on("*").to(step3()).on("*").to(step4())
                .build().build();
    }

    //Calculate from statistics thet reputation score(iterate all objects from a select on database)
    @Bean(name = "step4")
    public Step step4() {
        return stepBuilderFactory.get("step4")
                .chunk(10)
                .reader(new MongoDBItemReader(mongodbhost, mongodbport, "orion-organicity", "entities", null, "{},{\"_id.id\":1}", null, null))
                .processor(new FuseStatisticObjectToReputationScore())
                .writer(new OrionItemWriter(mongodbhost, mongodbport, "orion-organicity", "entities")).listener(new JobListener())
                .build();
    }


    public class JobListener implements StepExecutionListener {

        private final Logger LOG = LoggerFactory.getLogger(JobListener.class);
        @Override
        public void beforeStep(StepExecution stepExecution) {

        }

        @Override
        public ExitStatus afterStep(StepExecution stepExecution) {
            LOG.info("JOB FINISHED:"+stepExecution.getStatus());
            return new ExitStatus("COMPLETED");
        }
    }


}