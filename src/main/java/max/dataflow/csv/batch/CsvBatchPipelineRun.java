package max.dataflow.csv.batch;

import lombok.extern.slf4j.Slf4j;
import max.dataflow.csv.batch.transforms.StartCsvBatchJobTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

@Slf4j
public class CsvBatchPipelineRun {

  private Pipeline pipeline;

  public CsvBatchPipelineRun(String[] args) {
    PipelineOptionsFactory.register(CsvBatchOptions.class);
    CsvBatchOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CsvBatchOptions.class);
    registerPipeline(options);
    createPipelineSteps();
  }

  public static void main(String[] args) {
    new CsvBatchPipelineRun(args).runPipeline();
  }

  /** Method to register dataflow pipeline step and execute whole pipeline. */
  private void registerPipeline(CsvBatchOptions options) {
    FileSystems.setDefaultPipelineOptions(options);
    pipeline = Pipeline.create(options);
    log.debug("Runner: " + options.getRunner().getName());
    log.debug("JobName: " + options.getJobName());
    log.debug("OptionID: " + options.getOptionsId());
    log.debug("StableUniqueName: " + options.getStableUniqueNames());
    log.debug("TempLocation: " + options.getTempLocation());
    log.debug("UserAgent: " + options.getUserAgent());
  }

  private void runPipeline() {
    try {
      pipeline.run().waitUntilFinish();
    } catch (UnsupportedOperationException e) {
      log.debug("UnsupportedOperationException: ", e);
    } finally {
      log.debug("Completed pipeline setup");
    }
  }

  private void createPipelineSteps() {
    pipeline
        .apply("Start batch job", new StartCsvBatchJobTransform());
  }
}
