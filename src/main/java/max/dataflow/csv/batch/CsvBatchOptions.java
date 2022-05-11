package max.dataflow.csv.batch;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;

public interface CsvBatchOptions extends DataflowPipelineOptions {

  @Description("Destination to read CSV files of Type1")
  String getInputType1Folder();

  void setInputType1Folder(String value);

  @Description("Destination to read CSV files of Type2")
  String getInputType2Folder();

  void setInputType2Folder(String value);

  @Description("Destination to write output CSV files")
  String getOutputFolder();

  void setOutputFolder(String value);

  @Description("Destination to write processing errors")
  String getErrorsOutputFolder();

  void setErrorsOutputFolder(String value);
}
